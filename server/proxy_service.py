from __future__ import annotations

import asyncio
import ipaddress
import socket
import time
from datetime import UTC, datetime
from uuid import uuid4
from typing import Any

import httpx
from pymongo.errors import DuplicateKeyError

from server.core.config import Settings
from server.core.database import DatabaseManager, PROXIES
from server.core.security import now_utc
from server.core.crypto import ProxyEncryptor


# Networks that must never be used as proxy endpoints (SSRF guard).
_BLOCKED_NETWORKS: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = [
    ipaddress.ip_network("127.0.0.0/8"),  # IPv4 loopback
    ipaddress.ip_network("::1/128"),  # IPv6 loopback
    ipaddress.ip_network("10.0.0.0/8"),  # RFC 1918
    ipaddress.ip_network("172.16.0.0/12"),  # RFC 1918
    ipaddress.ip_network("192.168.0.0/16"),  # RFC 1918
    ipaddress.ip_network("169.254.0.0/16"),  # Link-local / AWS IMDS
    ipaddress.ip_network("fe80::/10"),  # IPv6 link-local
    ipaddress.ip_network("fc00::/7"),  # IPv6 ULA
    ipaddress.ip_network("100.64.0.0/10"),  # CGNAT
    ipaddress.ip_network("0.0.0.0/8"),  # "This" network
]


def _is_blocked_ip(ip_str: str) -> bool:
    """Return True if *ip_str* falls into a reserved / private network."""
    try:
        ip = ipaddress.ip_address(ip_str)
        return any(ip in net for net in _BLOCKED_NETWORKS)
    except ValueError:
        return True  # Malformed → reject


def _validate_proxy_host(host: str) -> None:
    """Raise ValueError if *host* is a blocked IP or resolves to one.

    Called both at **create** time (static IP check) and at **check** time
    (DNS resolution → re-validate resolved IPs to prevent DNS rebinding).
    """
    stripped = host.strip().strip("[]")

    # Fast path: if the host is already an IP address, check it directly.
    try:
        ip = ipaddress.ip_address(stripped)
        if _is_blocked_ip(str(ip)):
            raise ValueError(f"Proxy host '{host}' resolves to a reserved address")
        return
    except ValueError as exc:
        if "reserved" in str(exc):
            raise
        # Not a bare IP — treat as hostname and resolve below.

    # Resolve hostname to all returned addresses and validate each one.
    try:
        infos = socket.getaddrinfo(stripped, None, type=socket.SOCK_STREAM)
    except socket.gaierror:
        # Cannot resolve at create-time — allow; will re-check at probe-time.
        return
    for info in infos:
        resolved = info[4][0]
        if _is_blocked_ip(resolved):
            raise ValueError(
                f"Proxy host '{host}' resolves to a reserved address: {resolved}"
            )


class ProxyService:
    def __init__(self, db: DatabaseManager, settings: Settings) -> None:
        self.db = db
        self.settings = settings
        # Use the dedicated SECURITY__PROXY_ENCRYPTION_KEY, NOT the JWT secret.
        # This enforces key separation: rotating one secret does not break the other.
        proxy_key = settings.security.proxy_encryption_key.get_secret_value()
        self.encryptor = ProxyEncryptor(proxy_key)

    async def create_proxy(
        self,
        user_id: str,
        *,
        protocol: str,
        host: str,
        port: int,
        username: str | None = None,
        password: str | None = None,
        label: str | None = None,
    ) -> dict[str, Any]:
        # Enforce maximum 500 proxies limit
        count = await self.count_proxies(user_id)
        if count >= 500:
            raise ValueError("Proxy limit exceeded (maximum 500 proxies per user)")

        # SSRF guard: reject reserved / private addresses at create-time.
        # Hostnames that cannot be resolved at this point are allowed through;
        # they will be re-validated (with actual DNS resolution) in check_proxy_single.
        _validate_proxy_host(host)

        proxy_id = uuid4().hex
        encrypted_password = self.encryptor.encrypt(password)

        doc = {
            "_id": proxy_id,
            "user_id": user_id,
            "protocol": protocol,
            "host": host.strip(),
            "port": port,
            "username": username.strip() if username else None,
            "password_encrypted": encrypted_password,
            "label": label.strip() if label else None,
            "last_check": None,
            "created_at": now_utc(),
        }

        try:
            await self.db.collection(PROXIES).insert_one(doc)
        except DuplicateKeyError as exc:
            raise ValueError(
                "Proxy with this protocol, host and port already exists"
            ) from exc

        return doc

    async def list_proxies(self, user_id: str) -> list[dict[str, Any]]:
        cursor = (
            self.db.collection(PROXIES)
            .find({"user_id": user_id})
            .sort("created_at", -1)
        )
        return [item async for item in cursor]

    async def get_proxy(self, user_id: str, proxy_id: str) -> dict[str, Any] | None:
        return await self.db.collection(PROXIES).find_one(
            {"_id": proxy_id, "user_id": user_id}
        )

    async def delete_proxy(self, user_id: str, proxy_id: str) -> bool:
        result = await self.db.collection(PROXIES).delete_one(
            {"_id": proxy_id, "user_id": user_id}
        )
        return result.deleted_count > 0

    async def count_proxies(self, user_id: str) -> int:
        return await self.db.collection(PROXIES).count_documents({"user_id": user_id})

    async def update_proxy_check(
        self, proxy_id: str, check_result: dict[str, Any]
    ) -> None:
        await self.db.collection(PROXIES).update_one(
            {"_id": proxy_id},
            {"$set": {"last_check": check_result}},
        )

    def decrypt_password(self, encrypted_password: str | None) -> str | None:
        if not encrypted_password:
            return None
        try:
            return self.encryptor.decrypt(encrypted_password)
        except Exception as exc:
            import logging

            logging.getLogger("multiplex.proxy").warning(
                f"Failed to decrypt proxy password: {exc}"
            )
            return None

    @staticmethod
    def parse_proxy_url(url: str, protocol: str) -> dict[str, Any]:
        url = url.strip()
        if "://" in url:
            url = url.split("://", 1)[1]

        username = None
        password = None

        if "@" in url:
            auth_part, _, host_port_part = url.rpartition("@")
            if ":" in auth_part:
                username, _, password = auth_part.partition(":")
            else:
                username = auth_part
        else:
            host_port_part = url

        if not host_port_part:
            raise ValueError("Missing host and port")

        host, _, port_str = host_port_part.rpartition(":")
        if not host or not port_str:
            raise ValueError("Invalid host:port format")

        try:
            port = int(port_str)
        except ValueError as exc:
            raise ValueError("Port must be an integer") from exc

        if port < 1 or port > 65535:
            raise ValueError("Port must be between 1 and 65535")

        return {
            "protocol": protocol,
            "host": host,
            "port": port,
            "username": username,
            "password": password,
        }

    @staticmethod
    def parse_proxifier_xml(xml_content: str) -> list[dict[str, Any]]:
        # Use defusedxml to prevent Billion Laughs / XXE attacks.
        # Falls back to stdlib ET with a warning if defusedxml is unavailable.
        _MAX_XML_BYTES = 1_048_576  # 1 MiB hard cap
        if len(xml_content.encode("utf-8")) > _MAX_XML_BYTES:
            raise ValueError("XML content exceeds maximum allowed size (1 MiB)")

        try:
            import defusedxml.ElementTree as ET  # type: ignore[import]
        except ImportError:
            import warnings
            import xml.etree.ElementTree as ET  # type: ignore[assignment]

            warnings.warn(
                "defusedxml is not installed; XML parsing may be vulnerable to "
                "entity expansion attacks. Install: pip install defusedxml",
                RuntimeWarning,
                stacklevel=2,
            )

        try:
            root = ET.fromstring(xml_content.strip())
        except Exception as exc:
            raise ValueError(f"Invalid XML format: {exc}") from exc

        proxy_list = root.find("ProxyList")
        if proxy_list is None:
            return []

        proxies = []
        for proxy_node in proxy_list.findall("Proxy"):
            proto_type = proxy_node.get("type", "").lower()
            if proto_type == "https":
                protocol = "https"
            elif proto_type == "http":
                protocol = "http"
            elif proto_type == "socks5":
                protocol = "socks5"
            else:
                # SOCKS4 or unknown protocols are ignored
                continue

            port_node = proxy_node.find("Port")
            addr_node = proxy_node.find("Address")
            if port_node is None or addr_node is None:
                continue

            try:
                port = int(port_node.text.strip())
            except (ValueError, AttributeError):
                continue

            if port < 1 or port > 65535:
                continue

            host = addr_node.text.strip() if addr_node.text else ""
            if not host:
                continue

            username = None
            password = None
            auth_node = proxy_node.find("Authentication")
            if auth_node is not None and auth_node.get("enabled") == "true":
                user_node = auth_node.find("Username")
                pass_node = auth_node.find("Password")
                if user_node is not None and user_node.text:
                    username = user_node.text.strip()
                if pass_node is not None and pass_node.text:
                    password = pass_node.text.strip()

            proxies.append(
                {
                    "protocol": protocol,
                    "host": host,
                    "port": port,
                    "username": username,
                    "password": password,
                }
            )
        return proxies

    @staticmethod
    def export_as_proxifier_xml(proxies: list[dict[str, Any]]) -> str:
        import xml.etree.ElementTree as ET

        root = ET.Element(
            "ProxifierProfile",
            {
                "version": "102",
                "platform": "Windows",
                "product_id": "0",
                "product_minver": "400",
            },
        )

        options = ET.SubElement(root, "Options")
        resolve = ET.SubElement(options, "Resolve")
        ET.SubElement(resolve, "AutoModeDetection", {"enabled": "false"})
        ET.SubElement(resolve, "ViaProxy", {"enabled": "true"})
        ET.SubElement(resolve, "BlockNonATypes", {"enabled": "true"})
        ex_list = ET.SubElement(resolve, "ExclusionList", {"OnlyFromListMode": "false"})
        ex_list.text = "%ComputerName%; localhost; *.local"
        ET.SubElement(resolve, "DnsUdpMode").text = "0"

        ET.SubElement(options, "Encryption", {"mode": "disabled"})
        ET.SubElement(
            options, "ConnectionLoopDetection", {"enabled": "false", "resolve": "true"}
        )
        ET.SubElement(options, "Udp", {"mode": "mode_block_all"})
        ET.SubElement(options, "LeakPreventionMode", {"enabled": "true"})
        ET.SubElement(options, "ProcessOtherUsers", {"enabled": "false"})
        ET.SubElement(options, "ProcessServices", {"enabled": "false"})
        ET.SubElement(options, "HandleDirectConnections", {"enabled": "false"})
        ET.SubElement(options, "HttpProxiesSupport", {"enabled": "false"})

        proxy_list = ET.SubElement(root, "ProxyList")

        for i, proxy in enumerate(proxies):
            proxy_id = str(100 + i)
            p_type = proxy["protocol"].upper()
            proxy_node = ET.SubElement(
                proxy_list,
                "Proxy",
                {
                    "id": proxy_id,
                    "type": p_type,
                },
            )

            addr_node = ET.SubElement(proxy_node, "Address")
            addr_node.text = proxy["host"]

            port_node = ET.SubElement(proxy_node, "Port")
            port_node.text = str(proxy["port"])

            options_node = ET.SubElement(proxy_node, "Options")
            options_node.text = "48"

            if proxy.get("username") or proxy.get("password"):
                auth_node = ET.SubElement(
                    proxy_node, "Authentication", {"enabled": "true"}
                )
                if proxy.get("username"):
                    u_node = ET.SubElement(auth_node, "Username")
                    u_node.text = proxy["username"]
                if proxy.get("password"):
                    pass_node = ET.SubElement(auth_node, "Password")
                    pass_node.text = proxy["password"]
            else:
                ET.SubElement(proxy_node, "Authentication", {"enabled": "false"})

        ET.SubElement(root, "ChainList")

        rule_list = ET.SubElement(root, "RuleList")
        default_rule = ET.SubElement(rule_list, "Rule", {"enabled": "true"})
        ET.SubElement(default_rule, "Action", {"type": "Direct"})
        name_node = ET.SubElement(default_rule, "Name")
        name_node.text = "Default"

        from xml.dom import minidom

        xml_str = ET.tostring(root, encoding="utf-8")
        reparsed = minidom.parseString(xml_str)
        pretty_xml = reparsed.toprettyxml(indent="\t")

        # Strip standard minidom declaration and prep our own clean declaration
        lines = pretty_xml.splitlines()
        if lines and lines[0].startswith("<?xml"):
            lines = lines[1:]

        xml_body = "\n".join(lines)
        return '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n' + xml_body

    @staticmethod
    def export_as_url(proxy: dict[str, Any], plain_password: str | None = None) -> str:
        protocol = proxy["protocol"]
        host = proxy["host"]
        port = proxy["port"]
        username = proxy.get("username")

        if username:
            pass_str = f":{plain_password}" if plain_password else ""
            return f"{protocol}://{username}{pass_str}@{host}:{port}"
        return f"{protocol}://{host}:{port}"

    @staticmethod
    def export_as_lines(
        proxy: dict[str, Any], plain_password: str | None = None
    ) -> str:
        username = proxy.get("username") or ""
        password = plain_password or ""
        host = proxy["host"]
        port = str(proxy["port"])
        return f"{username}\n{password}\n{host}\n{port}"

    @staticmethod
    def export_as_tg_proxy(
        proxy: dict[str, Any], secret: str | None = None
    ) -> dict[str, str]:
        host = proxy["host"]
        port = proxy["port"]
        sec_param = f"&secret={secret}" if secret else ""

        deep_link = f"tg://proxy?server={host}&port={port}{sec_param}"
        web_url = f"https://t.me/proxy?server={host}&port={port}{sec_param}"
        return {
            "deep_link": deep_link,
            "web_url": web_url,
        }

    async def check_proxy_single(
        self, proxy: dict[str, Any], timeout: int = 10
    ) -> dict[str, Any]:
        protocol = proxy["protocol"]
        host = proxy["host"]
        port = proxy["port"]
        username = proxy.get("username")
        encrypted_pass = proxy.get("password_encrypted")
        password = self.decrypt_password(encrypted_pass) if encrypted_pass else None

        # SSRF guard: re-validate host with DNS resolution at check-time.
        # This catches DNS rebinding and cases where a hostname changed after creation.
        try:
            _validate_proxy_host(host)
        except ValueError as exc:
            return {
                "checked_at": datetime.now(UTC).isoformat(),
                "ok": False,
                "avg_latency_ms": None,
                "details": {
                    "ssrf_guard": {
                        "ok": False,
                        "latency_ms": None,
                        "external_ip": None,
                        "error": str(exc),
                    }
                },
            }

        auth_str = ""
        if username:
            auth_str = f"{username}:{password}@" if password else f"{username}@"

        # Build transport proxy config
        # Map https protocol to http scheme since httpx expects TLS proxy for https://
        scheme = "http" if protocol in ("http", "https") else protocol
        proxy_url = f"{scheme}://{auth_str}{host}:{port}"

        targets = {
            "ip_api": "http://ip-api.com/json",
            "google": "https://www.google.com",
            "telegram": "https://t.me",
        }

        results = {}
        ok = False
        latencies = []
        country_res = None
        provider_res = None

        async def _test_url(target_name: str, target_url: str) -> None:
            nonlocal ok, country_res, provider_res
            start = time.perf_counter()
            try:
                # follow_redirects=False: prevents a redirect from the proxy
                # target from routing through private addresses.
                async with httpx.AsyncClient(
                    proxy=proxy_url,
                    verify=False,
                    follow_redirects=False,
                ) as client:
                    resp = await client.get(target_url, timeout=timeout)
                    latency = int((time.perf_counter() - start) * 1000)
                    external_ip = None
                    if target_name == "ip_api":
                        try:
                            data = resp.json()
                            external_ip = data.get("query")
                            country_res = data.get("country")
                            provider_res = data.get("isp")
                        except Exception:
                            external_ip = resp.text.strip()

                    results[target_name] = {
                        "ok": True,
                        "latency_ms": latency,
                        "external_ip": external_ip,
                    }
                    latencies.append(latency)
                    ok = True
            except Exception as exc:
                err_msg = f"{type(exc).__name__}"
                if str(exc):
                    err_msg += f": {exc}"
                results[target_name] = {
                    "ok": False,
                    "latency_ms": None,
                    "external_ip": None,
                    "error": err_msg,
                }

        # Run tests sequentially to avoid local CPU spikes, but under timeout
        for name, url in targets.items():
            await _test_url(name, url)

        avg_latency = int(sum(latencies) / len(latencies)) if latencies else None

        return {
            "checked_at": datetime.now(UTC).isoformat(),
            "ok": ok,
            "avg_latency_ms": avg_latency,
            "details": results,
            "country": country_res,
            "provider": provider_res,
        }

    async def check_all_background(
        self, user_id: str, proxy_ids: list[str] | None = None
    ) -> None:
        if proxy_ids is None:
            proxies = await self.list_proxies(user_id)
        else:
            proxies = []
            for pid in proxy_ids:
                p = await self.get_proxy(user_id, pid)
                if p:
                    proxies.append(p)

        sem = asyncio.Semaphore(5)

        async def _check_and_update(proxy: dict[str, Any]) -> None:
            async with sem:
                try:
                    res = await self.check_proxy_single(proxy, timeout=8)
                    await self.update_proxy_check(proxy["_id"], res)
                except Exception:
                    # Ignore background errors to let other checks continue
                    pass

        tasks = [_check_and_update(p) for p in proxies]
        await asyncio.gather(*tasks, return_exceptions=True)
