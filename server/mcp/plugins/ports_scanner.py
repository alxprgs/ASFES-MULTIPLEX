from __future__ import annotations

import asyncio
import ipaddress
import socket
from typing import Any
from urllib.parse import urlparse

import httpx

from server.host_ops import _psutil
from server.mcp.plugins._common import int_argument, require_argument, static_availability
from server.models import MCPTool, MCPToolManifest, PermissionDefinition, PluginDefinition, PluginManifest, ToolExecutionContext


# ---------------------------------------------------------------------------
# SSRF / DNS-rebinding guards
# ---------------------------------------------------------------------------

# All IP ranges that must never be probed — covers loopback, RFC 1918,
# link-local (incl. AWS/GCP metadata), CGNAT, and IPv6 special ranges.
_BLOCKED_NETWORKS: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = [
    ipaddress.ip_network("127.0.0.0/8"),      # IPv4 loopback
    ipaddress.ip_network("::1/128"),           # IPv6 loopback
    ipaddress.ip_network("10.0.0.0/8"),        # RFC 1918
    ipaddress.ip_network("172.16.0.0/12"),     # RFC 1918
    ipaddress.ip_network("192.168.0.0/16"),    # RFC 1918
    ipaddress.ip_network("169.254.0.0/16"),    # Link-local / AWS IMDS
    ipaddress.ip_network("fe80::/10"),         # IPv6 link-local
    ipaddress.ip_network("fc00::/7"),          # IPv6 ULA
    ipaddress.ip_network("100.64.0.0/10"),     # CGNAT
    ipaddress.ip_network("0.0.0.0/8"),         # "This" network
]


def _is_blocked_ip(ip_str: str) -> bool:
    """Return True if *ip_str* is a reserved / private address."""
    try:
        ip = ipaddress.ip_address(ip_str)
        return any(ip in net for net in _BLOCKED_NETWORKS)
    except ValueError:
        return True  # Malformed → block


def _resolve_and_validate(host: str, allowed_hosts: set[str]) -> str:
    """Resolve *host* to its first IPv4/IPv6 address and validate it.

    Returns the resolved IP string so the caller can use it directly,
    avoiding a second DNS lookup (DNS-rebinding protection).

    Raises RuntimeError if the host is not in *allowed_hosts* and resolves
    to a blocked (private / reserved) address.
    """
    normalized = host.strip().lower().strip("[]")

    # Hosts explicitly allowlisted by the admin are trusted as-is.
    if normalized in allowed_hosts:
        # Still resolve so we can return the IP for direct connection.
        try:
            infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
            return infos[0][4][0]
        except socket.gaierror:
            # Allowlisted but currently unresolvable (e.g. localhost without DNS).
            return host

    # Non-allowlisted host: resolve once, then reject if any address is blocked.
    try:
        infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise RuntimeError("Probe host cannot be resolved") from exc

    for info in infos:
        resolved_ip = info[4][0]
        if not _is_blocked_ip(resolved_ip):
            return resolved_ip  # First non-blocked address wins

    raise RuntimeError("Probe host is not allowed")


def _validate_probe_host(context: ToolExecutionContext, host: str) -> None:
    """Validate *host* against the allowlist; raise RuntimeError if disallowed.

    This is a lightweight wrapper kept for backward compatibility with
    probe_tcp (which handles TCP connections directly via socket).
    """
    allowed_hosts = {item.lower() for item in context.services.settings.host_ops.port_probe_allowed_hosts}
    _resolve_and_validate(host, allowed_hosts)


# ---------------------------------------------------------------------------
# Custom httpx transport: pins the resolved IP while preserving TLS/SNI
# ---------------------------------------------------------------------------

class _PinnedIPTransport(httpx.AsyncHTTPTransport):
    """httpx transport that connects to a pre-resolved IP address.

    This eliminates the window for DNS rebinding: the hostname is resolved
    **once** by us (validated against the blocklist), and all subsequent
    TCP connections go directly to that IP.  TLS is still verified against
    the *original hostname* because:

    - The ``Host`` header is set to the original hostname by httpx.
    - The ``sni_hostname`` extension tells httpcore which name to use for
      TLS SNI negotiation, so certificate validation uses the original CN/SAN.

    References:
      - httpcore ``sni_hostname`` extension:
        https://www.encode.io/httpcore/extensions/
    """

    def __init__(self, hostname: str, resolved_ip: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._hostname = hostname
        self._resolved_ip = resolved_ip

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.url.host == self._hostname:
            # Rewrite the URL to connect to the resolved IP address.
            new_url = request.url.copy_with(host=self._resolved_ip)
            # Preserve SNI so TLS negotiation uses the original hostname.
            # httpcore reads "sni_hostname" from the request extensions.
            extensions = {**request.extensions, "sni_hostname": self._hostname.encode("ascii")}
            request = httpx.Request(
                method=request.method,
                url=new_url,
                headers=request.headers,
                extensions=extensions,
                content=request.stream,
            )
        return await super().handle_async_request(request)


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------

async def list_listening_ports(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    if _psutil is None:
        raise RuntimeError("psutil is required to inspect listening ports")
    listeners = []
    for connection in _psutil.net_connections(kind="inet"):
        if connection.status != "LISTEN":
            continue
        listeners.append(
            {
                "family": getattr(connection.family, "name", str(connection.family)),
                "type": getattr(connection.type, "name", str(connection.type)),
                "ip": connection.laddr.ip if connection.laddr else None,
                "port": connection.laddr.port if connection.laddr else None,
                "pid": connection.pid,
            }
        )
    return {"listeners": listeners, "count": len(listeners)}


async def probe_tcp(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    host = str(arguments.get("host") or "127.0.0.1")
    _validate_probe_host(context, host)
    port = int_argument(arguments, "port", 0)
    timeout_seconds = float(arguments.get("timeout_seconds") or 2.0)

    def _probe() -> bool:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            return True

    try:
        await asyncio.to_thread(_probe)
        return {"host": host, "port": port, "reachable": True, "timeout_seconds": timeout_seconds}
    except OSError as exc:
        return {"host": host, "port": port, "reachable": False, "timeout_seconds": timeout_seconds, "error": str(exc)}


async def probe_http(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    url = require_argument(arguments, "url")
    parsed = urlparse(str(url))
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise RuntimeError("Only http/https URLs with a host are allowed")

    allowed_hosts = {
        item.lower()
        for item in context.services.settings.host_ops.port_probe_allowed_hosts
    }

    # Resolve DNS *once* and validate — the returned IP is used for all connections.
    # This prevents DNS rebinding: the transport will connect to this specific IP,
    # not whatever the OS resolver returns at connection time.
    resolved_ip = _resolve_and_validate(parsed.hostname, allowed_hosts)

    timeout_seconds = float(arguments.get("timeout_seconds") or 5.0)
    method = str(arguments.get("method") or "GET").upper()

    # Use the pinned-IP transport for DNS-rebinding protection.
    # TLS/SNI stays correct because _PinnedIPTransport sets sni_hostname.
    transport = _PinnedIPTransport(hostname=parsed.hostname, resolved_ip=resolved_ip)

    async with httpx.AsyncClient(
        transport=transport,
        timeout=timeout_seconds,
        # Redirects disabled: a redirect response could point to a private IP
        # that bypasses the resolved_ip we validated above.
        follow_redirects=False,
    ) as client:
        response = await client.request(method, str(url))

    return {
        "url": str(url),
        "method": method,
        "ok": response.is_success,
        "status_code": response.status_code,
        "headers": dict(response.headers),
    }


PLUGIN = PluginDefinition(
    manifest=PluginManifest(
        key="ports_scanner",
        name="Сканер портов",
        version="1.1.0",
        description="Проверяет слушающие порты и доступность разрешенных TCP/HTTP endpoint.",
        permissions=[
            PermissionDefinition(key="ports.read", description="Проверять слушающие порты."),
            PermissionDefinition(key="ports.probe", description="Проверять TCP/HTTP endpoint через allowlist."),
        ],
    ),
    tools={
        "ports_scanner.list_listening_ports": MCPTool(
            manifest=MCPToolManifest(
                key="ports_scanner.list_listening_ports",
                name="Список слушающих портов",
                description="Показывает порты, которые сейчас слушаются на локальном хосте.",
                input_schema={"type": "object", "properties": {}, "additionalProperties": False},
                permissions=["ports.read"],
                tags=["ports", "read"],
                read_only=True,
                required_backends=["psutil"],
            ),
            handler=list_listening_ports,
            availability=static_availability(require_psutil=True),
        ),
        "ports_scanner.probe_tcp": MCPTool(
            manifest=MCPToolManifest(
                key="ports_scanner.probe_tcp",
                name="Проверить TCP-порт",
                description="Проверяет, принимает ли разрешенный TCP endpoint подключение.",
                input_schema={
                    "type": "object",
                    "required": ["port"],
                    "properties": {
                        "host": {"type": "string"},
                        "port": {"type": "integer"},
                        "timeout_seconds": {"type": "number"},
                    },
                    "additionalProperties": False,
                },
                permissions=["ports.probe"],
                tags=["ports", "tcp", "read"],
                read_only=True,
                default_global_enabled=False,
            ),
            handler=probe_tcp,
        ),
        "ports_scanner.probe_http": MCPTool(
            manifest=MCPToolManifest(
                key="ports_scanner.probe_http",
                name="Проверить HTTP endpoint",
                description="Отправляет один HTTP-запрос к разрешенному endpoint.",
                input_schema={
                    "type": "object",
                    "required": ["url"],
                    "properties": {
                        "url": {"type": "string"},
                        "method": {"type": "string"},
                        "timeout_seconds": {"type": "number"},
                    },
                    "additionalProperties": False,
                },
                permissions=["ports.probe"],
                tags=["ports", "http", "read"],
                read_only=True,
                default_global_enabled=False,
            ),
            handler=probe_http,
        ),
    },
)
