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


def _validate_probe_host(context: ToolExecutionContext, host: str) -> None:
    allowed_hosts = {item.lower() for item in context.services.settings.host_ops.port_probe_allowed_hosts}
    normalized = host.strip().lower().strip("[]")
    if normalized in allowed_hosts:
        return
    try:
        addresses = {item[4][0] for item in socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)}
    except socket.gaierror as exc:
        raise RuntimeError("Probe host cannot be resolved") from exc
    for address in addresses:
        ip = ipaddress.ip_address(address)
        if ip.is_loopback and ("127.0.0.1" in allowed_hosts or "::1" in allowed_hosts):
            continue
        raise RuntimeError("Probe host is not allowed")


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
    _validate_probe_host(context, parsed.hostname)
    timeout_seconds = float(arguments.get("timeout_seconds") or 5.0)
    method = str(arguments.get("method") or "GET").upper()
    async with httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=True) as client:
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
        version="1.0.0",
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
