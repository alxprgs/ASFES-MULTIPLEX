from __future__ import annotations

import ssl as ssl_module
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from server.mcp.plugins._common import require_argument
from server.models import MCPTool, MCPToolManifest, PermissionDefinition, PluginDefinition, PluginManifest, RuntimeAvailability, ToolExecutionContext


def _default_ssl_provider(context: ToolExecutionContext) -> str:
    override = context.services.host_ops.provider_override("ssl")
    if override:
        return override
    return "certbot" if context.services.host_ops.is_linux else "wacs"


async def _ssl_provider_availability(services) -> RuntimeAvailability:
    override = services.host_ops.provider_override("ssl")
    if override:
        return services.host_ops.availability_for_command(override, providers=[override])
    return services.host_ops.availability_for_any_command(["certbot", "wacs", "wacs.exe"], providers=["certbot", "win-acme"])


def _load_profile(context: ToolExecutionContext, name: str) -> dict[str, Any]:
    return context.services.host_ops.load_json_profile("ssl", name)


async def list_profiles(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    return {"profiles": context.services.host_ops.list_profiles("ssl")}


async def issue_certificate(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    profile_name = str(require_argument(arguments, "profile"))
    profile = _load_profile(context, profile_name)
    provider = str(profile.get("provider") or _default_ssl_provider(context))
    executable = context.services.host_ops.executable_path(provider)
    if not executable:
        raise RuntimeError(f"SSL provider '{provider}' is not available")
    domains = [str(item) for item in profile.get("domains", []) if str(item).strip()]
    if provider == "certbot":
        email = str(profile.get("email") or "")
        webroot = str(profile.get("webroot") or "")
        if not domains or not email:
            raise RuntimeError("Certbot profiles require 'domains' and 'email'")
        command = [executable, "certonly", "--non-interactive", "--agree-tos", "-m", email]
        if webroot:
            command.extend(["--webroot", "-w", webroot])
        else:
            command.append("--standalone")
        for domain in domains:
            command.extend(["-d", domain])
    else:
        extra_args = [str(item) for item in profile.get("arguments", []) if str(item).strip()]
        if extra_args:
            command = [executable, *extra_args]
        else:
            if not domains:
                raise RuntimeError("win-acme profiles require 'domains' or explicit 'arguments'")
            command = [executable, "--accepttos", "--source", "manual", "--host", ",".join(domains)]
            if profile.get("email"):
                command.extend(["--emailaddress", str(profile["email"])])
    result = await context.services.host_ops.run(command, check=False)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "SSL issue command failed")
    return {"profile": profile_name, "provider": provider, **result.to_dict()}


async def renew_certificate(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    profile_name = str(require_argument(arguments, "profile"))
    profile = _load_profile(context, profile_name)
    provider = str(profile.get("provider") or _default_ssl_provider(context))
    executable = context.services.host_ops.executable_path(provider)
    if not executable:
        raise RuntimeError(f"SSL provider '{provider}' is not available")
    if provider == "certbot":
        command = [executable, "renew"]
    else:
        command = [executable, "--renew"]
    result = await context.services.host_ops.run(command, check=False)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "SSL renew command failed")
    return {"profile": profile_name, "provider": provider, **result.to_dict()}


async def check_expiry(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    profile_name = str(require_argument(arguments, "profile"))
    profile = _load_profile(context, profile_name)
    certificate_path = profile.get("certificate_path") or profile.get("cert_path")
    if not certificate_path:
        raise RuntimeError("SSL profile does not define certificate_path")
    cert_path = context.services.host_ops.resolve_managed_path(str(certificate_path), roots=context.services.host_ops.managed_file_roots())
    decoded = ssl_module._ssl._test_decode_cert(str(cert_path))  # type: ignore[attr-defined]
    expires_at = datetime.strptime(decoded["notAfter"], "%b %d %H:%M:%S %Y %Z").replace(tzinfo=UTC)
    remaining = expires_at - datetime.now(UTC)
    return {
        "profile": profile_name,
        "certificate_path": str(cert_path),
        "expires_at": expires_at.isoformat(),
        "days_remaining": remaining.days,
    }


PLUGIN = PluginDefinition(
    manifest=PluginManifest(
        key="ssl",
        name="SSL",
        version="1.0.0",
        description="Issue, renew and inspect SSL certificates using named server-side profiles.",
        permissions=[
            PermissionDefinition(key="ssl.read", description="Read SSL profile metadata and certificate expiry."),
            PermissionDefinition(key="ssl.write", description="Issue and renew SSL certificates from named profiles."),
        ],
        providers=["certbot", "win-acme"],
    ),
    tools={
        "ssl.list_profiles": MCPTool(
            manifest=MCPToolManifest(
                key="ssl.list_profiles",
                name="List SSL Profiles",
                description="List named SSL profiles stored on the server.",
                input_schema={"type": "object", "properties": {}, "additionalProperties": False},
                permissions=["ssl.read"],
                tags=["ssl", "read"],
                read_only=True,
            ),
            handler=list_profiles,
        ),
        "ssl.issue_certificate": MCPTool(
            manifest=MCPToolManifest(
                key="ssl.issue_certificate",
                name="Issue Certificate",
                description="Issue or re-issue a certificate using a named SSL profile.",
                input_schema={
                    "type": "object",
                    "required": ["profile"],
                    "properties": {"profile": {"type": "string"}},
                    "additionalProperties": False,
                },
                permissions=["ssl.write"],
                tags=["ssl", "write"],
                read_only=False,
                providers=["certbot", "win-acme"],
            ),
            handler=issue_certificate,
            availability=_ssl_provider_availability,
        ),
        "ssl.renew_certificate": MCPTool(
            manifest=MCPToolManifest(
                key="ssl.renew_certificate",
                name="Renew Certificate",
                description="Renew certificates using a named SSL profile and the configured provider.",
                input_schema={
                    "type": "object",
                    "required": ["profile"],
                    "properties": {"profile": {"type": "string"}},
                    "additionalProperties": False,
                },
                permissions=["ssl.write"],
                tags=["ssl", "write"],
                read_only=False,
                providers=["certbot", "win-acme"],
            ),
            handler=renew_certificate,
            availability=_ssl_provider_availability,
        ),
        "ssl.check_expiry": MCPTool(
            manifest=MCPToolManifest(
                key="ssl.check_expiry",
                name="Check Certificate Expiry",
                description="Read a certificate from a named profile and report remaining validity.",
                input_schema={
                    "type": "object",
                    "required": ["profile"],
                    "properties": {"profile": {"type": "string"}},
                    "additionalProperties": False,
                },
                permissions=["ssl.read"],
                tags=["ssl", "read"],
                read_only=True,
            ),
            handler=check_expiry,
        ),
    },
)
