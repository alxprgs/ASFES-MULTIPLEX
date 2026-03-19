from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from server.mcp.plugins._common import require_argument
from server.models import MCPTool, MCPToolManifest, PermissionDefinition, PluginDefinition, PluginManifest, RuntimeAvailability, ToolExecutionContext


async def _vpn_control_availability(services) -> RuntimeAvailability:
    if services.host_ops.is_linux:
        return services.host_ops.availability_for_any_command(["wg-quick", "openvpn", "systemctl"], providers=["wireguard", "openvpn"])
    return services.host_ops.availability_for_any_command(["sc", "wireguard"], providers=["windows-service", "wireguard"])


def _profile_dir(context: ToolExecutionContext) -> Path:
    directory = context.services.host_ops.profile_directory("vpn")
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _load_profile(context: ToolExecutionContext, name: str) -> dict[str, Any]:
    return context.services.host_ops.load_json_profile("vpn", name)


async def list_profiles(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    return {"profiles": context.services.host_ops.list_profiles("vpn")}


async def import_profile(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    name = str(require_argument(arguments, "name"))
    vpn_type = str(require_argument(arguments, "vpn_type")).lower()
    source_path = context.services.host_ops.resolve_managed_path(str(require_argument(arguments, "source_path")), roots=context.services.host_ops.managed_file_roots())
    target_dir = _profile_dir(context)
    target_config = target_dir / f"{name}{source_path.suffix or '.conf'}"
    target_config.write_text(source_path.read_text(encoding="utf-8", errors="replace"), encoding="utf-8")
    profile_path = target_dir / f"{name}.json"
    document = {
        "name": name,
        "vpn_type": vpn_type,
        "config_path": str(target_config),
        "service_name": arguments.get("service_name"),
        "metadata": {"imported_from": str(source_path)},
    }
    profile_path.write_text(json.dumps(document, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"name": name, "profile_path": str(profile_path), "config_path": str(target_config), "imported": True}


async def remove_profile(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    name = str(require_argument(arguments, "name"))
    profile = _load_profile(context, name)
    profile_path = _profile_dir(context) / f"{name}.json"
    if profile_path.exists():
        profile_path.unlink()
    config_path = Path(str(profile.get("config_path") or ""))
    if config_path.exists() and config_path.parent == _profile_dir(context):
        config_path.unlink()
    return {"name": name, "removed": True}


async def vpn_status(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    name = str(require_argument(arguments, "name"))
    profile = _load_profile(context, name)
    vpn_type = str(profile.get("vpn_type") or "").lower()
    service_name = str(profile.get("service_name") or name)
    if context.services.host_ops.is_windows:
        result = await context.services.host_ops.run_backend("sc", "query", service_name, check=False)
        return {"name": name, "service_name": service_name, "vpn_type": vpn_type, "running": "RUNNING" in result.stdout, **result.to_dict()}
    if vpn_type == "wireguard" and context.services.host_ops.command_exists("wg"):
        result = await context.services.host_ops.run_backend("wg", "show", check=False)
        return {"name": name, "vpn_type": vpn_type, "running": service_name in result.stdout or name in result.stdout, **result.to_dict()}
    if profile.get("service_name") and context.services.host_ops.command_exists("systemctl"):
        result = await context.services.host_ops.run_backend("systemctl", "status", service_name, check=False)
        return {"name": name, "service_name": service_name, "vpn_type": vpn_type, "running": result.returncode == 0, **result.to_dict()}
    raise RuntimeError("Unable to determine VPN status for this profile")


async def vpn_control(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    name = str(require_argument(arguments, "name"))
    action = str(require_argument(arguments, "action")).lower()
    if action not in {"start", "stop", "restart"}:
        raise RuntimeError("action must be one of: start, stop, restart")
    profile = _load_profile(context, name)
    vpn_type = str(profile.get("vpn_type") or "").lower()
    service_name = str(profile.get("service_name") or name)
    config_path = str(profile.get("config_path") or "")
    if context.services.host_ops.is_windows:
        mapped = "start" if action == "start" else "stop"
        if action == "restart":
            stop_result = await context.services.host_ops.run_backend("sc", "stop", service_name, check=False)
            start_result = await context.services.host_ops.run_backend("sc", "start", service_name, check=False)
            return {"name": name, "action": action, "stop": stop_result.to_dict(), "start": start_result.to_dict()}
        result = await context.services.host_ops.run_backend("sc", mapped, service_name, check=False)
        return {"name": name, "action": action, **result.to_dict()}
    if vpn_type == "wireguard" and context.services.host_ops.command_exists("wg-quick"):
        if action == "restart":
            down_result = await context.services.host_ops.run_backend("wg-quick", "down", config_path, check=False)
            up_result = await context.services.host_ops.run_backend("wg-quick", "up", config_path, check=False)
            return {"name": name, "action": action, "down": down_result.to_dict(), "up": up_result.to_dict()}
        result = await context.services.host_ops.run_backend("wg-quick", "up" if action == "start" else "down", config_path, check=False)
        return {"name": name, "action": action, **result.to_dict()}
    if profile.get("service_name") and context.services.host_ops.command_exists("systemctl"):
        command_action = "restart" if action == "restart" else action
        result = await context.services.host_ops.run_backend("systemctl", command_action, service_name, check=False)
        return {"name": name, "action": action, **result.to_dict()}
    raise RuntimeError("No supported VPN control backend is available for this profile")


PLUGIN = PluginDefinition(
    manifest=PluginManifest(
        key="vpn",
        name="VPN",
        version="1.0.0",
        description="Manage WireGuard or OpenVPN profiles stored on the server.",
        permissions=[
            PermissionDefinition(key="vpn.read", description="Read VPN profile metadata and status."),
            PermissionDefinition(key="vpn.write", description="Import, remove and control VPN profiles."),
        ],
        providers=["wireguard", "openvpn"],
    ),
    tools={
        "vpn.list_profiles": MCPTool(
            manifest=MCPToolManifest(
                key="vpn.list_profiles",
                name="List VPN Profiles",
                description="List named VPN profiles stored on the server.",
                input_schema={"type": "object", "properties": {}, "additionalProperties": False},
                permissions=["vpn.read"],
                tags=["vpn", "read"],
                read_only=True,
            ),
            handler=list_profiles,
        ),
        "vpn.import_profile": MCPTool(
            manifest=MCPToolManifest(
                key="vpn.import_profile",
                name="Import VPN Profile",
                description="Import a VPN config from managed storage into the server-side VPN profile directory.",
                input_schema={
                    "type": "object",
                    "required": ["name", "vpn_type", "source_path"],
                    "properties": {
                        "name": {"type": "string"},
                        "vpn_type": {"type": "string"},
                        "source_path": {"type": "string"},
                        "service_name": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
                permissions=["vpn.write"],
                tags=["vpn", "write"],
                read_only=False,
            ),
            handler=import_profile,
        ),
        "vpn.remove_profile": MCPTool(
            manifest=MCPToolManifest(
                key="vpn.remove_profile",
                name="Remove VPN Profile",
                description="Delete a stored VPN profile and its imported config copy.",
                input_schema={
                    "type": "object",
                    "required": ["name"],
                    "properties": {"name": {"type": "string"}},
                    "additionalProperties": False,
                },
                permissions=["vpn.write"],
                tags=["vpn", "write"],
                read_only=False,
            ),
            handler=remove_profile,
        ),
        "vpn.status": MCPTool(
            manifest=MCPToolManifest(
                key="vpn.status",
                name="VPN Status",
                description="Inspect the runtime status of a named VPN profile.",
                input_schema={
                    "type": "object",
                    "required": ["name"],
                    "properties": {"name": {"type": "string"}},
                    "additionalProperties": False,
                },
                permissions=["vpn.read"],
                tags=["vpn", "read"],
                read_only=True,
                providers=["wireguard", "openvpn"],
            ),
            handler=vpn_status,
            availability=_vpn_control_availability,
        ),
        "vpn.control": MCPTool(
            manifest=MCPToolManifest(
                key="vpn.control",
                name="Control VPN",
                description="Start, stop or restart a named VPN profile.",
                input_schema={
                    "type": "object",
                    "required": ["name", "action"],
                    "properties": {
                        "name": {"type": "string"},
                        "action": {"type": "string", "enum": ["start", "stop", "restart"]},
                    },
                    "additionalProperties": False,
                },
                permissions=["vpn.write"],
                tags=["vpn", "write"],
                read_only=False,
                providers=["wireguard", "openvpn"],
            ),
            handler=vpn_control,
            availability=_vpn_control_availability,
        ),
    },
)
