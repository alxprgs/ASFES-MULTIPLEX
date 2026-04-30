from __future__ import annotations

from typing import Any

from server.mcp.plugins._common import require_argument
from server.models import MCPTool, MCPToolManifest, PermissionDefinition, PluginDefinition, PluginManifest, RuntimeAvailability, ToolExecutionContext


async def _firewall_availability(services) -> RuntimeAvailability:
    if services.host_ops.is_linux:
        return services.host_ops.availability_for_command("ufw", providers=["ufw"])
    return services.host_ops.availability_for_command("netsh", providers=["netsh"])


async def list_rules(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    if context.services.host_ops.is_linux:
        result = await context.services.host_ops.run_backend("ufw", "status", "numbered", check=False)
    else:
        result = await context.services.host_ops.run_backend("netsh", "advfirewall", "firewall", "show", "rule", "name=all", check=False)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "firewall status command failed")
    return {"rules_text": result.stdout, **result.to_dict()}


async def set_enabled(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    enabled = bool(arguments.get("enabled", True))
    if context.services.host_ops.is_linux:
        result = await context.services.host_ops.run_backend("ufw", "--force", "enable" if enabled else "disable", check=False)
    else:
        result = await context.services.host_ops.run_backend(
            "netsh",
            "advfirewall",
            "set",
            "allprofiles",
            "state",
            "on" if enabled else "off",
            check=False,
        )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "firewall enable/disable failed")
    return {"enabled": enabled, "changed": True, **result.to_dict()}


async def upsert_rule(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    name = str(require_argument(arguments, "name"))
    port = str(require_argument(arguments, "port"))
    protocol = str(arguments.get("protocol") or "tcp").lower()
    direction = str(arguments.get("direction") or "in").lower()
    action = str(arguments.get("action") or "allow").lower()
    if context.services.host_ops.is_linux:
        result = await context.services.host_ops.run_backend("ufw", action, direction, f"{port}/{protocol}", check=False)
    else:
        mapped_action = "allow" if action == "allow" else "block"
        mapped_direction = "in" if direction == "in" else "out"
        result = await context.services.host_ops.run_backend(
            "netsh",
            "advfirewall",
            "firewall",
            "add",
            "rule",
            f"name={name}",
            f"dir={mapped_direction}",
            f"action={mapped_action}",
            f"protocol={protocol.upper()}",
            f"localport={port}",
            check=False,
        )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "firewall rule update failed")
    return {"name": name, "port": port, "protocol": protocol, "direction": direction, "action": action, **result.to_dict()}


async def delete_rule(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    name = str(arguments.get("name") or "")
    port = str(arguments.get("port") or "")
    protocol = str(arguments.get("protocol") or "tcp").lower()
    direction = str(arguments.get("direction") or "in").lower()
    action = str(arguments.get("action") or "allow").lower()
    if context.services.host_ops.is_linux:
        if not port:
            raise RuntimeError("Linux firewall rule deletion requires 'port'")
        result = await context.services.host_ops.run_backend("ufw", "delete", action, direction, f"{port}/{protocol}", check=False)
    else:
        if not name:
            raise RuntimeError("Windows firewall rule deletion requires 'name'")
        result = await context.services.host_ops.run_backend("netsh", "advfirewall", "firewall", "delete", "rule", f"name={name}", check=False)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "firewall rule delete failed")
    return {"name": name or None, "port": port or None, "deleted": True, **result.to_dict()}


PLUGIN = PluginDefinition(
    manifest=PluginManifest(
        key="firewall",
        name="Файрвол",
        version="1.0.0",
        description="Просматривает и управляет локальными правилами файрвола через ufw на Linux и netsh на Windows.",
        permissions=[
            PermissionDefinition(key="firewall.read", description="Читать статус и правила файрвола."),
            PermissionDefinition(key="firewall.write", description="Включать, отключать и изменять правила файрвола."),
        ],
        required_backends=["ufw", "netsh"],
        providers=["ufw", "netsh"],
    ),
    tools={
        "firewall.list_rules": MCPTool(
            manifest=MCPToolManifest(
                key="firewall.list_rules",
                name="Список правил файрвола",
                description="Читает текущий набор правил файрвола через backend текущей платформы.",
                input_schema={"type": "object", "properties": {}, "additionalProperties": False},
                permissions=["firewall.read"],
                tags=["firewall", "read"],
                read_only=True,
                required_backends=["ufw", "netsh"],
                providers=["ufw", "netsh"],
            ),
            handler=list_rules,
            availability=_firewall_availability,
        ),
        "firewall.set_enabled": MCPTool(
            manifest=MCPToolManifest(
                key="firewall.set_enabled",
                name="Включить или отключить файрвол",
                description="Включает или отключает backend файрвола на текущем хосте.",
                input_schema={
                    "type": "object",
                    "properties": {"enabled": {"type": "boolean"}},
                    "additionalProperties": False,
                },
                permissions=["firewall.write"],
                tags=["firewall", "write"],
                read_only=False,
                required_backends=["ufw", "netsh"],
                providers=["ufw", "netsh"],
            ),
            handler=set_enabled,
            availability=_firewall_availability,
        ),
        "firewall.upsert_rule": MCPTool(
            manifest=MCPToolManifest(
                key="firewall.upsert_rule",
                name="Создать правило файрвола",
                description="Создаёт или обновляет правило файрвола для локального порта.",
                input_schema={
                    "type": "object",
                    "required": ["name", "port"],
                    "properties": {
                        "name": {"type": "string"},
                        "port": {"type": "integer"},
                        "protocol": {"type": "string"},
                        "direction": {"type": "string"},
                        "action": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
                permissions=["firewall.write"],
                tags=["firewall", "write"],
                read_only=False,
                required_backends=["ufw", "netsh"],
                providers=["ufw", "netsh"],
            ),
            handler=upsert_rule,
            availability=_firewall_availability,
        ),
        "firewall.delete_rule": MCPTool(
            manifest=MCPToolManifest(
                key="firewall.delete_rule",
                name="Удалить правило файрвола",
                description="Удаляет правило файрвола по управляемому имени на Windows или по спецификации правила на Linux.",
                input_schema={
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "port": {"type": "integer"},
                        "protocol": {"type": "string"},
                        "direction": {"type": "string"},
                        "action": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
                permissions=["firewall.write"],
                tags=["firewall", "write"],
                read_only=False,
                required_backends=["ufw", "netsh"],
                providers=["ufw", "netsh"],
            ),
            handler=delete_rule,
            availability=_firewall_availability,
        ),
    },
)
