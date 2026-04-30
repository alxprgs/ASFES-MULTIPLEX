from __future__ import annotations

from typing import Any

from server.host_ops import _psutil
from server.mcp.plugins._common import command_result_payload, require_argument, static_availability
from server.models import MCPTool, MCPToolManifest, PermissionDefinition, PluginDefinition, PluginManifest, ToolExecutionContext


def _nginx_roots(context: ToolExecutionContext):
    configured = context.services.host_ops.configured_nginx_paths()
    return configured or context.services.host_ops.managed_file_roots()


def _optional_config_arg(context: ToolExecutionContext, arguments: dict[str, Any]) -> list[str]:
    if not arguments.get("config_path"):
        return []
    config_path = context.services.host_ops.resolve_managed_path(str(arguments["config_path"]), roots=_nginx_roots(context))
    return ["-c", str(config_path)]


async def nginx_status(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    processes = []
    if _psutil is not None:
        for proc in _psutil.process_iter(["pid", "name", "status", "cmdline"]):
            if "nginx" in (proc.info.get("name") or "").lower():
                processes.append(proc.info)
    test_result = await context.services.host_ops.run_backend("nginx", "-t", *_optional_config_arg(context, arguments), check=False)
    return {
        "running": bool(processes),
        "processes": processes,
        "config_ok": test_result.returncode == 0,
        "config_check": test_result.to_dict(),
    }


async def nginx_test_config(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    result = await context.services.host_ops.run_backend("nginx", "-t", *_optional_config_arg(context, arguments), check=False)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "nginx -t failed")
    return command_result_payload(result, valid=True)


async def nginx_control(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    action = str(require_argument(arguments, "action")).lower()
    if action not in {"start", "stop", "reload", "quit"}:
        raise RuntimeError("action must be one of: start, stop, reload, quit")
    if action == "start":
        command = ["nginx", *_optional_config_arg(context, arguments)]
        result = await context.services.host_ops.run(command, check=False)
    else:
        result = await context.services.host_ops.run_backend("nginx", "-s", action, *_optional_config_arg(context, arguments), check=False)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"nginx {action} failed")
    return command_result_payload(result, action=action, changed=True)


async def nginx_list_paths(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    return {"paths": [str(path) for path in _nginx_roots(context)]}


PLUGIN = PluginDefinition(
    manifest=PluginManifest(
        key="nginx",
        name="Nginx",
        version="1.0.0",
        description="Проверяет, просматривает и управляет локальными экземплярами Nginx.",
        permissions=[
            PermissionDefinition(key="nginx.read", description="Читать статус Nginx и управляемые пути конфигурации."),
            PermissionDefinition(key="nginx.write", description="Проверять и управлять локальным процессом Nginx."),
        ],
        required_backends=["nginx"],
    ),
    tools={
        "nginx.status": MCPTool(
            manifest=MCPToolManifest(
                key="nginx.status",
                name="Статус Nginx",
                description="Проверяет, запущен ли Nginx, и валидирует конфигурацию.",
                input_schema={
                    "type": "object",
                    "properties": {"config_path": {"type": "string"}},
                    "additionalProperties": False,
                },
                permissions=["nginx.read"],
                tags=["nginx", "read"],
                read_only=True,
                required_backends=["nginx"],
            ),
            handler=nginx_status,
            availability=static_availability(backend="nginx"),
        ),
        "nginx.test_config": MCPTool(
            manifest=MCPToolManifest(
                key="nginx.test_config",
                name="Проверить конфигурацию Nginx",
                description="Запускает nginx -t для стандартного или явно указанного управляемого пути конфигурации.",
                input_schema={
                    "type": "object",
                    "properties": {"config_path": {"type": "string"}},
                    "additionalProperties": False,
                },
                permissions=["nginx.write"],
                tags=["nginx", "write"],
                read_only=False,
                required_backends=["nginx"],
            ),
            handler=nginx_test_config,
            availability=static_availability(backend="nginx"),
        ),
        "nginx.control": MCPTool(
            manifest=MCPToolManifest(
                key="nginx.control",
                name="Управление Nginx",
                description="Запускает, останавливает, перезагружает или мягко завершает локальный процесс Nginx.",
                input_schema={
                    "type": "object",
                    "required": ["action"],
                    "properties": {
                        "action": {"type": "string", "enum": ["start", "stop", "reload", "quit"]},
                        "config_path": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
                permissions=["nginx.write"],
                tags=["nginx", "write"],
                read_only=False,
                required_backends=["nginx"],
            ),
            handler=nginx_control,
            availability=static_availability(backend="nginx"),
        ),
        "nginx.list_paths": MCPTool(
            manifest=MCPToolManifest(
                key="nginx.list_paths",
                name="Список путей Nginx",
                description="Показывает настроенные управляемые корни конфигурации Nginx, известные серверу.",
                input_schema={"type": "object", "properties": {}, "additionalProperties": False},
                permissions=["nginx.read"],
                tags=["nginx", "read"],
                read_only=True,
            ),
            handler=nginx_list_paths,
        ),
    },
)
