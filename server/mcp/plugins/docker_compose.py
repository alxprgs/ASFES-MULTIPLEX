from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from server.mcp.plugins._common import command_result_payload, parse_json_lines, require_argument, string_list_argument
from server.models import MCPTool, MCPToolManifest, PermissionDefinition, PluginDefinition, PluginManifest, RuntimeAvailability, ToolExecutionContext


async def _compose_availability(services) -> RuntimeAvailability:
    host_ops = services.host_ops
    if host_ops.command_exists("docker"):
        try:
            result = await host_ops.run_backend("docker", "compose", "version")
        except Exception:
            result = None
        if result and result.returncode == 0:
            return RuntimeAvailability(available=True, required_backends=["docker"], providers=["docker compose"])
    if host_ops.command_exists("docker-compose"):
        return RuntimeAvailability(available=True, required_backends=["docker-compose"], providers=["docker-compose"])
    return RuntimeAvailability(
        available=False,
        reason="Neither 'docker compose' nor legacy 'docker-compose' is available",
        required_backends=["docker", "docker-compose"],
        providers=["docker compose", "docker-compose"],
    )


def _resolve_project_dir(context: ToolExecutionContext, arguments: dict[str, Any]) -> Path:
    project_dir = require_argument(arguments, "project_dir")
    return context.services.host_ops.resolve_managed_path(str(project_dir), roots=context.services.host_ops.managed_file_roots())


async def _compose_command(context: ToolExecutionContext, arguments: dict[str, Any], *extra: str):
    project_dir = _resolve_project_dir(context, arguments)
    files = string_list_argument(arguments, "files")
    services = string_list_argument(arguments, "services")
    availability = await _compose_availability(context.services)
    if not availability.available:
        raise RuntimeError(availability.reason or "Docker Compose is unavailable")
    if "docker compose" in availability.providers:
        command = [context.services.host_ops.executable_path("docker") or "docker", "compose"]
    else:
        command = [context.services.host_ops.executable_path("docker-compose") or "docker-compose"]
    for compose_file in files:
        resolved = context.services.host_ops.resolve_managed_path(compose_file, roots=context.services.host_ops.managed_file_roots())
        command.extend(["-f", str(resolved)])
    command.extend(extra)
    command.extend(services)
    return await context.services.host_ops.run(command, cwd=project_dir, check=False)


async def compose_ps(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    result = await _compose_command(context, arguments, "ps", "--format", "json")
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "docker compose ps failed")
    try:
        containers = json.loads(result.stdout or "[]")
        if isinstance(containers, dict):
            containers = [containers]
    except json.JSONDecodeError:
        containers = parse_json_lines(result.stdout)
    return command_result_payload(result, services=containers, count=len(containers))


async def compose_config(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    result = await _compose_command(context, arguments, "config")
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "docker compose config failed")
    return command_result_payload(result, config=result.stdout)


async def compose_logs(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    tail_lines = int(arguments.get("tail_lines") or 200)
    result = await _compose_command(context, arguments, "logs", "--tail", str(max(1, tail_lines)))
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "docker compose logs failed")
    return command_result_payload(result, logs=result.stdout, tail_lines=max(1, tail_lines))


async def compose_up(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    extra = ["up"]
    if arguments.get("detach", True):
        extra.append("-d")
    result = await _compose_command(context, arguments, *extra)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "docker compose up failed")
    return command_result_payload(result, changed=True)


async def compose_down(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    result = await _compose_command(context, arguments, "down")
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "docker compose down failed")
    return command_result_payload(result, changed=True)


async def compose_restart(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    result = await _compose_command(context, arguments, "restart")
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "docker compose restart failed")
    return command_result_payload(result, changed=True)


PLUGIN = PluginDefinition(
    manifest=PluginManifest(
        key="docker_compose",
        name="Docker Compose",
        version="1.0.0",
        description="Управляет многоконтейнерными compose-проектами через Docker Compose или docker-compose.",
        permissions=[
            PermissionDefinition(key="docker.compose.read", description="Просматривать Docker Compose-проекты и логи."),
            PermissionDefinition(key="docker.compose.write", description="Запускать, останавливать и перезапускать Docker Compose-проекты."),
        ],
        required_backends=["docker", "docker-compose"],
        providers=["docker compose", "docker-compose"],
    ),
    tools={
        "docker_compose.ps": MCPTool(
            manifest=MCPToolManifest(
                key="docker_compose.ps",
                name="Список Compose-сервисов",
                description="Показывает контейнеры и сервисы, определённые для директории compose-проекта.",
                input_schema={
                    "type": "object",
                    "required": ["project_dir"],
                    "properties": {
                        "project_dir": {"type": "string", "description": "Управляемый путь к директории compose-проекта."},
                        "files": {"type": "array", "items": {"type": "string"}, "description": "Необязательные compose-файлы."},
                        "services": {"type": "array", "items": {"type": "string"}, "description": "Необязательный фильтр по именам сервисов."},
                    },
                    "additionalProperties": False,
                },
                permissions=["docker.compose.read"],
                tags=["docker", "compose", "read"],
                read_only=True,
                required_backends=["docker", "docker-compose"],
                providers=["docker compose", "docker-compose"],
            ),
            handler=compose_ps,
            availability=_compose_availability,
        ),
        "docker_compose.config": MCPTool(
            manifest=MCPToolManifest(
                key="docker_compose.config",
                name="Собрать Compose-конфигурацию",
                description="Формирует итоговую Docker Compose-конфигурацию для директории проекта.",
                input_schema={
                    "type": "object",
                    "required": ["project_dir"],
                    "properties": {
                        "project_dir": {"type": "string"},
                        "files": {"type": "array", "items": {"type": "string"}},
                    },
                    "additionalProperties": False,
                },
                permissions=["docker.compose.read"],
                tags=["docker", "compose", "config"],
                read_only=True,
                required_backends=["docker", "docker-compose"],
                providers=["docker compose", "docker-compose"],
            ),
            handler=compose_config,
            availability=_compose_availability,
        ),
        "docker_compose.logs": MCPTool(
            manifest=MCPToolManifest(
                key="docker_compose.logs",
                name="Читать Compose-логи",
                description="Читает логи всех или выбранных сервисов compose-проекта.",
                input_schema={
                    "type": "object",
                    "required": ["project_dir"],
                    "properties": {
                        "project_dir": {"type": "string"},
                        "files": {"type": "array", "items": {"type": "string"}},
                        "services": {"type": "array", "items": {"type": "string"}},
                        "tail_lines": {"type": "integer"},
                    },
                    "additionalProperties": False,
                },
                permissions=["docker.compose.read"],
                tags=["docker", "compose", "logs"],
                read_only=True,
                required_backends=["docker", "docker-compose"],
                providers=["docker compose", "docker-compose"],
            ),
            handler=compose_logs,
            availability=_compose_availability,
        ),
        "docker_compose.up": MCPTool(
            manifest=MCPToolManifest(
                key="docker_compose.up",
                name="Запустить Compose",
                description="Создаёт или обновляет compose-проект и при необходимости запускает его в фоне.",
                input_schema={
                    "type": "object",
                    "required": ["project_dir"],
                    "properties": {
                        "project_dir": {"type": "string"},
                        "files": {"type": "array", "items": {"type": "string"}},
                        "services": {"type": "array", "items": {"type": "string"}},
                        "detach": {"type": "boolean"},
                    },
                    "additionalProperties": False,
                },
                permissions=["docker.compose.write"],
                tags=["docker", "compose", "write"],
                read_only=False,
                required_backends=["docker", "docker-compose"],
                providers=["docker compose", "docker-compose"],
            ),
            handler=compose_up,
            availability=_compose_availability,
        ),
        "docker_compose.down": MCPTool(
            manifest=MCPToolManifest(
                key="docker_compose.down",
                name="Остановить Compose",
                description="Останавливает и удаляет контейнеры compose-проекта.",
                input_schema={
                    "type": "object",
                    "required": ["project_dir"],
                    "properties": {
                        "project_dir": {"type": "string"},
                        "files": {"type": "array", "items": {"type": "string"}},
                    },
                    "additionalProperties": False,
                },
                permissions=["docker.compose.write"],
                tags=["docker", "compose", "write"],
                read_only=False,
                required_backends=["docker", "docker-compose"],
                providers=["docker compose", "docker-compose"],
            ),
            handler=compose_down,
            availability=_compose_availability,
        ),
        "docker_compose.restart": MCPTool(
            manifest=MCPToolManifest(
                key="docker_compose.restart",
                name="Перезапустить Compose-сервисы",
                description="Перезапускает все или выбранные сервисы внутри compose-проекта.",
                input_schema={
                    "type": "object",
                    "required": ["project_dir"],
                    "properties": {
                        "project_dir": {"type": "string"},
                        "files": {"type": "array", "items": {"type": "string"}},
                        "services": {"type": "array", "items": {"type": "string"}},
                    },
                    "additionalProperties": False,
                },
                permissions=["docker.compose.write"],
                tags=["docker", "compose", "write"],
                read_only=False,
                required_backends=["docker", "docker-compose"],
                providers=["docker compose", "docker-compose"],
            ),
            handler=compose_restart,
            availability=_compose_availability,
        ),
    },
)
