from __future__ import annotations

import asyncio
import json
import shutil
from typing import Any

from server.mcp.plugins._common import command_result_payload, int_argument, parse_json_lines, require_argument, static_availability
from server.models import MCPTool, MCPToolManifest, PermissionDefinition, PluginDefinition, PluginManifest, ToolExecutionContext


def _docker_executable() -> str:
    executable = shutil.which("docker")
    if not executable:
        raise RuntimeError("Docker CLI is not installed or not available in PATH")
    return executable


async def _run_docker_command(*args: str) -> tuple[int, str, str]:
    executable = _docker_executable()
    process = await asyncio.create_subprocess_exec(
        executable,
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    return process.returncode, stdout.decode("utf-8", errors="replace"), stderr.decode("utf-8", errors="replace")


async def list_containers(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    command = ["ps", "--format", "{{json .}}"]
    if arguments.get("all"):
        command.insert(1, "-a")
    returncode, stdout, stderr = await _run_docker_command(*command)
    if returncode != 0:
        raise RuntimeError(stderr.strip() or "docker ps failed")
    containers = parse_json_lines(stdout)
    return {"containers": containers, "count": len(containers)}


async def restart_container(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    container = require_argument(arguments, "container")
    returncode, stdout, stderr = await _run_docker_command("restart", str(container))
    if returncode != 0:
        raise RuntimeError(stderr.strip() or "docker restart failed")
    restarted = [line.strip() for line in stdout.splitlines() if line.strip()]
    return {"restarted": restarted or [container], "requested_by": context.user.username}


async def start_container(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    container = require_argument(arguments, "container")
    returncode, stdout, stderr = await _run_docker_command("start", str(container))
    if returncode != 0:
        raise RuntimeError(stderr.strip() or "docker start failed")
    started = [line.strip() for line in stdout.splitlines() if line.strip()]
    return {"started": started or [container], "requested_by": context.user.username}


async def stop_container(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    container = require_argument(arguments, "container")
    returncode, stdout, stderr = await _run_docker_command("stop", str(container))
    if returncode != 0:
        raise RuntimeError(stderr.strip() or "docker stop failed")
    stopped = [line.strip() for line in stdout.splitlines() if line.strip()]
    return {"stopped": stopped or [container], "requested_by": context.user.username}


async def container_logs(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    container = require_argument(arguments, "container")
    tail_lines = min(max(1, int_argument(arguments, "tail_lines", 200)), 500)
    returncode, stdout, stderr = await _run_docker_command("logs", "--tail", str(max(1, tail_lines)), str(container))
    if returncode != 0:
        raise RuntimeError(stderr.strip() or "docker logs failed")
    return {"container": container, "tail_lines": max(1, tail_lines), "logs": stdout}


async def inspect_container(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    container = require_argument(arguments, "container")
    returncode, stdout, stderr = await _run_docker_command("inspect", str(container))
    if returncode != 0:
        raise RuntimeError(stderr.strip() or "docker inspect failed")
    inspected = [_redact_inspect_item(item) for item in json.loads(stdout or "[]")]
    return {"container": container, "inspect": inspected}


def _redact_inspect_item(item: dict[str, Any]) -> dict[str, Any]:
    redacted = json.loads(json.dumps(item))
    config = redacted.get("Config") or {}
    if isinstance(config.get("Env"), list):
        config["Env"] = [_redact_env_value(value) for value in config["Env"]]
    for section_name in ("Config", "ContainerConfig"):
        section = redacted.get(section_name) or {}
        labels = section.get("Labels")
        if isinstance(labels, dict):
            for key in list(labels):
                if _looks_sensitive(key):
                    labels[key] = "[REDACTED]"
    return redacted


def _redact_env_value(value: str) -> str:
    name, separator, raw = str(value).partition("=")
    if separator and _looks_sensitive(name):
        return f"{name}=[REDACTED]"
    return str(value)


def _looks_sensitive(name: str) -> bool:
    lowered = name.lower()
    return any(marker in lowered for marker in ("password", "passwd", "secret", "token", "key", "credential"))


async def container_stats(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    container = require_argument(arguments, "container")
    returncode, stdout, stderr = await _run_docker_command(
        "stats",
        "--no-stream",
        "--format",
        "{{json .}}",
        str(container),
    )
    if returncode != 0:
        raise RuntimeError(stderr.strip() or "docker stats failed")
    stats = parse_json_lines(stdout)
    return {"container": container, "stats": stats[0] if stats else {}}


PLUGIN = PluginDefinition(
    manifest=PluginManifest(
        key="docker",
        name="Docker",
        version="1.1.0",
        description="Управляет локальными Docker-контейнерами через защищённые MCP-инструменты.",
        permissions=[
            PermissionDefinition(key="docker.containers.read", description="Читать статус, логи и метрики Docker-контейнеров."),
            PermissionDefinition(key="docker.containers.start", description="Запускать Docker-контейнеры с MCP-сервера."),
            PermissionDefinition(key="docker.containers.stop", description="Останавливать Docker-контейнеры с MCP-сервера."),
            PermissionDefinition(key="docker.containers.restart", description="Перезапускать Docker-контейнеры с MCP-сервера."),
        ],
        required_backends=["docker"],
    ),
    tools={
        "docker.list_containers": MCPTool(
            manifest=MCPToolManifest(
                key="docker.list_containers",
                name="Список Docker-контейнеров",
                description="Показывает контейнеры, видимые локальному Docker daemon. Поддерживает вывод только запущенных или всех контейнеров.",
                input_schema={
                    "type": "object",
                    "properties": {
                        "all": {
                            "type": "boolean",
                            "description": "Set to true to include stopped containers. Defaults to false for only running containers.",
                        }
                    },
                    "additionalProperties": False,
                },
                permissions=["docker.containers.read"],
                tags=["docker", "containers", "read"],
                read_only=True,
                default_global_enabled=False,
                required_backends=["docker"],
            ),
            handler=list_containers,
            availability=static_availability(backend="docker"),
        ),
        "docker.inspect_container": MCPTool(
            manifest=MCPToolManifest(
                key="docker.inspect_container",
                name="Инспекция Docker-контейнера",
                description="Возвращает исходные данные Docker inspect для контейнера по имени или ID.",
                input_schema={
                    "type": "object",
                    "required": ["container"],
                    "properties": {"container": {"type": "string", "description": "Docker container name or container ID."}},
                    "additionalProperties": False,
                },
                permissions=["docker.containers.read"],
                tags=["docker", "containers", "inspect"],
                read_only=True,
                default_global_enabled=False,
                required_backends=["docker"],
            ),
            handler=inspect_container,
            availability=static_availability(backend="docker"),
        ),
        "docker.container_logs": MCPTool(
            manifest=MCPToolManifest(
                key="docker.container_logs",
                name="Читать логи Docker-контейнера",
                description="Читает последние логи Docker-контейнера без потоковой передачи.",
                input_schema={
                    "type": "object",
                    "required": ["container"],
                    "properties": {
                        "container": {"type": "string", "description": "Docker container name or ID."},
                        "tail_lines": {"type": "integer", "description": "Maximum number of log lines to return."},
                    },
                    "additionalProperties": False,
                },
                permissions=["docker.containers.read"],
                tags=["docker", "containers", "logs"],
                read_only=True,
                default_global_enabled=False,
                required_backends=["docker"],
            ),
            handler=container_logs,
            availability=static_availability(backend="docker"),
        ),
        "docker.container_stats": MCPTool(
            manifest=MCPToolManifest(
                key="docker.container_stats",
                name="Статистика Docker-контейнера",
                description="Возвращает одиночный снимок Docker stats для контейнера без потоковой передачи.",
                input_schema={
                    "type": "object",
                    "required": ["container"],
                    "properties": {"container": {"type": "string", "description": "Docker container name or ID."}},
                    "additionalProperties": False,
                },
                permissions=["docker.containers.read"],
                tags=["docker", "containers", "stats"],
                read_only=True,
                default_global_enabled=False,
                required_backends=["docker"],
            ),
            handler=container_stats,
            availability=static_availability(backend="docker"),
        ),
        "docker.start_container": MCPTool(
            manifest=MCPToolManifest(
                key="docker.start_container",
                name="Запустить Docker-контейнер",
                description="Запускает конкретный Docker-контейнер по имени или ID.",
                input_schema={
                    "type": "object",
                    "required": ["container"],
                    "properties": {"container": {"type": "string", "description": "Docker container name or ID."}},
                    "additionalProperties": False,
                },
                permissions=["docker.containers.start"],
                tags=["docker", "containers", "write"],
                read_only=False,
                default_global_enabled=False,
                required_backends=["docker"],
            ),
            handler=start_container,
            availability=static_availability(backend="docker"),
        ),
        "docker.stop_container": MCPTool(
            manifest=MCPToolManifest(
                key="docker.stop_container",
                name="Остановить Docker-контейнер",
                description="Останавливает конкретный Docker-контейнер по имени или ID.",
                input_schema={
                    "type": "object",
                    "required": ["container"],
                    "properties": {"container": {"type": "string", "description": "Docker container name or ID."}},
                    "additionalProperties": False,
                },
                permissions=["docker.containers.stop"],
                tags=["docker", "containers", "write"],
                read_only=False,
                default_global_enabled=False,
                required_backends=["docker"],
            ),
            handler=stop_container,
            availability=static_availability(backend="docker"),
        ),
        "docker.restart_container": MCPTool(
            manifest=MCPToolManifest(
                key="docker.restart_container",
                name="Перезапустить Docker-контейнер",
                description="Перезапускает конкретный Docker-контейнер по имени или ID. Вызывайте только при явном запросе на перезапуск контейнера.",
                input_schema={
                    "type": "object",
                    "required": ["container"],
                    "properties": {
                        "container": {
                            "type": "string",
                            "description": "Docker container name or container ID to restart.",
                        }
                    },
                    "additionalProperties": False,
                },
                permissions=["docker.containers.restart"],
                tags=["docker", "containers", "write"],
                read_only=False,
                default_global_enabled=False,
                required_backends=["docker"],
            ),
            handler=restart_container,
            availability=static_availability(backend="docker"),
        ),
    },
)
