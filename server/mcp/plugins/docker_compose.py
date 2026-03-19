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
        description="Manage multi-container compose projects through Docker Compose or docker-compose.",
        permissions=[
            PermissionDefinition(key="docker.compose.read", description="Inspect Docker Compose projects and logs."),
            PermissionDefinition(key="docker.compose.write", description="Start, stop and restart Docker Compose projects."),
        ],
        required_backends=["docker", "docker-compose"],
        providers=["docker compose", "docker-compose"],
    ),
    tools={
        "docker_compose.ps": MCPTool(
            manifest=MCPToolManifest(
                key="docker_compose.ps",
                name="List Compose Services",
                description="List containers and services defined for a compose project directory.",
                input_schema={
                    "type": "object",
                    "required": ["project_dir"],
                    "properties": {
                        "project_dir": {"type": "string", "description": "Managed path to the compose project directory."},
                        "files": {"type": "array", "items": {"type": "string"}, "description": "Optional compose files."},
                        "services": {"type": "array", "items": {"type": "string"}, "description": "Optional service names filter."},
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
                name="Render Compose Config",
                description="Render the effective Docker Compose configuration for a project directory.",
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
                name="Read Compose Logs",
                description="Read logs from all or selected services in a compose project.",
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
                name="Compose Up",
                description="Create or update a compose project and optionally detach.",
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
                name="Compose Down",
                description="Stop and remove containers for a compose project.",
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
                name="Restart Compose Services",
                description="Restart all or selected services inside a compose project.",
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
