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
    tail_lines = int_argument(arguments, "tail_lines", 200)
    returncode, stdout, stderr = await _run_docker_command("logs", "--tail", str(max(1, tail_lines)), str(container))
    if returncode != 0:
        raise RuntimeError(stderr.strip() or "docker logs failed")
    return {"container": container, "tail_lines": max(1, tail_lines), "logs": stdout}


async def inspect_container(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    container = require_argument(arguments, "container")
    returncode, stdout, stderr = await _run_docker_command("inspect", str(container))
    if returncode != 0:
        raise RuntimeError(stderr.strip() or "docker inspect failed")
    inspected = json.loads(stdout or "[]")
    return {"container": container, "inspect": inspected}


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
        description="Manage local Docker containers through guarded MCP tools.",
        permissions=[
            PermissionDefinition(key="docker.containers.read", description="Read Docker container status, logs and runtime metrics."),
            PermissionDefinition(key="docker.containers.start", description="Start Docker containers from the MCP server."),
            PermissionDefinition(key="docker.containers.stop", description="Stop Docker containers from the MCP server."),
            PermissionDefinition(key="docker.containers.restart", description="Restart Docker containers from the MCP server."),
        ],
        required_backends=["docker"],
    ),
    tools={
        "docker.list_containers": MCPTool(
            manifest=MCPToolManifest(
                key="docker.list_containers",
                name="List Docker Containers",
                description="Inspect containers visible to the local Docker daemon. Supports listing only running containers or all containers.",
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
                default_global_enabled=True,
                required_backends=["docker"],
            ),
            handler=list_containers,
            availability=static_availability(backend="docker"),
        ),
        "docker.inspect_container": MCPTool(
            manifest=MCPToolManifest(
                key="docker.inspect_container",
                name="Inspect Docker Container",
                description="Retrieve the raw Docker inspect payload for a container by name or ID.",
                input_schema={
                    "type": "object",
                    "required": ["container"],
                    "properties": {"container": {"type": "string", "description": "Docker container name or container ID."}},
                    "additionalProperties": False,
                },
                permissions=["docker.containers.read"],
                tags=["docker", "containers", "inspect"],
                read_only=True,
                default_global_enabled=True,
                required_backends=["docker"],
            ),
            handler=inspect_container,
            availability=static_availability(backend="docker"),
        ),
        "docker.container_logs": MCPTool(
            manifest=MCPToolManifest(
                key="docker.container_logs",
                name="Read Docker Container Logs",
                description="Read the latest logs from a Docker container without streaming.",
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
                default_global_enabled=True,
                required_backends=["docker"],
            ),
            handler=container_logs,
            availability=static_availability(backend="docker"),
        ),
        "docker.container_stats": MCPTool(
            manifest=MCPToolManifest(
                key="docker.container_stats",
                name="Get Docker Container Stats",
                description="Get a single no-stream Docker stats snapshot for a container.",
                input_schema={
                    "type": "object",
                    "required": ["container"],
                    "properties": {"container": {"type": "string", "description": "Docker container name or ID."}},
                    "additionalProperties": False,
                },
                permissions=["docker.containers.read"],
                tags=["docker", "containers", "stats"],
                read_only=True,
                default_global_enabled=True,
                required_backends=["docker"],
            ),
            handler=container_stats,
            availability=static_availability(backend="docker"),
        ),
        "docker.start_container": MCPTool(
            manifest=MCPToolManifest(
                key="docker.start_container",
                name="Start Docker Container",
                description="Start a specific Docker container by name or ID.",
                input_schema={
                    "type": "object",
                    "required": ["container"],
                    "properties": {"container": {"type": "string", "description": "Docker container name or ID."}},
                    "additionalProperties": False,
                },
                permissions=["docker.containers.start"],
                tags=["docker", "containers", "write"],
                read_only=False,
                default_global_enabled=True,
                required_backends=["docker"],
            ),
            handler=start_container,
            availability=static_availability(backend="docker"),
        ),
        "docker.stop_container": MCPTool(
            manifest=MCPToolManifest(
                key="docker.stop_container",
                name="Stop Docker Container",
                description="Stop a specific Docker container by name or ID.",
                input_schema={
                    "type": "object",
                    "required": ["container"],
                    "properties": {"container": {"type": "string", "description": "Docker container name or ID."}},
                    "additionalProperties": False,
                },
                permissions=["docker.containers.stop"],
                tags=["docker", "containers", "write"],
                read_only=False,
                default_global_enabled=True,
                required_backends=["docker"],
            ),
            handler=stop_container,
            availability=static_availability(backend="docker"),
        ),
        "docker.restart_container": MCPTool(
            manifest=MCPToolManifest(
                key="docker.restart_container",
                name="Restart Docker Container",
                description="Restart a specific Docker container by name or ID. Only call this when a container restart is explicitly requested.",
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
                default_global_enabled=True,
                required_backends=["docker"],
            ),
            handler=restart_container,
            availability=static_availability(backend="docker"),
        ),
    },
)
