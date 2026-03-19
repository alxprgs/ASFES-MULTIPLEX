from __future__ import annotations

import asyncio
import json
import shutil
from typing import Any

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
    containers = [json.loads(line) for line in stdout.splitlines() if line.strip()]
    return {"containers": containers, "count": len(containers)}


async def restart_container(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    container = arguments.get("container")
    if not container:
        raise RuntimeError("The 'container' argument is required")
    returncode, stdout, stderr = await _run_docker_command("restart", str(container))
    if returncode != 0:
        raise RuntimeError(stderr.strip() or "docker restart failed")
    restarted = [line.strip() for line in stdout.splitlines() if line.strip()]
    return {"restarted": restarted or [container], "requested_by": context.user.username}


PLUGIN = PluginDefinition(
    manifest=PluginManifest(
        key="docker",
        name="Docker",
        version="1.0.0",
        description="Manage local Docker containers through guarded MCP tools.",
        permissions=[
            PermissionDefinition(key="docker.containers.read", description="Read the list of running or stopped Docker containers."),
            PermissionDefinition(key="docker.containers.restart", description="Restart Docker containers from the MCP server."),
        ],
    ),
    tools={
        "docker.list_containers": MCPTool(
            manifest=MCPToolManifest(
                key="docker.list_containers",
                name="List Docker Containers",
                description="Use this to inspect containers visible to the local Docker daemon. Supports listing only running containers or all containers.",
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
            ),
            handler=list_containers,
        ),
        "docker.restart_container": MCPTool(
            manifest=MCPToolManifest(
                key="docker.restart_container",
                name="Restart Docker Container",
                description="Use this to restart a specific Docker container by name or ID. Only call this when a container restart is explicitly requested.",
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
            ),
            handler=restart_container,
        ),
    },
)
