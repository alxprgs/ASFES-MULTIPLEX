from __future__ import annotations

import json
from typing import Any

from server.mcp.plugins._common import require_argument, static_availability
from server.models import MCPTool, MCPToolManifest, PermissionDefinition, PluginDefinition, PluginManifest, RuntimeAvailability, ToolExecutionContext


async def _system_log_availability(services) -> RuntimeAvailability:
    if services.host_ops.is_linux:
        return services.host_ops.availability_for_command("journalctl", providers=["journalctl"])
    return services.host_ops.availability_for_any_command(["powershell", "wevtutil"], providers=["powershell", "wevtutil"])


async def read_file_logs(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    path = require_argument(arguments, "path")
    tail_lines = max(1, int(arguments.get("tail_lines") or 200))
    return context.services.host_ops.tail_text(str(path), roots=context.services.host_ops.managed_log_roots(), tail_lines=tail_lines)


async def read_system_logs(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    tail_lines = max(1, int(arguments.get("tail_lines") or 200))
    if context.services.host_ops.is_linux:
        command = ["journalctl", "-n", str(tail_lines), "--no-pager"]
        if arguments.get("unit"):
            command.extend(["-u", str(arguments["unit"])])
        result = await context.services.host_ops.run(command, check=False)
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "journalctl failed")
        return {"source": arguments.get("unit") or "journalctl", "logs": result.stdout, **result.to_dict()}

    if context.services.host_ops.command_exists("powershell"):
        script = (
            f"Get-WinEvent -LogName System -MaxEvents {tail_lines} | "
            "Select-Object TimeCreated,ProviderName,Id,LevelDisplayName,Message | "
            "ConvertTo-Json -Depth 3"
        )
        result = await context.services.host_ops.run_backend("powershell", "-NoProfile", "-Command", script, check=False)
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "Get-WinEvent failed")
        data = json.loads(result.stdout or "[]")
        if isinstance(data, dict):
            data = [data]
        return {"source": "windows-event-log", "entries": data, **result.to_dict()}

    result = await context.services.host_ops.run_backend("wevtutil", "qe", "System", f"/c:{tail_lines}", "/f:text", check=False)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "wevtutil failed")
    return {"source": "windows-event-log", "logs": result.stdout, **result.to_dict()}


async def read_docker_logs(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    container = require_argument(arguments, "container")
    tail_lines = max(1, int(arguments.get("tail_lines") or 200))
    result = await context.services.host_ops.run_backend(
        "docker",
        "logs",
        "--tail",
        str(tail_lines),
        str(container),
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "docker logs failed")
    return {"container": container, "logs": result.stdout, **result.to_dict()}


PLUGIN = PluginDefinition(
    manifest=PluginManifest(
        key="logs_viewer",
        name="Logs Viewer",
        version="1.0.0",
        description="Read local system logs, managed file logs and Docker container logs.",
        permissions=[PermissionDefinition(key="logs.read", description="Read local logs from files, system providers and Docker.")],
    ),
    tools={
        "logs_viewer.read_file_logs": MCPTool(
            manifest=MCPToolManifest(
                key="logs_viewer.read_file_logs",
                name="Read File Logs",
                description="Tail a managed log file inside the configured log roots.",
                input_schema={
                    "type": "object",
                    "required": ["path"],
                    "properties": {
                        "path": {"type": "string"},
                        "tail_lines": {"type": "integer"},
                    },
                    "additionalProperties": False,
                },
                permissions=["logs.read"],
                tags=["logs", "files", "read"],
                read_only=True,
            ),
            handler=read_file_logs,
        ),
        "logs_viewer.read_system_logs": MCPTool(
            manifest=MCPToolManifest(
                key="logs_viewer.read_system_logs",
                name="Read System Logs",
                description="Read system logs from journalctl on Linux or the Windows event log.",
                input_schema={
                    "type": "object",
                    "properties": {
                        "unit": {"type": "string"},
                        "tail_lines": {"type": "integer"},
                    },
                    "additionalProperties": False,
                },
                permissions=["logs.read"],
                tags=["logs", "system", "read"],
                read_only=True,
                required_backends=["journalctl", "powershell", "wevtutil"],
                providers=["journalctl", "windows-event-log"],
            ),
            handler=read_system_logs,
            availability=_system_log_availability,
        ),
        "logs_viewer.read_docker_logs": MCPTool(
            manifest=MCPToolManifest(
                key="logs_viewer.read_docker_logs",
                name="Read Docker Logs",
                description="Read a bounded set of logs from a Docker container.",
                input_schema={
                    "type": "object",
                    "required": ["container"],
                    "properties": {
                        "container": {"type": "string"},
                        "tail_lines": {"type": "integer"},
                    },
                    "additionalProperties": False,
                },
                permissions=["logs.read"],
                tags=["logs", "docker", "read"],
                read_only=True,
                required_backends=["docker"],
            ),
            handler=read_docker_logs,
            availability=static_availability(backend="docker"),
        ),
    },
)
