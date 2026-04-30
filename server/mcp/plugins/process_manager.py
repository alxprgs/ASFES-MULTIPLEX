from __future__ import annotations

import asyncio
from typing import Any

from server.host_ops import _psutil
from server.mcp.plugins._common import bool_argument, int_argument, managed_path, static_availability, string_list_argument
from server.models import MCPTool, MCPToolManifest, PermissionDefinition, PluginDefinition, PluginManifest, ToolExecutionContext


async def list_processes(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    if _psutil is None:
        raise RuntimeError("psutil is required for process inspection")
    name_filter = str(arguments.get("name") or "").strip().lower()
    limit = max(1, int_argument(arguments, "limit", 100))
    items = []
    for proc in _psutil.process_iter(["pid", "name", "status", "username", "cpu_percent", "memory_percent", "cmdline", "create_time"]):
        info = proc.info
        if name_filter and name_filter not in (info.get("name") or "").lower():
            continue
        items.append(info)
        if len(items) >= limit:
            break
    return {"processes": items, "count": len(items)}


async def inspect_process(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    if _psutil is None:
        raise RuntimeError("psutil is required for process inspection")
    pid = int_argument(arguments, "pid", 0)
    proc = _psutil.Process(pid)
    return {
        "pid": proc.pid,
        "name": proc.name(),
        "status": proc.status(),
        "username": proc.username(),
        "cmdline": proc.cmdline(),
        "cwd": proc.cwd() if proc.is_running() else None,
        "cpu_percent": proc.cpu_percent(interval=0.1),
        "memory_info": proc.memory_info()._asdict(),
        "memory_percent": proc.memory_percent(),
    }


async def start_process(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    command = string_list_argument(arguments, "command")
    if not command:
        raise RuntimeError("The 'command' argument must contain at least one element")
    cwd = None
    if arguments.get("cwd"):
        cwd = managed_path(context, str(arguments["cwd"]))
    process = await asyncio.create_subprocess_exec(*command, cwd=str(cwd) if cwd else None)
    return {"pid": process.pid, "command": command, "cwd": str(cwd) if cwd else None}


async def stop_process(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    if _psutil is None:
        raise RuntimeError("psutil is required for process control")
    pid = int_argument(arguments, "pid", 0)
    force = bool_argument(arguments, "force", False)
    proc = _psutil.Process(pid)
    if force:
        proc.kill()
    else:
        proc.terminate()
    proc.wait(timeout=5)
    return {"pid": pid, "force": force, "stopped": True}


async def restart_process(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    if _psutil is None:
        raise RuntimeError("psutil is required for process control")
    pid = int_argument(arguments, "pid", 0)
    proc = _psutil.Process(pid)
    cmdline = proc.cmdline()
    cwd = proc.cwd() if proc.is_running() else None
    if not cmdline:
        raise RuntimeError("The selected process does not expose a restartable command line")
    proc.terminate()
    proc.wait(timeout=5)
    restarted = await asyncio.create_subprocess_exec(*cmdline, cwd=cwd or None)
    return {"previous_pid": pid, "pid": restarted.pid, "command": cmdline, "cwd": cwd}


PLUGIN = PluginDefinition(
    manifest=PluginManifest(
        key="process_manager",
        name="Процессы",
        version="1.0.0",
        description="Просматривает и управляет локальными процессами операционной системы.",
        permissions=[
            PermissionDefinition(key="process.read", description="Читать метаданные и runtime-метрики локальных процессов."),
            PermissionDefinition(key="process.write", description="Запускать, останавливать и перезапускать локальные процессы."),
        ],
        required_backends=["psutil"],
    ),
    tools={
        "process_manager.list_processes": MCPTool(
            manifest=MCPToolManifest(
                key="process_manager.list_processes",
                name="Список процессов",
                description="Показывает локальные процессы с опциональной фильтрацией по имени и лимитом результатов.",
                input_schema={
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "limit": {"type": "integer"},
                    },
                    "additionalProperties": False,
                },
                permissions=["process.read"],
                tags=["process", "read"],
                read_only=True,
                required_backends=["psutil"],
            ),
            handler=list_processes,
            availability=static_availability(require_psutil=True),
        ),
        "process_manager.inspect_process": MCPTool(
            manifest=MCPToolManifest(
                key="process_manager.inspect_process",
                name="Инспекция процесса",
                description="Читает подробные метаданные локального процесса по PID.",
                input_schema={
                    "type": "object",
                    "required": ["pid"],
                    "properties": {"pid": {"type": "integer"}},
                    "additionalProperties": False,
                },
                permissions=["process.read"],
                tags=["process", "inspect"],
                read_only=True,
                required_backends=["psutil"],
            ),
            handler=inspect_process,
            availability=static_availability(require_psutil=True),
        ),
        "process_manager.start_process": MCPTool(
            manifest=MCPToolManifest(
                key="process_manager.start_process",
                name="Запустить процесс",
                description="Запускает локальный процесс с явным массивом команды и опциональной управляемой рабочей директорией.",
                input_schema={
                    "type": "object",
                    "required": ["command"],
                    "properties": {
                        "command": {"type": "array", "items": {"type": "string"}},
                        "cwd": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
                permissions=["process.write"],
                tags=["process", "write"],
                read_only=False,
            ),
            handler=start_process,
        ),
        "process_manager.stop_process": MCPTool(
            manifest=MCPToolManifest(
                key="process_manager.stop_process",
                name="Остановить процесс",
                description="Завершает или принудительно убивает локальный процесс по PID.",
                input_schema={
                    "type": "object",
                    "required": ["pid"],
                    "properties": {
                        "pid": {"type": "integer"},
                        "force": {"type": "boolean"},
                    },
                    "additionalProperties": False,
                },
                permissions=["process.write"],
                tags=["process", "write"],
                read_only=False,
                required_backends=["psutil"],
            ),
            handler=stop_process,
            availability=static_availability(require_psutil=True),
        ),
        "process_manager.restart_process": MCPTool(
            manifest=MCPToolManifest(
                key="process_manager.restart_process",
                name="Перезапустить процесс",
                description="Перезапускает локальный процесс по PID, используя его текущую командную строку и рабочую директорию.",
                input_schema={
                    "type": "object",
                    "required": ["pid"],
                    "properties": {"pid": {"type": "integer"}},
                    "additionalProperties": False,
                },
                permissions=["process.write"],
                tags=["process", "write"],
                read_only=False,
                required_backends=["psutil"],
            ),
            handler=restart_process,
            availability=static_availability(require_psutil=True),
        ),
    },
)
