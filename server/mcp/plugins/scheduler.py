from __future__ import annotations

import csv
import io
from typing import Any

from server.mcp.plugins._common import require_argument
from server.models import MCPTool, MCPToolManifest, PermissionDefinition, PluginDefinition, PluginManifest, RuntimeAvailability, ToolExecutionContext


TASK_PREFIX = "multiplex_"


async def _scheduler_availability(services) -> RuntimeAvailability:
    if services.host_ops.is_linux:
        return services.host_ops.availability_for_command("crontab", providers=["crontab"])
    return services.host_ops.availability_for_command("schtasks", providers=["schtasks"])


def _windows_task_name(name: str) -> str:
    return f"{TASK_PREFIX}{name}"


def _linux_schedule(arguments: dict[str, Any]) -> str:
    schedule = str(require_argument(arguments, "schedule")).lower()
    time_value = str(arguments.get("time") or "00:00")
    hour, minute = time_value.split(":")
    interval = int(arguments.get("interval") or 1)
    if schedule == "hourly":
        return f"{minute} */{max(1, interval)} * * *"
    if schedule == "daily":
        return f"{minute} {hour} * * *"
    if schedule == "weekly":
        days = arguments.get("days") or ["MON"]
        mapping = {"MON": "1", "TUE": "2", "WED": "3", "THU": "4", "FRI": "5", "SAT": "6", "SUN": "0"}
        resolved = ",".join(mapping[str(day).upper()] for day in days)
        return f"{minute} {hour} * * {resolved}"
    raise RuntimeError("Linux scheduler supports schedule values: hourly, daily, weekly")


def _windows_schedule_args(arguments: dict[str, Any]) -> list[str]:
    schedule = str(require_argument(arguments, "schedule")).upper()
    if schedule not in {"HOURLY", "DAILY", "WEEKLY"}:
        raise RuntimeError("Windows scheduler supports schedule values: hourly, daily, weekly")
    task_args = ["/SC", "MINUTE" if schedule == "HOURLY" else schedule]
    if schedule == "HOURLY":
        task_args.extend(["/MO", str(max(1, int(arguments.get("interval") or 60)))])
    else:
        task_args.extend(["/ST", str(arguments.get("time") or "00:00")])
    if schedule == "WEEKLY":
        days = arguments.get("days") or ["MON"]
        task_args.extend(["/D", ",".join(str(day).upper() for day in days)])
    return task_args


async def list_tasks(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    if context.services.host_ops.is_linux:
        result = await context.services.host_ops.run_backend("crontab", "-l", check=False)
        if result.returncode != 0 and "no crontab" not in result.stderr.lower():
            raise RuntimeError(result.stderr.strip() or "crontab -l failed")
        items = []
        for line in result.stdout.splitlines():
            if "# multiplex:" not in line:
                continue
            schedule, remainder = line.split(" ", 5)[:5], line.split(" ", 5)[5]
            command, marker = remainder.rsplit("# multiplex:", 1)
            items.append({"name": marker.strip(), "schedule": " ".join(schedule), "command": command.strip(), "platform": "linux"})
        return {"tasks": items, "count": len(items)}

    result = await context.services.host_ops.run_backend("schtasks", "/Query", "/FO", "CSV", "/V", check=False)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "schtasks query failed")
    reader = csv.DictReader(io.StringIO(result.stdout))
    items = []
    for row in reader:
        task_name = row.get("TaskName") or row.get("Task Name") or ""
        if TASK_PREFIX not in task_name:
            continue
        items.append(
            {
                "name": task_name.split(TASK_PREFIX, 1)[-1],
                "task_name": task_name,
                "status": row.get("Status"),
                "schedule": row.get("Schedule Type"),
                "command": row.get("Task To Run"),
                "platform": "windows",
            }
        )
    return {"tasks": items, "count": len(items)}


async def upsert_task(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    name = str(require_argument(arguments, "name")).strip()
    command = str(require_argument(arguments, "command")).strip()
    if context.services.host_ops.is_linux:
        cron = _linux_schedule(arguments)
        current = await context.services.host_ops.run_backend("crontab", "-l", check=False)
        existing_lines = [] if current.returncode != 0 else current.stdout.splitlines()
        filtered = [line for line in existing_lines if f"# multiplex:{name}" not in line]
        filtered.append(f"{cron} {command} # multiplex:{name}")
        payload = "\n".join(filtered).strip() + "\n"
        result = await context.services.host_ops.run(["crontab", "-"], input_text=payload, check=False)
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "crontab update failed")
        return {"name": name, "platform": "linux", "updated": True, "schedule": cron}

    task_name = _windows_task_name(name)
    command_args = [
        "schtasks",
        "/Create",
        "/F",
        "/TN",
        task_name,
        "/TR",
        command,
        *_windows_schedule_args(arguments),
    ]
    result = await context.services.host_ops.run(command_args, check=False)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "schtasks create failed")
    return {"name": name, "task_name": task_name, "platform": "windows", "updated": True}


async def delete_task(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    name = str(require_argument(arguments, "name")).strip()
    if context.services.host_ops.is_linux:
        current = await context.services.host_ops.run_backend("crontab", "-l", check=False)
        existing_lines = [] if current.returncode != 0 else current.stdout.splitlines()
        filtered = [line for line in existing_lines if f"# multiplex:{name}" not in line]
        payload = ("\n".join(filtered).strip() + "\n") if filtered else "\n"
        result = await context.services.host_ops.run(["crontab", "-"], input_text=payload, check=False)
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "crontab delete failed")
        return {"name": name, "platform": "linux", "deleted": True}

    result = await context.services.host_ops.run_backend("schtasks", "/Delete", "/F", "/TN", _windows_task_name(name), check=False)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "schtasks delete failed")
    return {"name": name, "platform": "windows", "deleted": True}


async def run_task(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    name = str(require_argument(arguments, "name")).strip()
    if context.services.host_ops.is_linux:
        current = await context.services.host_ops.run_backend("crontab", "-l", check=False)
        for line in current.stdout.splitlines():
            if f"# multiplex:{name}" not in line:
                continue
            command = line.rsplit("# multiplex:", 1)[0].split(" ", 5)[-1].strip()
            result = await context.services.host_ops.run(["/bin/sh", "-lc", command], check=False)
            if result.returncode != 0:
                raise RuntimeError(result.stderr.strip() or "scheduled command failed")
            return {"name": name, "platform": "linux", "triggered": True, **result.to_dict()}
        raise RuntimeError("Managed task not found")

    result = await context.services.host_ops.run_backend("schtasks", "/Run", "/TN", _windows_task_name(name), check=False)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "schtasks run failed")
    return {"name": name, "platform": "windows", "triggered": True, **result.to_dict()}


PLUGIN = PluginDefinition(
    manifest=PluginManifest(
        key="scheduler",
        name="Task Scheduler",
        version="1.0.0",
        description="Manage scheduled tasks through crontab on Linux and schtasks on Windows.",
        permissions=[
            PermissionDefinition(key="scheduler.read", description="Read managed scheduled tasks."),
            PermissionDefinition(key="scheduler.write", description="Create, delete and trigger managed scheduled tasks."),
        ],
        required_backends=["crontab", "schtasks"],
        providers=["crontab", "schtasks"],
    ),
    tools={
        "scheduler.list_tasks": MCPTool(
            manifest=MCPToolManifest(
                key="scheduler.list_tasks",
                name="List Scheduled Tasks",
                description="List tasks managed by Multiplex for the current platform.",
                input_schema={"type": "object", "properties": {}, "additionalProperties": False},
                permissions=["scheduler.read"],
                tags=["scheduler", "read"],
                read_only=True,
                required_backends=["crontab", "schtasks"],
                providers=["crontab", "schtasks"],
            ),
            handler=list_tasks,
            availability=_scheduler_availability,
        ),
        "scheduler.upsert_task": MCPTool(
            manifest=MCPToolManifest(
                key="scheduler.upsert_task",
                name="Upsert Scheduled Task",
                description="Create or update a managed scheduled task using a cross-platform schedule contract.",
                input_schema={
                    "type": "object",
                    "required": ["name", "command", "schedule"],
                    "properties": {
                        "name": {"type": "string"},
                        "command": {"type": "string"},
                        "schedule": {"type": "string", "enum": ["hourly", "daily", "weekly", "HOURLY", "DAILY", "WEEKLY"]},
                        "time": {"type": "string"},
                        "interval": {"type": "integer"},
                        "days": {"type": "array", "items": {"type": "string"}},
                    },
                    "additionalProperties": False,
                },
                permissions=["scheduler.write"],
                tags=["scheduler", "write"],
                read_only=False,
                required_backends=["crontab", "schtasks"],
                providers=["crontab", "schtasks"],
            ),
            handler=upsert_task,
            availability=_scheduler_availability,
        ),
        "scheduler.delete_task": MCPTool(
            manifest=MCPToolManifest(
                key="scheduler.delete_task",
                name="Delete Scheduled Task",
                description="Delete a managed scheduled task by name.",
                input_schema={
                    "type": "object",
                    "required": ["name"],
                    "properties": {"name": {"type": "string"}},
                    "additionalProperties": False,
                },
                permissions=["scheduler.write"],
                tags=["scheduler", "write"],
                read_only=False,
                required_backends=["crontab", "schtasks"],
                providers=["crontab", "schtasks"],
            ),
            handler=delete_task,
            availability=_scheduler_availability,
        ),
        "scheduler.run_task": MCPTool(
            manifest=MCPToolManifest(
                key="scheduler.run_task",
                name="Run Scheduled Task",
                description="Run a managed scheduled task immediately.",
                input_schema={
                    "type": "object",
                    "required": ["name"],
                    "properties": {"name": {"type": "string"}},
                    "additionalProperties": False,
                },
                permissions=["scheduler.write"],
                tags=["scheduler", "write"],
                read_only=False,
                required_backends=["crontab", "schtasks"],
                providers=["crontab", "schtasks"],
            ),
            handler=run_task,
            availability=_scheduler_availability,
        ),
    },
)
