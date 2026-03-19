from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from server.host_ops import CommandResult
from server.models import RuntimeAvailability, ToolExecutionContext


def require_argument(arguments: dict[str, Any], key: str) -> Any:
    value = arguments.get(key)
    if value in (None, ""):
        raise RuntimeError(f"The '{key}' argument is required")
    return value


def string_list_argument(arguments: dict[str, Any], key: str) -> list[str]:
    value = arguments.get(key) or []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    raise RuntimeError(f"The '{key}' argument must be an array")


def int_argument(arguments: dict[str, Any], key: str, default: int) -> int:
    raw = arguments.get(key, default)
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"The '{key}' argument must be an integer") from exc


def bool_argument(arguments: dict[str, Any], key: str, default: bool = False) -> bool:
    value = arguments.get(key, default)
    return bool(value)


def dict_argument(arguments: dict[str, Any], key: str) -> dict[str, Any]:
    value = arguments.get(key) or {}
    if not isinstance(value, dict):
        raise RuntimeError(f"The '{key}' argument must be an object")
    return value


def command_result_payload(result: CommandResult, **extra: Any) -> dict[str, Any]:
    payload = result.to_dict()
    payload.update(extra)
    return payload


def parse_json_lines(stdout: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for line in stdout.splitlines():
        if not line.strip():
            continue
        items.append(json.loads(line))
    return items


def managed_path(context: ToolExecutionContext, raw_path: str, *, use_logs_root: bool = False) -> Path:
    roots = context.services.host_ops.managed_log_roots() if use_logs_root else context.services.host_ops.managed_file_roots()
    return context.services.host_ops.resolve_managed_path(str(raw_path), roots=roots)


def static_availability(
    *,
    backend: str | None = None,
    any_backends: list[str] | None = None,
    require_psutil: bool = False,
) -> Any:
    async def _availability(services) -> RuntimeAvailability:
        statuses: list[RuntimeAvailability] = []
        if backend:
            statuses.append(services.host_ops.availability_for_command(backend))
        if any_backends:
            statuses.append(services.host_ops.availability_for_any_command(any_backends))
        if require_psutil:
            statuses.append(services.host_ops.availability_for_psutil())
        if not statuses:
            return RuntimeAvailability(available=True)
        available = all(item.available for item in statuses)
        reason = next((item.reason for item in statuses if not item.available and item.reason), None)
        required_backends = sorted({backend_name for item in statuses for backend_name in item.required_backends})
        providers = sorted({provider for item in statuses for provider in item.providers})
        return RuntimeAvailability(
            available=available,
            reason=reason,
            required_backends=required_backends,
            providers=providers,
        )

    return _availability
