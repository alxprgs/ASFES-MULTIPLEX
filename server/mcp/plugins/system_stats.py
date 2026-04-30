from __future__ import annotations

import os
from typing import Any

from server.host_ops import _psutil
from server.mcp.plugins._common import static_availability
from server.models import MCPTool, MCPToolManifest, PermissionDefinition, PluginDefinition, PluginManifest, ToolExecutionContext


async def get_snapshot(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    if _psutil is None:
        raise RuntimeError("psutil is required for system stats")
    snapshot = {
        "cpu_percent": _psutil.cpu_percent(interval=0.1),
        "memory": _psutil.virtual_memory()._asdict(),
        "swap": _psutil.swap_memory()._asdict(),
        "disk": [
            {
                "device": partition.device,
                "mountpoint": partition.mountpoint,
                "fstype": partition.fstype,
                "usage": _psutil.disk_usage(partition.mountpoint)._asdict(),
            }
            for partition in _psutil.disk_partitions(all=False)
        ],
        "network": {
            "io_counters": {
                name: counters._asdict() for name, counters in _psutil.net_io_counters(pernic=True).items()
            },
            "interfaces": {
                name: [addr._asdict() for addr in addrs] for name, addrs in _psutil.net_if_addrs().items()
            },
        },
    }
    if hasattr(os, "getloadavg"):
        snapshot["load_average"] = list(os.getloadavg())
    return snapshot


PLUGIN = PluginDefinition(
    manifest=PluginManifest(
        key="system_stats",
        name="Системная статистика",
        version="1.0.0",
        description="Читает метрики CPU, памяти, дисков и сети локального хоста.",
        permissions=[PermissionDefinition(key="system.stats.read", description="Читать метрики CPU, памяти, дисков и сети хоста.")],
        required_backends=["psutil"],
    ),
    tools={
        "system_stats.get_snapshot": MCPTool(
            manifest=MCPToolManifest(
                key="system_stats.get_snapshot",
                name="Снимок системы",
                description="Возвращает моментальный снимок метрик CPU, памяти, дисков и сети локального хоста.",
                input_schema={"type": "object", "properties": {}, "additionalProperties": False},
                permissions=["system.stats.read"],
                tags=["system", "stats", "read"],
                read_only=True,
                required_backends=["psutil"],
            ),
            handler=get_snapshot,
            availability=static_availability(require_psutil=True),
        )
    },
)
