from __future__ import annotations

from typing import Any

from server.mcp.plugins._common import bool_argument, require_argument
from server.models import MCPTool, MCPToolManifest, PermissionDefinition, PluginDefinition, PluginManifest, ToolExecutionContext


async def list_directory(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    return context.services.host_ops.list_directory(str(arguments.get("path") or "."), roots=context.services.host_ops.managed_file_roots())


async def read_file(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    path = require_argument(arguments, "path")
    offset = max(0, int(arguments.get("offset") or 0))
    max_bytes = max(1, int(arguments.get("max_bytes") or context.services.settings.host_ops.max_output_bytes))
    return context.services.host_ops.read_text(str(path), roots=context.services.host_ops.managed_file_roots(), offset=offset, max_bytes=max_bytes)


async def write_file(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    path = require_argument(arguments, "path")
    content = str(arguments.get("content") or "")
    return context.services.host_ops.atomic_write_text(
        str(path),
        content,
        roots=context.services.host_ops.managed_file_roots(),
        backup_existing=bool_argument(arguments, "backup_existing", True),
    )


async def append_file(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    path = require_argument(arguments, "path")
    content = str(arguments.get("content") or "")
    return context.services.host_ops.atomic_write_text(
        str(path),
        content,
        roots=context.services.host_ops.managed_file_roots(),
        append=True,
    )


async def move_path(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    source = require_argument(arguments, "source")
    destination = require_argument(arguments, "destination")
    return context.services.host_ops.move_path(str(source), str(destination), roots=context.services.host_ops.managed_file_roots())


async def delete_path(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    path = require_argument(arguments, "path")
    recursive = bool_argument(arguments, "recursive", False)
    return context.services.host_ops.delete_path(str(path), roots=context.services.host_ops.managed_file_roots(), recursive=recursive)


async def make_directory(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    path = require_argument(arguments, "path")
    return context.services.host_ops.mkdir(str(path), roots=context.services.host_ops.managed_file_roots())


PLUGIN = PluginDefinition(
    manifest=PluginManifest(
        key="file_manager",
        name="File Manager",
        version="1.0.0",
        description="Read and edit files inside configured managed roots.",
        permissions=[
            PermissionDefinition(key="files.read", description="Read managed files and directories."),
            PermissionDefinition(key="files.write", description="Write, move and delete managed files and directories."),
        ],
    ),
    tools={
        "file_manager.list_directory": MCPTool(
            manifest=MCPToolManifest(
                key="file_manager.list_directory",
                name="List Directory",
                description="List directory contents within configured managed roots.",
                input_schema={
                    "type": "object",
                    "properties": {"path": {"type": "string"}},
                    "additionalProperties": False,
                },
                permissions=["files.read"],
                tags=["files", "read"],
                read_only=True,
            ),
            handler=list_directory,
        ),
        "file_manager.read_file": MCPTool(
            manifest=MCPToolManifest(
                key="file_manager.read_file",
                name="Read File",
                description="Read a bounded slice of a managed file with offset and byte limit controls.",
                input_schema={
                    "type": "object",
                    "required": ["path"],
                    "properties": {
                        "path": {"type": "string"},
                        "offset": {"type": "integer"},
                        "max_bytes": {"type": "integer"},
                    },
                    "additionalProperties": False,
                },
                permissions=["files.read"],
                tags=["files", "read"],
                read_only=True,
            ),
            handler=read_file,
        ),
        "file_manager.write_file": MCPTool(
            manifest=MCPToolManifest(
                key="file_manager.write_file",
                name="Write File",
                description="Replace the contents of a managed file using an atomic write and optional backup.",
                input_schema={
                    "type": "object",
                    "required": ["path", "content"],
                    "properties": {
                        "path": {"type": "string"},
                        "content": {"type": "string"},
                        "backup_existing": {"type": "boolean"},
                    },
                    "additionalProperties": False,
                },
                permissions=["files.write"],
                tags=["files", "write"],
                read_only=False,
                audit_redact_fields=["content"],
            ),
            handler=write_file,
        ),
        "file_manager.append_file": MCPTool(
            manifest=MCPToolManifest(
                key="file_manager.append_file",
                name="Append File",
                description="Append content to a managed file and create a backup if the file already exists.",
                input_schema={
                    "type": "object",
                    "required": ["path", "content"],
                    "properties": {
                        "path": {"type": "string"},
                        "content": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
                permissions=["files.write"],
                tags=["files", "write"],
                read_only=False,
                audit_redact_fields=["content"],
            ),
            handler=append_file,
        ),
        "file_manager.move_path": MCPTool(
            manifest=MCPToolManifest(
                key="file_manager.move_path",
                name="Move Path",
                description="Move or rename a managed file or directory.",
                input_schema={
                    "type": "object",
                    "required": ["source", "destination"],
                    "properties": {
                        "source": {"type": "string"},
                        "destination": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
                permissions=["files.write"],
                tags=["files", "write"],
                read_only=False,
            ),
            handler=move_path,
        ),
        "file_manager.delete_path": MCPTool(
            manifest=MCPToolManifest(
                key="file_manager.delete_path",
                name="Delete Path",
                description="Delete a managed file or, with recursive=true, a managed directory.",
                input_schema={
                    "type": "object",
                    "required": ["path"],
                    "properties": {
                        "path": {"type": "string"},
                        "recursive": {"type": "boolean"},
                    },
                    "additionalProperties": False,
                },
                permissions=["files.write"],
                tags=["files", "write"],
                read_only=False,
            ),
            handler=delete_path,
        ),
        "file_manager.make_directory": MCPTool(
            manifest=MCPToolManifest(
                key="file_manager.make_directory",
                name="Make Directory",
                description="Create a managed directory tree if it does not already exist.",
                input_schema={
                    "type": "object",
                    "required": ["path"],
                    "properties": {"path": {"type": "string"}},
                    "additionalProperties": False,
                },
                permissions=["files.write"],
                tags=["files", "write"],
                read_only=False,
            ),
            handler=make_directory,
        ),
    },
)
