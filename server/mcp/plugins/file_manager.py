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
        name="Файлы",
        version="1.0.0",
        description="Читает и редактирует файлы внутри настроенных управляемых корней.",
        permissions=[
            PermissionDefinition(key="files.read", description="Читать управляемые файлы и директории."),
            PermissionDefinition(key="files.write", description="Записывать, перемещать и удалять управляемые файлы и директории."),
        ],
    ),
    tools={
        "file_manager.list_directory": MCPTool(
            manifest=MCPToolManifest(
                key="file_manager.list_directory",
                name="Список директории",
                description="Показывает содержимое директории внутри настроенных управляемых корней.",
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
                name="Читать файл",
                description="Читает ограниченный фрагмент управляемого файла с настройками смещения и лимита байт.",
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
                name="Записать файл",
                description="Заменяет содержимое управляемого файла атомарной записью с опциональной резервной копией.",
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
                name="Дополнить файл",
                description="Добавляет содержимое в управляемый файл и создаёт резервную копию, если файл уже существует.",
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
                name="Переместить путь",
                description="Перемещает или переименовывает управляемый файл или директорию.",
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
                name="Удалить путь",
                description="Удаляет управляемый файл или, при recursive=true, управляемую директорию.",
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
                name="Создать директорию",
                description="Создаёт дерево управляемых директорий, если оно ещё не существует.",
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
