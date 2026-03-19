from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from server.mcp.plugins._common import require_argument
from server.models import MCPTool, MCPToolManifest, PermissionDefinition, PluginDefinition, PluginManifest, RuntimeAvailability, ToolExecutionContext


async def _database_availability(services) -> RuntimeAvailability:
    return services.host_ops.availability_for_any_command(["mysql", "mysqldump", "psql", "pg_dump"], providers=["mysql", "postgres"])


def _load_profile(context: ToolExecutionContext, name: str) -> dict[str, Any]:
    return context.services.host_ops.load_json_profile("database", name)


def _resolve_dump_path(context: ToolExecutionContext, arguments: dict[str, Any], profile_name: str) -> Path:
    if arguments.get("dump_path"):
        return context.services.host_ops.resolve_managed_path(str(arguments["dump_path"]), roots=context.services.host_ops.managed_file_roots())
    backup_root = context.services.host_ops.backup_directory() / "databases"
    backup_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    return backup_root / f"{profile_name}-{stamp}.sql"


def _database_env(profile: dict[str, Any]) -> dict[str, str]:
    engine = str(profile.get("engine") or "").lower()
    password = str(profile.get("password") or "")
    if engine in {"mysql", "mariadb"} and password:
        return {"MYSQL_PWD": password}
    if engine == "postgres" and password:
        return {"PGPASSWORD": password}
    return {}


async def list_profiles(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    return {"profiles": context.services.host_ops.list_profiles("database")}


async def connection_status(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    profile_name = str(require_argument(arguments, "profile"))
    profile = _load_profile(context, profile_name)
    engine = str(profile.get("engine") or "").lower()
    host = str(profile.get("host") or "127.0.0.1")
    port = str(profile.get("port") or ("5432" if engine == "postgres" else "3306"))
    database = str(profile.get("database") or "")
    user = str(profile.get("username") or "")
    env = _database_env(profile)
    if engine == "postgres":
        command = ["psql", "-h", host, "-p", port, "-U", user, "-d", database, "-c", "SELECT 1;"]
    elif engine in {"mysql", "mariadb"}:
        command = ["mysql", "-h", host, "-P", port, "-u", user, database, "-e", "SELECT 1;"]
    else:
        raise RuntimeError("Unsupported database engine")
    result = await context.services.host_ops.run(command, env=env, check=False)
    return {"profile": profile_name, "engine": engine, "connected": result.returncode == 0, **result.to_dict()}


async def backup_database(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    profile_name = str(require_argument(arguments, "profile"))
    profile = _load_profile(context, profile_name)
    engine = str(profile.get("engine") or "").lower()
    host = str(profile.get("host") or "127.0.0.1")
    port = str(profile.get("port") or ("5432" if engine == "postgres" else "3306"))
    database = str(profile.get("database") or "")
    user = str(profile.get("username") or "")
    env = _database_env(profile)
    dump_path = _resolve_dump_path(context, arguments, profile_name)
    dump_path.parent.mkdir(parents=True, exist_ok=True)
    if engine == "postgres":
        command = ["pg_dump", "-h", host, "-p", port, "-U", user, "-d", database, "-f", str(dump_path)]
    elif engine in {"mysql", "mariadb"}:
        command = ["mysqldump", "-h", host, "-P", port, "-u", user, f"--result-file={dump_path}", database]
    else:
        raise RuntimeError("Unsupported database engine")
    result = await context.services.host_ops.run(command, env=env, check=False)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "database backup failed")
    return {"profile": profile_name, "engine": engine, "dump_path": str(dump_path), **result.to_dict()}


async def restore_database(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    profile_name = str(require_argument(arguments, "profile"))
    dump_path = context.services.host_ops.resolve_managed_path(str(require_argument(arguments, "dump_path")), roots=context.services.host_ops.managed_file_roots())
    profile = _load_profile(context, profile_name)
    engine = str(profile.get("engine") or "").lower()
    host = str(profile.get("host") or "127.0.0.1")
    port = str(profile.get("port") or ("5432" if engine == "postgres" else "3306"))
    database = str(profile.get("database") or "")
    user = str(profile.get("username") or "")
    env = _database_env(profile)
    if engine == "postgres":
        command = ["psql", "-h", host, "-p", port, "-U", user, "-d", database, "-f", str(dump_path)]
    elif engine in {"mysql", "mariadb"}:
        command = ["mysql", "-h", host, "-P", port, "-u", user, database, "-e", f"source {dump_path}"]
    else:
        raise RuntimeError("Unsupported database engine")
    result = await context.services.host_ops.run(command, env=env, check=False)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "database restore failed")
    return {"profile": profile_name, "engine": engine, "dump_path": str(dump_path), **result.to_dict()}


PLUGIN = PluginDefinition(
    manifest=PluginManifest(
        key="database_manager",
        name="Database Manager",
        version="1.0.0",
        description="Check, back up and restore databases using named server-side profiles.",
        permissions=[
            PermissionDefinition(key="database.read", description="Read database profile metadata and connection status."),
            PermissionDefinition(key="database.write", description="Back up and restore databases using named profiles."),
        ],
        providers=["mysql", "postgres"],
    ),
    tools={
        "database_manager.list_profiles": MCPTool(
            manifest=MCPToolManifest(
                key="database_manager.list_profiles",
                name="List Database Profiles",
                description="List named database profiles stored on the server.",
                input_schema={"type": "object", "properties": {}, "additionalProperties": False},
                permissions=["database.read"],
                tags=["database", "read"],
                read_only=True,
            ),
            handler=list_profiles,
        ),
        "database_manager.connection_status": MCPTool(
            manifest=MCPToolManifest(
                key="database_manager.connection_status",
                name="Database Connection Status",
                description="Check whether a named database profile is reachable and can execute a simple query.",
                input_schema={
                    "type": "object",
                    "required": ["profile"],
                    "properties": {"profile": {"type": "string"}},
                    "additionalProperties": False,
                },
                permissions=["database.read"],
                tags=["database", "read"],
                read_only=True,
                providers=["mysql", "postgres"],
            ),
            handler=connection_status,
            availability=_database_availability,
        ),
        "database_manager.backup_database": MCPTool(
            manifest=MCPToolManifest(
                key="database_manager.backup_database",
                name="Backup Database",
                description="Create a database backup using a named profile and write the dump inside managed storage.",
                input_schema={
                    "type": "object",
                    "required": ["profile"],
                    "properties": {
                        "profile": {"type": "string"},
                        "dump_path": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
                permissions=["database.write"],
                tags=["database", "write"],
                read_only=False,
                providers=["mysql", "postgres"],
            ),
            handler=backup_database,
            availability=_database_availability,
        ),
        "database_manager.restore_database": MCPTool(
            manifest=MCPToolManifest(
                key="database_manager.restore_database",
                name="Restore Database",
                description="Restore a database from a managed dump file using a named profile.",
                input_schema={
                    "type": "object",
                    "required": ["profile", "dump_path"],
                    "properties": {
                        "profile": {"type": "string"},
                        "dump_path": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
                permissions=["database.write"],
                tags=["database", "write"],
                read_only=False,
                providers=["mysql", "postgres"],
            ),
            handler=restore_database,
            availability=_database_availability,
        ),
    },
)
