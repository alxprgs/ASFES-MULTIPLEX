from __future__ import annotations

import json
import pytest
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from server.core.config import Settings
from server.host_ops import HostOpsService, CommandResult
from server.models import ToolExecutionContext
from server.mcp.plugins.database_manager import (
    list_profiles as db_list_profiles,
    connection_status as db_connection_status,
    backup_database as db_backup_database,
    restore_database as db_restore_database,
)
from server.mcp.plugins.scheduler import (
    list_tasks as sched_list_tasks,
    upsert_task as sched_upsert_task,
    delete_task as sched_delete_task,
    run_task as sched_run_task,
)


@pytest.fixture
def temp_workspace():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        db_dir = tmp_path / "db_profiles"
        db_dir.mkdir()
        backup_dir = tmp_path / "backups"
        backup_dir.mkdir()

        yield {
            "tmpdir": tmp_path,
            "db_profiles": db_dir,
            "backups": backup_dir,
        }


@pytest.fixture
def host_ops(temp_workspace):
    s = Settings()
    s.host_ops.database_profiles_directory = temp_workspace["db_profiles"]
    s.host_ops.backup_directory = temp_workspace["backups"]
    s.host_ops.managed_file_roots = [temp_workspace["tmpdir"]]
    return HostOpsService(s)


@pytest.mark.asyncio
async def test_database_manager_plugin(host_ops, temp_workspace) -> None:
    services = MagicMock()
    services.host_ops = host_ops
    context = ToolExecutionContext(user=MagicMock(), services=services, request_meta={})

    # Create dummy database profiles
    pg_profile = {
        "engine": "postgres",
        "host": "localhost",
        "port": 5432,
        "database": "my_db",
        "username": "pg_user",
        "password": "pg_password",
    }
    mysql_profile = {
        "engine": "mysql",
        "host": "127.0.0.1",
        "port": 3306,
        "database": "my_mysql",
        "username": "root",
        "password": "root_password",
    }

    # Save profiles
    (temp_workspace["db_profiles"] / "pg_profile.json").write_text(
        json.dumps(pg_profile), encoding="utf-8"
    )
    (temp_workspace["db_profiles"] / "mysql_profile.json").write_text(
        json.dumps(mysql_profile), encoding="utf-8"
    )

    # 1. list_profiles
    res_list = await db_list_profiles(context, {})
    assert len(res_list["profiles"]) == 2

    # 2. connection_status (postgres)
    mock_run = AsyncMock(
        return_value=CommandResult(command=[], returncode=0, stdout="1", stderr="")
    )
    with patch.object(host_ops, "run", mock_run):
        res = await db_connection_status(context, {"profile": "pg_profile"})
        assert res["connected"] is True
        # Check command params
        cmd = mock_run.call_args[0][0]
        assert "psql" in cmd
        assert "pg_user" in cmd

    # 3. connection_status (mysql)
    mock_run = AsyncMock(
        return_value=CommandResult(command=[], returncode=0, stdout="1", stderr="")
    )
    with patch.object(host_ops, "run", mock_run):
        res = await db_connection_status(context, {"profile": "mysql_profile"})
        assert res["connected"] is True
        cmd = mock_run.call_args[0][0]
        assert "mysql" in cmd

    # 4. backup_database (postgres)
    mock_run = AsyncMock(
        return_value=CommandResult(command=[], returncode=0, stdout="Dumped", stderr="")
    )
    with patch.object(host_ops, "run", mock_run):
        res = await db_backup_database(
            context, {"profile": "pg_profile", "dump_path": "backup.sql"}
        )
        assert res["profile"] == "pg_profile"
        assert "backup.sql" in res["dump_path"]
        cmd = mock_run.call_args[0][0]
        assert "pg_dump" in cmd

    # 5. restore_database (mysql)
    mock_run = AsyncMock(
        return_value=CommandResult(
            command=[], returncode=0, stdout="Restored", stderr=""
        )
    )
    with patch.object(host_ops, "run", mock_run):
        res = await db_restore_database(
            context, {"profile": "mysql_profile", "dump_path": "backup.sql"}
        )
        assert res["profile"] == "mysql_profile"
        cmd = mock_run.call_args[0][0]
        assert "mysql" in cmd


@pytest.mark.asyncio
async def test_scheduler_plugin_linux(host_ops) -> None:
    services = MagicMock()
    services.host_ops = host_ops
    context = ToolExecutionContext(user=MagicMock(), services=services, request_meta={})

    with patch.object(host_ops, "platform_name", "linux"):
        # 1. list_tasks
        crontab_out = "* * * * * /bin/sh -c 'run' # multiplex:backup_job\n"
        mock_run = AsyncMock(
            return_value=CommandResult(
                command=[], returncode=0, stdout=crontab_out, stderr=""
            )
        )
        with patch.object(host_ops, "run_backend", mock_run):
            res = await sched_list_tasks(context, {})
            assert len(res["tasks"]) == 1
            assert res["tasks"][0]["name"] == "backup_job"

        # 2. upsert_task
        mock_list = AsyncMock(
            return_value=CommandResult(command=[], returncode=0, stdout="", stderr="")
        )
        mock_save = AsyncMock(
            return_value=CommandResult(command=[], returncode=0, stdout="", stderr="")
        )
        with patch.object(host_ops, "run_backend", mock_list):
            with patch.object(host_ops, "run", mock_save):
                res = await sched_upsert_task(
                    context,
                    {
                        "name": "daily_job",
                        "command": "python script.py",
                        "schedule": "daily",
                        "time": "14:30",
                    },
                )
                assert res["name"] == "daily_job"
                assert res["updated"] is True
                assert res["schedule"] == "30 14 * * *"

        # 3. delete_task
        mock_list = AsyncMock(
            return_value=CommandResult(
                command=[],
                returncode=0,
                stdout="* * * * * # multiplex:daily_job",
                stderr="",
            )
        )
        mock_save = AsyncMock(
            return_value=CommandResult(command=[], returncode=0, stdout="", stderr="")
        )
        with patch.object(host_ops, "run_backend", mock_list):
            with patch.object(host_ops, "run", mock_save):
                res = await sched_delete_task(context, {"name": "daily_job"})
                assert res["deleted"] is True

        # 4. run_task
        mock_list = AsyncMock(
            return_value=CommandResult(
                command=[],
                returncode=0,
                stdout="* * * * * python script.py # multiplex:daily_job",
                stderr="",
            )
        )
        mock_run_cmd = AsyncMock(
            return_value=CommandResult(
                command=[], returncode=0, stdout="Script run", stderr=""
            )
        )
        with patch.object(host_ops, "run_backend", mock_list):
            with patch.object(host_ops, "run", mock_run_cmd):
                res = await sched_run_task(context, {"name": "daily_job"})
                assert res["triggered"] is True


@pytest.mark.asyncio
async def test_scheduler_plugin_windows(host_ops) -> None:
    services = MagicMock()
    services.host_ops = host_ops
    context = ToolExecutionContext(user=MagicMock(), services=services, request_meta={})

    with patch.object(host_ops, "platform_name", "windows"):
        # 1. list_tasks
        schtasks_csv = (
            '"TaskName","Task Name","Status","Schedule Type","Task To Run"\n'
            '"\\multiplex_backup_job","\\multiplex_backup_job","Ready","Daily","C:\\cmd.exe"\n'
        )
        mock_run = AsyncMock(
            return_value=CommandResult(
                command=[], returncode=0, stdout=schtasks_csv, stderr=""
            )
        )
        with patch.object(host_ops, "run_backend", mock_run):
            res = await sched_list_tasks(context, {})
            assert len(res["tasks"]) == 1
            assert res["tasks"][0]["name"] == "backup_job"

        # 2. upsert_task
        mock_save = AsyncMock(
            return_value=CommandResult(
                command=[], returncode=0, stdout="SUCCESS", stderr=""
            )
        )
        with patch.object(host_ops, "run", mock_save):
            res = await sched_upsert_task(
                context,
                {
                    "name": "daily_job",
                    "command": "C:\\script.exe",
                    "schedule": "daily",
                    "time": "14:30",
                },
            )
            assert res["name"] == "daily_job"
            # Check windows task registration args
            args = mock_save.call_args[0][0]
            assert "schtasks" in args
            assert "/TN" in args
            assert "multiplex_daily_job" in args

        # 3. delete_task
        mock_del = AsyncMock(
            return_value=CommandResult(
                command=[], returncode=0, stdout="SUCCESS", stderr=""
            )
        )
        with patch.object(host_ops, "run_backend", mock_del):
            res = await sched_delete_task(context, {"name": "daily_job"})
            assert res["deleted"] is True
            mock_del.assert_called_with(
                "schtasks", "/Delete", "/F", "/TN", "multiplex_daily_job", check=False
            )
