from __future__ import annotations

import json
import pytest
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from server.core.config import Settings
from server.host_ops import HostOpsService, CommandResult
from server.models import ToolExecutionContext
from server.mcp.plugins.docker import (
    list_containers,
    restart_container,
    inspect_container,
    _redact_env_value,
    _looks_sensitive,
)
from server.mcp.plugins.docker_compose import (
    compose_ps,
    compose_up,
)


@pytest.fixture
def temp_workspace():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        proj_dir = tmp_path / "my_project"
        proj_dir.mkdir()
        compose_yml = proj_dir / "docker-compose.yml"
        compose_yml.write_text("version: '3'\nservices:\n  web:\n    image: nginx", encoding="utf-8")
        yield {
            "tmpdir": tmp_path,
            "project_dir": proj_dir,
            "compose_yml": compose_yml,
        }


@pytest.fixture
def host_ops(temp_workspace):
    s = Settings()
    s.host_ops.managed_file_roots = [temp_workspace["tmpdir"]]
    return HostOpsService(s)


@pytest.mark.asyncio
async def test_docker_plugin_handlers(host_ops) -> None:
    services = MagicMock()
    services.host_ops = host_ops
    context = ToolExecutionContext(user=MagicMock(), services=services, request_meta={})
    context.user.username = "alice"

    # Mock shutil.which to bypass check
    with patch("shutil.which", return_value="/usr/bin/docker"):
        # 1. list_containers
        mock_run = AsyncMock(return_value=(0, '{"ID":"123","Names":"web"}\n', ""))
        with patch("server.mcp.plugins.docker._run_docker_command", mock_run):
            res = await list_containers(context, {"all": True})
            assert len(res["containers"]) == 1
            assert res["containers"][0]["ID"] == "123"
            mock_run.assert_called_with("ps", "-a", "--format", "{{json .}}")

        # 2. restart_container
        mock_run = AsyncMock(return_value=(0, "123\n", ""))
        with patch("server.mcp.plugins.docker._run_docker_command", mock_run):
            res = await restart_container(context, {"container": "123"})
            assert res["restarted"] == ["123"]
            assert res["requested_by"] == "alice"

        # 3. inspect_container with redaction
        inspect_stdout = json.dumps([{
            "Id": "123",
            "Config": {
                "Env": ["DB_PASSWORD=secret123", "PORT=80"],
                "Labels": {"api_key": "somekey", "version": "1.0"}
            }
        }])
        mock_run = AsyncMock(return_value=(0, inspect_stdout, ""))
        with patch("server.mcp.plugins.docker._run_docker_command", mock_run):
            res = await inspect_container(context, {"container": "123"})
            env = res["inspect"][0]["Config"]["Env"]
            labels = res["inspect"][0]["Config"]["Labels"]
            assert "DB_PASSWORD=[REDACTED]" in env
            assert "PORT=80" in env
            assert labels["api_key"] == "[REDACTED]"
            assert labels["version"] == "1.0"


def test_docker_redaction_helpers() -> None:
    assert _looks_sensitive("DATABASE_PASSWORD") is True
    assert _looks_sensitive("DB_SECRET") is True
    assert _looks_sensitive("API_TOKEN") is True
    assert _looks_sensitive("PORT") is False

    assert _redact_env_value("MYSQL_PASSWORD=root123") == "MYSQL_PASSWORD=[REDACTED]"
    assert _redact_env_value("APP_ENV=production") == "APP_ENV=production"


@pytest.mark.asyncio
async def test_docker_compose_plugin_handlers(host_ops, temp_workspace) -> None:
    services = MagicMock()
    services.host_ops = host_ops
    context = ToolExecutionContext(user=MagicMock(), services=services, request_meta={})

    # Mock availability to docker compose
    availability_mock = MagicMock()
    availability_mock.available = True
    availability_mock.providers = ["docker compose"]
    
    with patch("server.mcp.plugins.docker_compose._compose_availability", AsyncMock(return_value=availability_mock)):
        with patch.object(host_ops, "executable_path", return_value="/usr/bin/docker"):
            # 1. compose_ps
            mock_run = AsyncMock(return_value=CommandResult(command=[], returncode=0, stdout='[{"Service":"web","State":"running"}]', stderr=""))
            with patch.object(host_ops, "run", mock_run):
                res = await compose_ps(context, {
                    "project_dir": str(temp_workspace["project_dir"]),
                    "files": ["my_project/docker-compose.yml"],
                })
                assert len(res["services"]) == 1
                assert res["services"][0]["Service"] == "web"
                # Check that -f resolved compose path is in command
                call_args = mock_run.call_args[0][0]
                assert "docker" in call_args[0]
                assert "compose" in call_args[1]
                assert "-f" in call_args

            # 2. compose_up
            mock_run = AsyncMock(return_value=CommandResult(command=[], returncode=0, stdout="Created", stderr=""))
            with patch.object(host_ops, "run", mock_run):
                res = await compose_up(context, {
                    "project_dir": str(temp_workspace["project_dir"]),
                    "detach": True,
                })
                assert res["changed"] is True
