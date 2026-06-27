from __future__ import annotations

import os
import pytest
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from server.core.config import Settings
from server.host_ops import HostOpsService, CommandResult
from server.models import ToolExecutionContext
from server.mcp.plugins.system_stats import get_snapshot
from server.mcp.plugins.process_manager import list_processes, inspect_process, start_process, stop_process, restart_process
from server.mcp.plugins.logs_viewer import read_file_logs, read_system_logs, read_docker_logs


@pytest.fixture
def temp_workspace():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        
        # Write some log file
        log_file = log_dir / "app.log"
        log_file.write_text("Line 1\nLine 2\nLine 3\n", encoding="utf-8")
        
        yield {
            "tmpdir": tmp_path,
            "log_dir": log_dir,
            "log_file": log_file,
        }


@pytest.fixture
def host_ops(temp_workspace):
    s = Settings()
    s.host_ops.managed_log_roots = [temp_workspace["log_dir"]]
    s.host_ops.process_allowed_executables = ["python", "ping"]
    return HostOpsService(s)


@pytest.mark.asyncio
async def test_system_stats_get_snapshot(host_ops) -> None:
    services = MagicMock()
    services.host_ops = host_ops
    context = ToolExecutionContext(user=MagicMock(), services=services, request_meta={})

    # Mock psutil attributes
    with patch("server.mcp.plugins.system_stats._psutil") as mock_psutil:
        mock_psutil.cpu_percent.return_value = 10.5
        mock_psutil.virtual_memory.return_value._asdict.return_value = {"total": 1000}
        mock_psutil.swap_memory.return_value._asdict.return_value = {"total": 500}
        
        part = MagicMock()
        part.device = "/dev/sda1"
        part.mountpoint = "/"
        part.fstype = "ext4"
        mock_psutil.disk_partitions.return_value = [part]
        mock_psutil.disk_usage.return_value._asdict.return_value = {"free": 200}
        
        mock_psutil.net_io_counters.return_value.items.return_value = []
        mock_psutil.net_if_addrs.return_value.items.return_value = []

        res = await get_snapshot(context, {})
        assert res["cpu_percent"] == 10.5
        assert res["memory"]["total"] == 1000


@pytest.mark.asyncio
async def test_process_manager_tools(host_ops) -> None:
    services = MagicMock()
    services.host_ops = host_ops
    services.settings = host_ops.settings
    context = ToolExecutionContext(user=MagicMock(), services=services, request_meta={})

    # 1. list_processes
    with patch("server.mcp.plugins.process_manager._psutil") as mock_psutil:
        proc = MagicMock()
        proc.info = {"pid": 999, "name": "python"}
        mock_psutil.process_iter.return_value = [proc]
        
        res = await list_processes(context, {"name": "py", "limit": 5})
        assert len(res["processes"]) == 1
        assert res["processes"][0]["pid"] == 999

    # 2. inspect_process
    with patch("server.mcp.plugins.process_manager._psutil") as mock_psutil:
        proc_mock = MagicMock()
        proc_mock.pid = 999
        proc_mock.name.return_value = "python"
        proc_mock.status.return_value = "running"
        proc_mock.username.return_value = "user"
        proc_mock.cmdline.return_value = ["python", "-m", "http.server"]
        proc_mock.cwd.return_value = "/app"
        proc_mock.cpu_percent.return_value = 0.5
        proc_mock.memory_info.return_value._asdict.return_value = {"rss": 1000}
        proc_mock.memory_percent.return_value = 0.1
        mock_psutil.Process.return_value = proc_mock

        res = await inspect_process(context, {"pid": 999})
        assert res["name"] == "python"
        assert res["cmdline"] == ["python", "-m", "http.server"]

    # 3. start_process (allowed executable)
    with patch("asyncio.create_subprocess_exec") as mock_exec:
        proc_stub = MagicMock()
        proc_stub.pid = 1234
        mock_exec.return_value = proc_stub
        
        res = await start_process(context, {"command": ["python", "-c", "print(1)"]})
        assert res["pid"] == 1234
        mock_exec.assert_called_once_with("python", "-c", "print(1)", cwd=None)

    # 4. start_process (disallowed executable) -> raises error
    with pytest.raises(RuntimeError) as exc:
        await start_process(context, {"command": ["rm", "-rf", "/"]})
    assert "Process start is not allowed" in str(exc.value)


@pytest.mark.asyncio
async def test_logs_viewer_tools(host_ops, temp_workspace) -> None:
    services = MagicMock()
    services.host_ops = host_ops
    context = ToolExecutionContext(user=MagicMock(), services=services, request_meta={})

    # 1. read_file_logs
    res1 = await read_file_logs(context, {"path": "app.log", "tail_lines": 2})
    assert "Line 2\nLine 3" in res1["content"]

    # 2. read_system_logs (Linux / journalctl)
    with patch.object(host_ops, "platform_name", "linux"):
        mock_run = AsyncMock(return_value=CommandResult(command=["journalctl"], returncode=0, stdout="Jun 28 System logs here", stderr=""))
        with patch.object(host_ops, "run", mock_run):
            res2 = await read_system_logs(context, {"tail_lines": 5, "unit": "nginx"})
            assert "System logs here" in res2["logs"]
            mock_run.assert_called_with(["journalctl", "-n", "5", "--no-pager", "-u", "nginx"], check=False)

    # 3. read_system_logs (Windows / powershell)
    with patch.object(host_ops, "platform_name", "windows"):
        def mock_exists(cmd):
            return cmd == "powershell"
        with patch.object(host_ops, "command_exists", mock_exists):
            mock_run_win = AsyncMock(return_value=CommandResult(command=["powershell"], returncode=0, stdout='[{"TimeCreated":"today","Message":"log msg"}]', stderr=""))
            with patch.object(host_ops, "run_backend", mock_run_win):
                res3 = await read_system_logs(context, {"tail_lines": 10})
                assert res3["source"] == "windows-event-log"
                assert len(res3["entries"]) == 1

    # 4. read_docker_logs
    mock_run_docker = AsyncMock(return_value=CommandResult(command=["docker"], returncode=0, stdout="Docker container output", stderr=""))
    with patch.object(host_ops, "run_backend", mock_run_docker):
        res4 = await read_docker_logs(context, {"container": "my_web_app", "tail_lines": 50})
        assert "Docker container output" in res4["logs"]
        mock_run_docker.assert_called_with("docker", "logs", "--tail", "50", "my_web_app", check=False)
