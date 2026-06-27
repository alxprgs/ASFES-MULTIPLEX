from __future__ import annotations

import json
import pytest
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from server.core.config import Settings
from server.host_ops import HostOpsService, CommandResult
from server.models import ToolExecutionContext
from server.mcp.plugins.firewall import list_rules, set_enabled, upsert_rule, delete_rule
from server.mcp.plugins.vpn import (
    list_profiles as vpn_list_profiles,
    import_profile as vpn_import_profile,
    remove_profile as vpn_remove_profile,
    vpn_status,
    vpn_control,
)
from server.mcp.plugins.ports_scanner import list_listening_ports, probe_tcp, probe_http


@pytest.fixture
def temp_workspace():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        vpn_dir = tmp_path / "vpn_profiles"
        vpn_dir.mkdir()
        
        # Create a source VPN config to import
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        src_conf = src_dir / "my_vpn.ovpn"
        src_conf.write_text("client dev tun", encoding="utf-8")
        
        yield {
            "tmpdir": tmp_path,
            "vpn_profiles": vpn_dir,
            "src_conf": src_conf,
        }


@pytest.fixture
def host_ops(temp_workspace):
    s = Settings()
    s.host_ops.vpn_profiles_directory = temp_workspace["vpn_profiles"]
    s.host_ops.managed_file_roots = [temp_workspace["tmpdir"]]
    s.host_ops.port_probe_allowed_hosts = ["127.0.0.1", "localhost", "github.com"]
    return HostOpsService(s)


@pytest.mark.asyncio
async def test_firewall_plugin_linux(host_ops) -> None:
    services = MagicMock()
    services.host_ops = host_ops
    context = ToolExecutionContext(user=MagicMock(), services=services, request_meta={})

    # Mock Linux
    with patch.object(host_ops, "platform_name", "linux"):
        # 1. list_rules
        mock_run = AsyncMock(return_value=CommandResult(command=["ufw"], returncode=0, stdout="Status: active\n1 ALLOW IN 80/tcp", stderr=""))
        with patch.object(host_ops, "run_backend", mock_run):
            res = await list_rules(context, {})
            assert "Status: active" in res["rules_text"]
            mock_run.assert_called_with("ufw", "status", "numbered", check=False)

        # 2. set_enabled
        mock_run = AsyncMock(return_value=CommandResult(command=["ufw"], returncode=0, stdout="Firewall stop/waiting", stderr=""))
        with patch.object(host_ops, "run_backend", mock_run):
            res = await set_enabled(context, {"enabled": False})
            assert res["enabled"] is False
            mock_run.assert_called_with("ufw", "--force", "disable", check=False)

        # 3. upsert_rule
        mock_run = AsyncMock(return_value=CommandResult(command=["ufw"], returncode=0, stdout="Rule added", stderr=""))
        with patch.object(host_ops, "run_backend", mock_run):
            res = await upsert_rule(context, {"name": "http", "port": 80, "protocol": "tcp", "action": "allow", "direction": "in"})
            assert res["port"] == "80"
            mock_run.assert_called_with("ufw", "allow", "in", "80/tcp", check=False)

        # 4. delete_rule
        mock_run = AsyncMock(return_value=CommandResult(command=["ufw"], returncode=0, stdout="Rule deleted", stderr=""))
        with patch.object(host_ops, "run_backend", mock_run):
            res = await delete_rule(context, {"port": 80, "protocol": "tcp", "action": "allow", "direction": "in"})
            assert res["deleted"] is True
            mock_run.assert_called_with("ufw", "delete", "allow", "in", "80/tcp", check=False)


@pytest.mark.asyncio
async def test_firewall_plugin_windows(host_ops) -> None:
    services = MagicMock()
    services.host_ops = host_ops
    context = ToolExecutionContext(user=MagicMock(), services=services, request_meta={})

    # Mock Windows
    with patch.object(host_ops, "platform_name", "windows"):
        # 1. list_rules
        mock_run = AsyncMock(return_value=CommandResult(command=["netsh"], returncode=0, stdout="Rule Name: Allow HTTP", stderr=""))
        with patch.object(host_ops, "run_backend", mock_run):
            res = await list_rules(context, {})
            assert "Allow HTTP" in res["rules_text"]
            mock_run.assert_called_with("netsh", "advfirewall", "firewall", "show", "rule", "name=all", check=False)

        # 2. upsert_rule
        mock_run = AsyncMock(return_value=CommandResult(command=["netsh"], returncode=0, stdout="Ok.", stderr=""))
        with patch.object(host_ops, "run_backend", mock_run):
            res = await upsert_rule(context, {"name": "http", "port": 80, "protocol": "tcp", "action": "allow", "direction": "in"})
            mock_run.assert_called_with(
                "netsh", "advfirewall", "firewall", "add", "rule", "name=http", "dir=in", "action=allow", "protocol=TCP", "localport=80", check=False
            )


@pytest.mark.asyncio
async def test_vpn_plugin_lifecycle(host_ops, temp_workspace) -> None:
    services = MagicMock()
    services.host_ops = host_ops
    context = ToolExecutionContext(user=MagicMock(), services=services, request_meta={})

    # 1. Import VPN profile
    imp_res = await vpn_import_profile(context, {
        "name": "office_vpn",
        "vpn_type": "openvpn",
        "source_path": str(temp_workspace["src_conf"]),
        "service_name": "office_vpn",
    })
    assert imp_res["imported"] is True
    assert Path(imp_res["profile_path"]).exists()

    # 2. List profiles
    list_res = await vpn_list_profiles(context, {})
    assert len(list_res["profiles"]) == 1
    assert list_res["profiles"][0]["name"] == "office_vpn"

    # 3. VPN Status (mocking systemctl query on Linux)
    with patch.object(host_ops, "platform_name", "linux"):
        # Mock command_exists to True for systemctl
        def mock_exists(cmd):
            return cmd in ("systemctl", "wg-quick")
        with patch.object(host_ops, "command_exists", mock_exists):
            # Status running
            mock_run = AsyncMock(return_value=CommandResult(command=["systemctl"], returncode=0, stdout="active (running)", stderr=""))
            with patch.object(host_ops, "run_backend", mock_run):
                status_res = await vpn_status(context, {"name": "office_vpn"})
                assert status_res["running"] is True

            # Control start
            mock_run_control = AsyncMock(return_value=CommandResult(command=["systemctl"], returncode=0, stdout="", stderr=""))
            with patch.object(host_ops, "run_backend", mock_run_control):
                control_res = await vpn_control(context, {"name": "office_vpn", "action": "start"})
                assert control_res["action"] == "start"

    # 4. Remove VPN profile
    rem_res = await vpn_remove_profile(context, {"name": "office_vpn"})
    assert rem_res["removed"] is True
    assert not Path(imp_res["profile_path"]).exists()


@pytest.mark.asyncio
async def test_ports_scanner_listening_ports(host_ops) -> None:
    services = MagicMock()
    services.host_ops = host_ops
    context = ToolExecutionContext(user=MagicMock(), services=services, request_meta={})

    # Mock psutil connection objects
    mock_conn = MagicMock()
    mock_conn.status = "LISTEN"
    mock_conn.family = 2 # IPv4
    mock_conn.type = 1 # SOCK_STREAM
    mock_conn.laddr.ip = "127.0.0.1"
    mock_conn.laddr.port = 8080
    mock_conn.pid = 1234

    with patch("server.mcp.plugins.ports_scanner._psutil") as mock_psutil:
        mock_psutil.net_connections.return_value = [mock_conn]
        res = await list_listening_ports(context, {})
        assert len(res["listeners"]) == 1
        assert res["listeners"][0]["port"] == 8080


@pytest.mark.asyncio
async def test_ports_scanner_probes(host_ops) -> None:
    services = MagicMock()
    services.host_ops = host_ops
    services.settings = host_ops.settings
    context = ToolExecutionContext(user=MagicMock(), services=services, request_meta={})

    # 1. probe_tcp allowed host (reachable)
    with patch("socket.create_connection") as mock_conn:
        tcp_res = await probe_tcp(context, {"host": "127.0.0.1", "port": 80})
        assert tcp_res["reachable"] is True

    # 2. probe_tcp disallowed host -> raises error
    # (dns lookup for arbitrary host, mock to return external ip)
    with patch("socket.getaddrinfo", return_value=[(None, None, None, None, ("8.8.8.8", 0))]):
        with pytest.raises(RuntimeError) as exc:
            await probe_tcp(context, {"host": "google.com", "port": 80})
        assert "Probe host is not allowed" in str(exc.value)

    # 3. probe_http reachable url
    class FakeResponse:
        def __init__(self):
            self.is_success = True
            self.status_code = 200
            self.headers = {"Content-Type": "application/json"}

    mock_request = AsyncMock(return_value=FakeResponse())
    with patch("httpx.AsyncClient.request", mock_request):
        http_res = await probe_http(context, {"url": "http://127.0.0.1/health", "method": "GET"})
        assert http_res["ok"] is True
        assert http_res["status_code"] == 200
