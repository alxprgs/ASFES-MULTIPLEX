from __future__ import annotations

import pytest
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from server.core.config import Settings
from server.host_ops import HostOpsService, CommandResult
from server.models import ToolExecutionContext
from server.mcp.plugins.nginx import (
    nginx_status,
    nginx_test_config,
    nginx_control,
    nginx_list_paths,
)
from server.mcp.plugins.ssl import (
    list_profiles as ssl_list_profiles,
    issue_certificate as ssl_issue_certificate,
    renew_certificate as ssl_renew_certificate,
    check_expiry as ssl_check_expiry,
)
from server.mcp.plugins.mail import send_test_email
from server.mcp.plugins.alerts import (
    list_rules as alerts_list_rules,
    upsert_rule as alerts_upsert_rule,
    delete_rule as alerts_delete_rule,
    list_events as alerts_list_events,
    evaluate_now as alerts_evaluate_now,
    send_test_notification as alerts_send_test_notification,
)


@pytest.fixture
def temp_workspace():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        ssl_dir = tmp_path / "ssl_profiles"
        ssl_dir.mkdir()

        # Write dummy SSL cert pem
        cert_file = tmp_path / "cert.pem"
        cert_file.write_text("DUMMY CERTIFICATE", encoding="utf-8")

        # Save a dummy SSL profile pointing to cert_file
        {
            "provider": "certbot",
            "domains": ["example.com"],
            "email": "admin@example.com",
            "certificate_path": str(cert_file),
        }
        (ssl_dir / "my_ssl.json").write_text(
            '{"provider": "certbot", "domains": ["example.com"], "email": "admin@example.com", "certificate_path": "'
            + str(cert_file).replace("\\", "\\\\")
            + '"}',
            encoding="utf-8",
        )

        yield {
            "tmpdir": tmp_path,
            "ssl_profiles": ssl_dir,
            "cert_file": cert_file,
        }


@pytest.fixture
def host_ops(temp_workspace):
    s = Settings()
    s.host_ops.ssl_profiles_directory = temp_workspace["ssl_profiles"]
    s.host_ops.managed_file_roots = [temp_workspace["tmpdir"]]
    return HostOpsService(s)


@pytest.mark.asyncio
async def test_nginx_plugin(host_ops) -> None:
    services = MagicMock()
    services.host_ops = host_ops
    context = ToolExecutionContext(user=MagicMock(), services=services, request_meta={})

    # 1. nginx_list_paths
    paths_res = await nginx_list_paths(context, {})
    assert len(paths_res["paths"]) > 0

    # Mock psutil connection and process running
    with patch("server.mcp.plugins.nginx._psutil") as mock_psutil:
        proc = MagicMock()
        proc.info = {"pid": 200, "name": "nginx"}
        mock_psutil.process_iter.return_value = [proc]

        mock_run = AsyncMock(
            return_value=CommandResult(
                command=[], returncode=0, stdout="Syntax OK", stderr=""
            )
        )
        with patch.object(host_ops, "run_backend", mock_run):
            # 2. nginx_status
            status_res = await nginx_status(context, {})
            assert status_res["running"] is True
            assert status_res["config_ok"] is True

            # 3. nginx_test_config
            test_res = await nginx_test_config(context, {})
            assert test_res["valid"] is True

            # 4. nginx_control reload
            control_res = await nginx_control(context, {"action": "reload"})
            assert control_res["action"] == "reload"
            assert control_res["changed"] is True


@pytest.mark.asyncio
async def test_ssl_plugin(host_ops, temp_workspace) -> None:
    services = MagicMock()
    services.host_ops = host_ops
    context = ToolExecutionContext(user=MagicMock(), services=services, request_meta={})

    # 1. list_profiles
    list_res = await ssl_list_profiles(context, {})
    assert "my_ssl" in list_res["profiles"][0]["name"]

    # 2. issue_certificate
    mock_run = AsyncMock(
        return_value=CommandResult(
            command=[], returncode=0, stdout="Cert issued", stderr=""
        )
    )
    with patch.object(host_ops, "run", mock_run):
        with patch.object(host_ops, "executable_path", return_value="/usr/bin/certbot"):
            issue_res = await ssl_issue_certificate(context, {"profile": "my_ssl"})
            assert issue_res["provider"] == "certbot"

    # 3. renew_certificate
    mock_run = AsyncMock(
        return_value=CommandResult(
            command=[], returncode=0, stdout="Cert renewed", stderr=""
        )
    )
    with patch.object(host_ops, "run", mock_run):
        with patch.object(host_ops, "executable_path", return_value="/usr/bin/certbot"):
            renew_res = await ssl_renew_certificate(context, {"profile": "my_ssl"})
            assert renew_res["provider"] == "certbot"

    # 4. check_expiry with mock ssl decoder
    mock_decoder = MagicMock(return_value={"notAfter": "Jun 28 12:00:00 2035 UTC"})
    with patch("ssl._ssl._test_decode_cert", mock_decoder):
        expiry_res = await ssl_check_expiry(context, {"profile": "my_ssl"})
        assert expiry_res["profile"] == "my_ssl"
        assert expiry_res["days_remaining"] > 0


@pytest.mark.asyncio
async def test_mail_plugin(host_ops) -> None:
    services = MagicMock()
    services.host_ops = host_ops
    services.mailer.send_email = AsyncMock(return_value=True)
    context = ToolExecutionContext(user=MagicMock(), services=services, request_meta={})

    # send_test_email
    res = await send_test_email(
        context, {"recipient": "user@example.com", "subject": "test", "body": "msg"}
    )
    assert res["sent"] is True
    services.mailer.send_email.assert_called_once_with(
        "user@example.com", "test", "msg"
    )


@pytest.mark.asyncio
async def test_alerts_plugin(host_ops) -> None:
    services = MagicMock()
    services.host_ops = host_ops
    services.alerts.list_rules = AsyncMock(return_value=[{"name": "cpu_usage_alert"}])
    services.alerts.upsert_rule = AsyncMock(return_value={"rule_id": "rule1"})
    services.alerts.delete_rule = AsyncMock(return_value={"deleted": True})
    services.alerts.list_events = AsyncMock(return_value=[{"event": "triggered"}])
    services.alerts.evaluate_rules_once = AsyncMock(return_value={"evaluated": True})
    services.alerts.send_test_notification = AsyncMock(return_value={"sent": True})
    context = ToolExecutionContext(user=MagicMock(), services=services, request_meta={})

    # 1. list_rules
    list_res = await alerts_list_rules(context, {})
    assert list_res["count"] == 1

    # 2. upsert_rule
    upsert_res = await alerts_upsert_rule(
        context, {"name": "rule1", "source": "system", "condition": "cpu > 80"}
    )
    assert upsert_res["rule_id"] == "rule1"

    # 3. delete_rule
    delete_res = await alerts_delete_rule(context, {"rule_id": "rule1"})
    assert delete_res["deleted"] is True

    # 4. list_events
    events_res = await alerts_list_events(context, {"limit": 10})
    assert events_res["count"] == 1

    # 5. evaluate_now
    eval_res = await alerts_evaluate_now(context, {})
    assert eval_res["evaluated"] is True

    # 6. send_test_notification
    notify_res = await alerts_send_test_notification(
        context, {"recipients": ["admin@example.com"]}
    )
    assert notify_res["sent"] is True
