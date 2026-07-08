from __future__ import annotations

import logging
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch, AsyncMock
from uuid import uuid4

import pytest

from server.core.config import LoggingConfig, SMTPConfig
from server.core.logging import IntegrityLogManager, Mailer


@pytest.mark.asyncio
async def test_mailer_disabled() -> None:
    smtp_cfg = SMTPConfig(enabled=False)
    mailer = Mailer(smtp_cfg)
    res = await mailer.send_email("test@example.com", "Subject", "Body")
    assert res is False


@pytest.mark.asyncio
async def test_mailer_ssl_enabled() -> None:
    smtp_cfg = SMTPConfig(
        enabled=True,
        host="smtp.example.com",
        port=465,
        use_ssl=True,
        username="user",
        password="pass123",
        from_email="from@example.com",
    )
    mailer = Mailer(smtp_cfg)

    mock_client = MagicMock()
    with patch("smtplib.SMTP_SSL", return_value=mock_client) as mock_ssl:
        res = await mailer.send_email("to@example.com", "Subject", "Body")
        assert res is True
        mock_ssl.assert_called_once_with("smtp.example.com", 465, timeout=10)
        mock_client.__enter__.assert_called_once()
        mock_client.__enter__.return_value.login.assert_called_once_with(
            "user", "pass123"
        )
        mock_client.__enter__.return_value.send_message.assert_called_once()


@pytest.mark.asyncio
async def test_mailer_starttls_enabled() -> None:
    smtp_cfg = SMTPConfig(
        enabled=True,
        host="smtp.example.com",
        port=587,
        use_ssl=False,
        starttls=True,
        from_email="from@example.com",
    )
    mailer = Mailer(smtp_cfg)

    mock_client = MagicMock()
    with patch("smtplib.SMTP", return_value=mock_client) as mock_smtp:
        res = await mailer.send_email("to@example.com", "Subject", "Body")
        assert res is True
        mock_smtp.assert_called_once_with("smtp.example.com", 587, timeout=10)
        mock_client.__enter__.return_value.starttls.assert_called_once()


@pytest.mark.asyncio
async def test_integrity_log_missing_file() -> None:
    workspace = Path.cwd() / ".test_runtime" / f"logging_test_{uuid4().hex}"
    logs_dir = workspace / "logs"
    config = LoggingConfig(
        level="INFO",
        directory=logs_dir,
        sqlite_path=workspace / "logs.db",
        verifier_interval_seconds=60,
        console_rich_tracebacks=False,
    )
    workspace.mkdir(parents=True, exist_ok=True)
    try:
        manager = IntegrityLogManager(
            config, Mailer(SMTPConfig(enabled=False)), "root@example.com"
        )
        manager.initialize()

        logger = logging.getLogger("tests.integrity.missing")
        logger.info("baseline log")
        manager.finalize()

        # Delete the file
        log_file = next(logs_dir.glob("*.log"))
        log_file.unlink()

        verifier = IntegrityLogManager(
            config, Mailer(SMTPConfig(enabled=False)), "root@example.com"
        )
        verifier.initialize()
        detections = await verifier.verify_integrity()
        verifier.finalize()

        assert len(detections) == 1
        assert detections[0].reason == "log file missing"
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


@pytest.mark.asyncio
async def test_integrity_log_invalid_json() -> None:
    workspace = Path.cwd() / ".test_runtime" / f"logging_test_{uuid4().hex}"
    logs_dir = workspace / "logs"
    config = LoggingConfig(
        level="INFO",
        directory=logs_dir,
        sqlite_path=workspace / "logs.db",
        verifier_interval_seconds=60,
        console_rich_tracebacks=False,
    )
    workspace.mkdir(parents=True, exist_ok=True)
    try:
        manager = IntegrityLogManager(
            config, Mailer(SMTPConfig(enabled=False)), "root@example.com"
        )
        manager.initialize()

        logger = logging.getLogger("tests.integrity.json")
        logger.info("baseline log")
        manager.finalize()

        # Write invalid JSON to log file
        log_file = next(logs_dir.glob("*.log"))
        log_file.write_text("invalid json lines here\n", encoding="utf-8")

        verifier = IntegrityLogManager(
            config, Mailer(SMTPConfig(enabled=False)), "root@example.com"
        )
        verifier.initialize()
        detections = await verifier.verify_integrity()
        verifier.finalize()

        assert len(detections) == 1
        assert detections[0].reason == "invalid JSON line"
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


@pytest.mark.asyncio
async def test_integrity_log_exception_formatting() -> None:
    workspace = Path.cwd() / ".test_runtime" / f"logging_test_{uuid4().hex}"
    logs_dir = workspace / "logs"
    config = LoggingConfig(
        level="INFO",
        directory=logs_dir,
        sqlite_path=workspace / "logs.db",
        verifier_interval_seconds=60,
        console_rich_tracebacks=False,
    )
    workspace.mkdir(parents=True, exist_ok=True)
    try:
        manager = IntegrityLogManager(
            config, Mailer(SMTPConfig(enabled=False)), "root@example.com"
        )
        manager.initialize()

        logger = logging.getLogger("tests.integrity.exc")
        try:
            raise ValueError("Test error")
        except ValueError:
            logger.exception("exception occurred")
        manager.finalize()

        # Verify that file has been sealed without errors
        log_file = next(logs_dir.glob("*.log"))
        assert "Test error" in log_file.read_text(encoding="utf-8")
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


@pytest.mark.asyncio
async def test_integrity_log_notification_on_tamper() -> None:
    workspace = Path.cwd() / ".test_runtime" / f"logging_test_{uuid4().hex}"
    logs_dir = workspace / "logs"
    config = LoggingConfig(
        level="INFO",
        directory=logs_dir,
        sqlite_path=workspace / "logs.db",
        verifier_interval_seconds=60,
        console_rich_tracebacks=False,
    )
    workspace.mkdir(parents=True, exist_ok=True)
    try:
        mock_mailer = MagicMock()
        mock_mailer.send_email = AsyncMock(return_value=True)

        manager = IntegrityLogManager(config, mock_mailer, "admin@example.com")
        manager.initialize()

        logger = logging.getLogger("tests.integrity.notify")
        logger.info("baseline log")
        manager.finalize()

        # Delete the file
        log_file = next(logs_dir.glob("*.log"))
        log_file.unlink()

        verifier = IntegrityLogManager(config, mock_mailer, "admin@example.com")
        verifier.initialize()
        await verifier.verify_integrity()
        verifier.finalize()

        # Ensure mailer was called to report tamper detection
        mock_mailer.send_email.assert_called_once()
        args = mock_mailer.send_email.call_args[0]
        assert args[0] == "admin@example.com"
        assert "[Multiplex] Log integrity violation detected" in args[1]
    finally:
        shutil.rmtree(workspace, ignore_errors=True)
