from __future__ import annotations

import logging
import shutil
from pathlib import Path
from uuid import uuid4

import pytest

from server.core.config import LoggingConfig, SMTPConfig
from server.core.logging import IntegrityLogManager, Mailer


@pytest.mark.asyncio
async def test_integrity_verifier_detects_tampered_log() -> None:
    workspace = Path.cwd() / ".test_runtime" / uuid4().hex
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
        manager = IntegrityLogManager(config, Mailer(SMTPConfig(enabled=False)), "root@example.com")
        manager.initialize()

        logger = logging.getLogger("tests.integrity")
        logger.info("integrity baseline", extra={"event_type": "tests.integrity", "payload": {"step": 1}})
        manager.finalize()

        log_file = next(logs_dir.glob("*.log"))
        original = log_file.read_text(encoding="utf-8")
        tampered = original.replace("baseline", "tampered", 1)
        log_file.write_text(tampered, encoding="utf-8")

        verifier = IntegrityLogManager(config, Mailer(SMTPConfig(enabled=False)), "root@example.com")
        verifier.initialize()
        detections = await verifier.verify_integrity()
        verifier.finalize()

        assert detections
        assert detections[0].reason in {"line hash mismatch", "hash chain mismatch", "sealed file hash mismatch"}
    finally:
        shutil.rmtree(workspace, ignore_errors=True)
