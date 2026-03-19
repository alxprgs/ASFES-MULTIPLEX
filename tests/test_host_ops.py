from __future__ import annotations

import shutil
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from server.core.config import settings as base_settings
from server.host_ops import HostOpsError, HostOpsService


@pytest.fixture
def workspace() -> Path:
    root = Path.cwd() / ".test_runtime" / f"host_ops_{uuid4().hex}"
    root.mkdir(parents=True, exist_ok=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


def build_host_ops(tmp_path: Path) -> HostOpsService:
    cfg = base_settings.model_copy(deep=True)
    cfg.host_ops.managed_file_roots = [tmp_path / "managed"]
    cfg.host_ops.managed_log_roots = [tmp_path / "managed" / "logs"]
    cfg.host_ops.backup_directory = tmp_path / "backups"
    cfg.host_ops.max_output_bytes = 32
    for root in cfg.host_ops.managed_file_roots + cfg.host_ops.managed_log_roots:
        root.mkdir(parents=True, exist_ok=True)
    return HostOpsService(cfg)


def test_resolve_managed_path_blocks_escape(workspace: Path) -> None:
    host_ops = build_host_ops(workspace)
    with pytest.raises(HostOpsError):
        host_ops.resolve_managed_path("..\\escape.txt")


def test_redact_arguments_and_truncate_strings(workspace: Path) -> None:
    host_ops = build_host_ops(workspace)
    payload = {
        "password": "super-secret",
        "nested": {"content": "x" * 64, "safe": "ok"},
    }
    redacted = host_ops.redact_arguments(payload, sensitive_fields=["content"], max_string_length=16)
    assert redacted["password"] == "[REDACTED]"
    assert redacted["nested"]["content"] == "[REDACTED]"
    assert redacted["nested"]["safe"] == "ok"


def test_atomic_write_creates_backup(workspace: Path) -> None:
    host_ops = build_host_ops(workspace)
    managed_root = host_ops.managed_file_roots()[0]
    target = managed_root / "sample.txt"
    target.write_text("before", encoding="utf-8")
    result = host_ops.atomic_write_text("sample.txt", "after", roots=host_ops.managed_file_roots())
    assert result["backup_created"] is True
    assert target.read_text(encoding="utf-8") == "after"
    assert any(host_ops.backup_directory().iterdir())


@pytest.mark.asyncio
async def test_run_truncates_large_output(workspace: Path) -> None:
    host_ops = build_host_ops(workspace)
    result = await host_ops.run([sys.executable, "-c", "print('x' * 1024)"])
    assert result.truncated is True
    assert result.stdout.endswith("...[truncated]")
