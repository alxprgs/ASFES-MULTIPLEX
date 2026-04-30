from __future__ import annotations

import pytest

from server.core.config import settings as base_settings
from server.update_manager import UpdateManager


@pytest.mark.asyncio
async def test_update_check_session_emits_status_and_stage_events(monkeypatch) -> None:
    manager = UpdateManager(base_settings.model_copy(deep=True))

    async def fake_check_code(session):
        await manager._emit(session, "stage", {"stage": session.stages["code"].to_dict()})
        return True

    async def fake_check_python(session):
        return False

    async def fake_check_frontend(session):
        return True

    monkeypatch.setattr(manager, "_check_code", fake_check_code)
    monkeypatch.setattr(manager, "_check_python", fake_check_python)
    monkeypatch.setattr(manager, "_check_frontend", fake_check_frontend)

    session = await manager.start_check()
    events = [event async for event in manager.events(session)]

    assert session.status == "success"
    assert session.requires_restart is True
    assert session.stages["code"].needed is True
    assert session.stages["python"].needed is False
    assert session.stages["frontend"].needed is True
    assert {event["type"] for event in events} >= {"status", "stage"}


@pytest.mark.asyncio
async def test_update_run_accepts_forced_stages_and_blocks_parallel(monkeypatch) -> None:
    manager = UpdateManager(base_settings.model_copy(deep=True))
    started = []

    async def fake_run_stage(session, stage, stdout_parts, stderr_parts, command_names):
        started.append(stage)
        session.stages[stage].status = "success"
        stdout_parts.append(f"{stage}\n")
        command_names.append(stage)

    monkeypatch.setattr(manager, "_run_stage", fake_run_stage)

    session = await manager.start_update(["code", "restart"], ["restart"])
    with pytest.raises(RuntimeError):
        await manager.start_update(["python"], [])
    await session.task

    assert started == ["code", "restart"]
    assert session.status == "success"
    assert session.requires_restart is True
    assert session.stages["restart"].forced is True
    assert session.result is not None
    assert session.result.stdout == "code\nrestart\n"
