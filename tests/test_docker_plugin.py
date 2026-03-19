from __future__ import annotations

import pytest

from server.mcp.plugins import docker
from server.models import UserPrincipal


class DummyServices:
    pass


@pytest.mark.asyncio
async def test_list_containers_parses_docker_json(monkeypatch) -> None:
    async def fake_run(*args: str):
        return 0, '{"ID":"abc","Image":"nginx","Names":"web"}\n', ""

    monkeypatch.setattr(docker, "_run_docker_command", fake_run)
    result = await docker.list_containers(
        context=type("Context", (), {"user": UserPrincipal(user_id="root", username="root", is_root=True), "services": DummyServices(), "request_meta": {}})(),
        arguments={},
    )
    assert result["count"] == 1
    assert result["containers"][0]["Names"] == "web"


@pytest.mark.asyncio
async def test_restart_container_requires_container_name() -> None:
    with pytest.raises(RuntimeError):
        await docker.restart_container(
            context=type("Context", (), {"user": UserPrincipal(user_id="root", username="root", is_root=True), "services": DummyServices(), "request_meta": {}})(),
            arguments={},
        )
