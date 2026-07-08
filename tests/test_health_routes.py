from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, patch


@pytest.mark.asyncio
async def test_healthcheck_success(integration_env) -> None:
    client = integration_env["client"]

    resp = await client.get("/api/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_healthcheck_degraded(integration_env) -> None:
    client = integration_env["client"]
    services = integration_env["services"]

    mock_db = AsyncMock()
    mock_db.command.side_effect = Exception("Connection lost")
    with patch.object(services.db.client, "admin", mock_db):
        resp = await client.get("/api/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "degraded"


@pytest.mark.asyncio
async def test_healthcheck_details_success(integration_env) -> None:
    client = integration_env["client"]
    cfg = integration_env["settings"]

    # Login as root to get permission
    login = await client.post(
        "/api/auth/login",
        json={
            "username": cfg.root.username,
            "password": cfg.root.password.get_secret_value(),
        },
    )
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}

    resp = await client.get("/api/health/details", headers=headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["mongodb"] == "ok"
    assert data["redis"] in ("enabled", "disabled")
    assert isinstance(data["mcp_enabled"], bool)


@pytest.mark.asyncio
async def test_healthcheck_details_permission_denied(integration_env) -> None:
    client = integration_env["client"]

    # Call details without auth -> 401
    resp1 = await client.get("/api/health/details")
    assert resp1.status_code == 401


@pytest.mark.asyncio
async def test_healthcheck_details_degraded(integration_env) -> None:
    client = integration_env["client"]
    cfg = integration_env["settings"]
    services = integration_env["services"]

    # Login
    login = await client.post(
        "/api/auth/login",
        json={
            "username": cfg.root.username,
            "password": cfg.root.password.get_secret_value(),
        },
    )
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}

    # Simulate MongoDB failure by overriding the admin attribute with mock_db
    mock_db = AsyncMock()
    mock_db.command.side_effect = Exception("Connection lost")
    with patch.object(services.db.client, "admin", mock_db):
        resp = await client.get("/api/health/details", headers=headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "degraded"
        assert data["mongodb"] == "error"
