from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_admin_bootstrap_anonymous(integration_env) -> None:
    client = integration_env["client"]
    resp = await client.get("/api/bootstrap")
    assert resp.status_code == 200
    assert resp.json()["user"] is None
    assert resp.json()["runtime"] is None


@pytest.mark.asyncio
async def test_admin_profile_update(integration_env) -> None:
    client = integration_env["client"]
    cfg = integration_env["settings"]

    # Login
    login = await client.post(
        "/api/auth/login",
        json={
            "username": cfg.root.username,
            "password": cfg.root.password.get_secret_value(),
        },
    )
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}

    # Update profile
    resp = await client.put(
        "/api/account/profile",
        headers=headers,
        json={"email": "new_email@example.com", "tg_id": "999", "vk_id": "888"},
    )
    assert resp.status_code == 200
    assert resp.json()["email"] == "new_email@example.com"
    assert resp.json()["tg_id"] == "999"
    assert resp.json()["vk_id"] == "888"


@pytest.mark.asyncio
async def test_admin_permissions_list(integration_env) -> None:
    client = integration_env["client"]
    cfg = integration_env["settings"]

    login = await client.post(
        "/api/auth/login",
        json={
            "username": cfg.root.username,
            "password": cfg.root.password.get_secret_value(),
        },
    )
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}

    resp = await client.get("/api/permissions", headers=headers)
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)
    assert len(resp.json()) > 0


@pytest.mark.asyncio
async def test_admin_mutate_permissions_failures(integration_env) -> None:
    client = integration_env["client"]
    cfg = integration_env["settings"]

    login = await client.post(
        "/api/auth/login",
        json={
            "username": cfg.root.username,
            "password": cfg.root.password.get_secret_value(),
        },
    )
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}

    # Case 1: non-existing user (404)
    resp1 = await client.put(
        "/api/users/64c9a51d2f6f4e1f7a8b9c1d/permissions",
        headers=headers,
        json={"permissions": ["docker.containers.read"], "mode": "grant"},
    )
    assert resp1.status_code == 404

    # Case 2: invalid mode (400)
    user_doc = await integration_env["services"].users.create_user(
        username="test_mutate_user",
        password="TestPassword123!",
        email="test_mutate@example.com",
        actor=None,
        request_meta={},
    )
    user_id = user_doc["_id"]
    resp2 = await client.put(
        f"/api/users/{user_id}/permissions",
        headers=headers,
        json={"permissions": ["alerts.read"], "mode": "invalid_mode"},
    )
    assert resp2.status_code == 400


@pytest.mark.asyncio
async def test_admin_settings_getters(integration_env) -> None:
    client = integration_env["client"]
    cfg = integration_env["settings"]

    login = await client.post(
        "/api/auth/login",
        json={
            "username": cfg.root.username,
            "password": cfg.root.password.get_secret_value(),
        },
    )
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}

    for endpoint in ["registration", "mcp", "redis"]:
        resp = await client.get(f"/api/settings/{endpoint}", headers=headers)
        assert resp.status_code == 200
        assert "redis_mode" in resp.json()


@pytest.mark.asyncio
async def test_admin_settings_redis_conflict(integration_env) -> None:
    client = integration_env["client"]
    cfg = integration_env["settings"]

    login = await client.post(
        "/api/auth/login",
        json={
            "username": cfg.root.username,
            "password": cfg.root.password.get_secret_value(),
        },
    )
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}

    # Try setting redis runtime enabled, which should raise a ValueError (409) if Redis mode is required and we try to disable it
    original_mode = cfg.redis.mode
    cfg.redis.mode = "required"
    try:
        resp = await client.put(
            "/api/settings/redis", headers=headers, json={"enabled": False}
        )
        assert resp.status_code == 409
    finally:
        cfg.redis.mode = original_mode


@pytest.mark.asyncio
async def test_admin_mcp_tools_failures(integration_env) -> None:
    client = integration_env["client"]
    cfg = integration_env["settings"]

    login = await client.post(
        "/api/auth/login",
        json={
            "username": cfg.root.username,
            "password": cfg.root.password.get_secret_value(),
        },
    )
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}

    # 1. GET non-existing tool -> 404
    resp = await client.get("/api/mcp/tools/non_existent_tool", headers=headers)
    assert resp.status_code == 404

    # 2. PUT non-existing tool -> 404
    resp = await client.put(
        "/api/mcp/tools/non_existent_tool", headers=headers, json={"enabled": True}
    )
    assert resp.status_code == 404

    # 3. GET user tool state for non-existing user -> 404
    resp = await client.get(
        "/api/mcp/users/64c9a51d2f6f4e1f7a8b9c1d/tools/docker.list_containers",
        headers=headers,
    )
    assert resp.status_code == 404

    # 4. PUT user tool state for non-existing user -> 404
    resp = await client.put(
        "/api/mcp/users/64c9a51d2f6f4e1f7a8b9c1d/tools/docker.list_containers",
        headers=headers,
        json={"enabled": True},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_admin_update_sessions_404(integration_env) -> None:
    client = integration_env["client"]
    cfg = integration_env["settings"]

    login = await client.post(
        "/api/auth/login",
        json={
            "username": cfg.root.username,
            "password": cfg.root.password.get_secret_value(),
        },
    )
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}

    # Get session 404
    resp1 = await client.get(
        "/api/system/update/sessions/non_existent_session", headers=headers
    )
    assert resp1.status_code == 404

    # Stream session events 404
    resp2 = await client.get(
        "/api/system/update/sessions/non_existent_session/events", headers=headers
    )
    assert resp2.status_code == 404


@pytest.mark.asyncio
async def test_admin_mcp_plugins_404_and_mutations(integration_env) -> None:
    client = integration_env["client"]
    cfg = integration_env["settings"]

    login = await client.post(
        "/api/auth/login",
        json={
            "username": cfg.root.username,
            "password": cfg.root.password.get_secret_value(),
        },
    )
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}

    # PUT non-existing plugin -> 404
    resp1 = await client.put(
        "/api/mcp/plugins/non_existent_plugin", headers=headers, json={"enabled": False}
    )
    assert resp1.status_code == 404
