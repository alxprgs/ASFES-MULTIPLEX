from __future__ import annotations

from datetime import timedelta
import pytest
import httpx
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport

from server.core.database import API_KEYS
from server.core.security import sha256_text, now_utc


@pytest.mark.asyncio
async def test_api_keys_full_flow(integration_env) -> None:
    app = integration_env["app"]
    client = integration_env["client"]
    cfg = integration_env["settings"]

    # 1. Login as root
    login_resp = await client.post(
        "/api/auth/login",
        json={
            "username": cfg.root.username,
            "password": cfg.root.password.get_secret_value(),
        },
    )
    assert login_resp.status_code == 200
    access_token = login_resp.json()["access_token"]
    auth_headers = {"Authorization": f"Bearer {access_token}"}

    # 2. Create API key
    create_resp = await client.post(
        "/api/auth/api-keys",
        headers=auth_headers,
        json={"name": "Antigravity Key", "expires_in_days": 30},
    )
    assert create_resp.status_code == 201
    key_data = create_resp.json()
    assert "token" in key_data
    token = key_data["token"]
    assert token.startswith("asfes_")
    assert key_data["name"] == "Antigravity Key"
    assert key_data["token_prefix"] == token[:12]
    key_id = key_data["key_id"]

    # 3. List API keys and verify it is present
    list_resp = await client.get("/api/auth/api-keys", headers=auth_headers)
    assert list_resp.status_code == 200
    keys_list = list_resp.json()
    assert len(keys_list) >= 1
    matched = [k for k in keys_list if k["key_id"] == key_id]
    assert len(matched) == 1
    assert "token" not in matched[0]  # Token should not be in listing

    # 4. Use API key for REST authentication
    api_key_headers = {"Authorization": f"Bearer {token}"}
    me_resp = await client.get("/api/auth/api-keys", headers=api_key_headers)
    assert me_resp.status_code == 200  # Authenticated successfully and can call API

    # 5. Use API key for MCP authentication
    def mcp_httpx_client_factory(**kwargs):
        return httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
            **kwargs,
        )

    mcp_transport = StreamableHttpTransport(
        "http://testserver/mcp",
        auth=token,
        httpx_client_factory=mcp_httpx_client_factory,
    )

    async with integration_env["mcp_gateway"].lifespan():
        async with Client(mcp_transport) as mcp_client:
            tools = await mcp_client.list_tools()
            assert isinstance(tools, list)

    # 6. Update API key (Rename)
    patch_resp = await client.patch(
        f"/api/auth/api-keys/{key_id}",
        headers=auth_headers,
        json={"name": "Antigravity Updated Key"},
    )
    assert patch_resp.status_code == 200
    assert patch_resp.json()["name"] == "Antigravity Updated Key"

    # Verify rename reflected in listing
    list_resp2 = await client.get("/api/auth/api-keys", headers=auth_headers)
    matched2 = [k for k in list_resp2.json() if k["key_id"] == key_id]
    assert matched2[0]["name"] == "Antigravity Updated Key"

    # 7. Revoke API key
    revoke_resp = await client.delete(
        f"/api/auth/api-keys/{key_id}", headers=auth_headers
    )
    assert revoke_resp.status_code == 204

    # Verify it is no longer usable (401)
    failed_resp = await client.get("/api/auth/api-keys", headers=api_key_headers)
    assert failed_resp.status_code == 401


@pytest.mark.asyncio
async def test_api_keys_max_limit(integration_env) -> None:
    client = integration_env["client"]
    cfg = integration_env["settings"]

    # Login
    login_resp = await client.post(
        "/api/auth/login",
        json={
            "username": cfg.root.username,
            "password": cfg.root.password.get_secret_value(),
        },
    )
    access_token = login_resp.json()["access_token"]
    auth_headers = {"Authorization": f"Bearer {access_token}"}

    # Create 20 keys
    for i in range(20):
        resp = await client.post(
            "/api/auth/api-keys",
            headers=auth_headers,
            json={"name": f"Key {i}"},
        )
        assert resp.status_code == 201

    # Try creating 21st key -> should fail
    fail_resp = await client.post(
        "/api/auth/api-keys",
        headers=auth_headers,
        json={"name": "Key 21"},
    )
    assert fail_resp.status_code == 400
    assert "Maximum number of API keys" in fail_resp.json()["detail"]


@pytest.mark.asyncio
async def test_api_keys_expiration(integration_env) -> None:
    client = integration_env["client"]
    services = integration_env["services"]
    cfg = integration_env["settings"]

    # Login
    login_resp = await client.post(
        "/api/auth/login",
        json={
            "username": cfg.root.username,
            "password": cfg.root.password.get_secret_value(),
        },
    )
    access_token = login_resp.json()["access_token"]
    auth_headers = {"Authorization": f"Bearer {access_token}"}

    # Create a key with 30 days expiry
    create_resp = await client.post(
        "/api/auth/api-keys",
        headers=auth_headers,
        json={"name": "Temporary Key", "expires_in_days": 30},
    )
    assert create_resp.status_code == 201
    key_data = create_resp.json()
    token = key_data["token"]

    # Manually expire the key in the database
    token_hash = sha256_text(token)
    await services.db.collection(API_KEYS).update_one(
        {"token_hash": token_hash},
        {"$set": {"expires_at": now_utc() - timedelta(seconds=10)}},
    )

    # Use the expired key -> should fail with 401
    api_key_headers = {"Authorization": f"Bearer {token}"}
    expired_resp = await client.get("/api/auth/api-keys", headers=api_key_headers)
    assert expired_resp.status_code == 401
