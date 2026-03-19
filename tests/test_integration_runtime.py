from __future__ import annotations

from urllib.parse import parse_qs, urlparse

import pytest

from server.core.database import SETTINGS
from server.core.security import build_pkce_challenge


@pytest.mark.asyncio
async def test_bootstrap_creates_runtime_root_plugins_and_safe_runtime_upserts(integration_env) -> None:
    services = integration_env["services"]
    cfg = integration_env["settings"]

    runtime = await services.settings_service.get_runtime_settings()
    assert runtime["kind"] == "runtime"
    assert runtime["registration_enabled"] is False
    assert runtime["mcp_enabled"] is True
    assert "created_at" in runtime
    assert "updated_at" in runtime

    root_doc = await services.users.get_user_by_username(cfg.root.username)
    assert root_doc is not None
    assert root_doc["is_root"] is True
    assert "docker.containers.read" in root_doc["permissions"]
    assert "mail.send" in root_doc["permissions"]

    plugins = {item["key"]: item for item in await services.plugins.list_plugins()}
    assert {"docker", "mail"} <= set(plugins)

    tools = {item["key"]: item for item in await services.plugins.list_tools()}
    assert tools["docker.list_containers"]["global_enabled"] is True
    assert tools["docker.restart_container"]["global_enabled"] is True
    assert tools["mail.send_test_email"]["global_enabled"] is False

    await services.db.collection(SETTINGS).delete_one({"_id": "runtime"})
    actor = services.users.to_principal(root_doc)
    recreated = await services.settings_service.set_registration(True, actor=actor, request_meta={"ip": "127.0.0.1", "user_agent": "pytest"})
    assert recreated["kind"] == "runtime"
    assert recreated["registration_enabled"] is True
    assert recreated["mcp_enabled"] is True
    assert "created_at" in recreated


@pytest.mark.asyncio
async def test_rest_oauth_and_mcp_flow_respects_user_scoping(integration_env) -> None:
    client = integration_env["client"]
    services = integration_env["services"]
    cfg = integration_env["settings"]

    root_login = await client.post(
        "/api/auth/login",
        json={"username": cfg.root.username, "password": cfg.root.password.get_secret_value()},
    )
    assert root_login.status_code == 200
    root_access = root_login.json()["access_token"]
    root_headers = {"Authorization": f"Bearer {root_access}"}

    toggle_registration = await client.put("/api/settings/registration", headers=root_headers, json={"enabled": True})
    assert toggle_registration.status_code == 200
    assert toggle_registration.json()["registration_enabled"] is True

    register = await client.post(
        "/api/auth/register",
        json={
            "username": "alice",
            "password": "AlicePassword123!",
            "email": "alice@example.com",
            "tg_id": "111",
            "vk_id": "222",
        },
    )
    assert register.status_code == 201

    alice_login = await client.post("/api/auth/login", json={"username": "alice", "password": "AlicePassword123!"})
    assert alice_login.status_code == 200
    alice_access = alice_login.json()["access_token"]
    alice_headers = {"Authorization": f"Bearer {alice_access}"}

    alice_me = await client.get("/api/auth/me", headers=alice_headers)
    assert alice_me.status_code == 200
    alice_id = alice_me.json()["user_id"]

    grant_perm = await client.put(
        f"/api/users/{alice_id}/permissions",
        headers=root_headers,
        json={"permissions": ["docker.containers.read"], "mode": "grant"},
    )
    assert grant_perm.status_code == 200
    assert "docker.containers.read" in grant_perm.json()["permissions"]

    enable_tool = await client.put(
        f"/api/mcp/users/{alice_id}/tools/docker.list_containers",
        headers=root_headers,
        json={"enabled": True},
    )
    assert enable_tool.status_code == 200
    assert enable_tool.json()["effective_enabled"] is True

    oauth_client = await client.post(
        "/api/oauth/clients",
        headers=root_headers,
        json={
            "name": "Integration MCP Client",
            "redirect_uris": ["https://example.test/callback"],
            "allowed_scopes": ["mcp"],
        },
    )
    assert oauth_client.status_code == 201
    oauth_client_id = oauth_client.json()["client_id"]

    verifier = "integration-pkce-verifier-123456789"
    challenge = build_pkce_challenge(verifier)

    authorize_page = await client.get(
        "/api/oauth/authorize",
        params={
            "response_type": "code",
            "client_id": oauth_client_id,
            "redirect_uri": "https://example.test/callback",
            "scope": "mcp",
            "state": "integration",
            "code_challenge": challenge,
            "code_challenge_method": "S256",
        },
    )
    assert authorize_page.status_code == 200
    assert "Authorize Integration MCP Client" in authorize_page.text

    authorize = await client.post(
        "/api/oauth/authorize",
        data={
            "response_type": "code",
            "client_id": oauth_client_id,
            "redirect_uri": "https://example.test/callback",
            "scope": "mcp",
            "state": "integration",
            "code_challenge": challenge,
            "code_challenge_method": "S256",
            "username": "alice",
            "password": "AlicePassword123!",
            "approve": "true",
        },
        follow_redirects=False,
    )
    assert authorize.status_code == 302
    redirect_url = authorize.headers["location"]
    code = parse_qs(urlparse(redirect_url).query)["code"][0]

    token = await client.post(
        "/api/oauth/token",
        data={
            "grant_type": "authorization_code",
            "client_id": oauth_client_id,
            "redirect_uri": "https://example.test/callback",
            "code": code,
            "code_verifier": verifier,
        },
    )
    assert token.status_code == 200
    oauth_access = token.json()["access_token"]
    mcp_headers = {"Authorization": f"Bearer {oauth_access}"}

    tool_manifest_list = await client.post(
        "/mcp",
        headers=mcp_headers,
        json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
    )
    assert tool_manifest_list.status_code == 200
    tool_names = {item["name"] for item in tool_manifest_list.json()["result"]["tools"]}
    assert "docker.list_containers" in tool_names
    assert "docker.restart_container" not in tool_names

    async def fake_list_containers(context, arguments):
        return {"containers": [{"Names": "web"}], "count": 1, "user": context.user.username}

    services.plugins.plugins["docker"].tools["docker.list_containers"].handler = fake_list_containers

    tool_call = await client.post(
        "/mcp",
        headers=mcp_headers,
        json={
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "docker.list_containers", "arguments": {"all": False}},
        },
    )
    assert tool_call.status_code == 200
    structured = tool_call.json()["result"]["structuredContent"]
    assert structured["count"] == 1
    assert structured["user"] == "alice"

    denied_call = await client.post(
        "/mcp",
        headers=mcp_headers,
        json={
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "docker.restart_container", "arguments": {"container": "web"}},
        },
    )
    assert denied_call.status_code == 200
    assert denied_call.json()["error"]["message"] == "Tool access denied"
