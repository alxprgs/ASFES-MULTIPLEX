from __future__ import annotations

from urllib.parse import parse_qs, urlparse

import httpx
import pytest
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport

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
    assert tools["docker.restart_container"]["global_enabled"] is False
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
    app = integration_env["app"]
    client = integration_env["client"]
    mcp_gateway = integration_env["mcp_gateway"]
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

    def mcp_httpx_client_factory(**kwargs):
        return httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
            **kwargs,
        )

    mcp_transport = StreamableHttpTransport(
        "http://testserver/mcp",
        auth=oauth_access,
        httpx_client_factory=mcp_httpx_client_factory,
    )

    async def fake_list_containers(context, arguments):
        return {"containers": [{"Names": "web"}], "count": 1, "user": context.user.username}

    services.plugins.plugins["docker"].tools["docker.list_containers"].handler = fake_list_containers

    async with mcp_gateway.lifespan():
        async with Client(mcp_transport) as mcp_client:
            tools = await mcp_client.list_tools()
            tool_names = {tool.name for tool in tools}
            assert "docker.list_containers" in tool_names
            assert "docker.restart_container" not in tool_names

            tool_call = await mcp_client.call_tool(
                "docker.list_containers",
                {"all": False},
                raise_on_error=False,
            )
            assert tool_call.is_error is False
            assert tool_call.structured_content is not None
            assert tool_call.structured_content["count"] == 1
            assert tool_call.structured_content["user"] == "alice"

            denied_call = await mcp_client.call_tool(
                "docker.restart_container",
                {"container": "web"},
                raise_on_error=False,
            )
            assert denied_call.is_error is True
            assert any("Tool access denied" in getattr(item, "text", "") for item in denied_call.content)


@pytest.mark.asyncio
async def test_cookie_auth_csrf_and_bearer_compatibility(integration_env) -> None:
    client = integration_env["client"]
    services = integration_env["services"]
    cfg = integration_env["settings"]

    login = await client.post(
        "/api/auth/login",
        json={"username": cfg.root.username, "password": cfg.root.password.get_secret_value()},
    )
    assert login.status_code == 200
    access_token = login.json()["access_token"]
    assert client.cookies.get(cfg.access_cookie_name)
    csrf_token = client.cookies.get(cfg.csrf_cookie_name)
    assert csrf_token

    cookie_me = await client.get("/api/auth/me")
    assert cookie_me.status_code == 200
    assert cookie_me.json()["username"] == cfg.root.username

    blocked_write = await client.put("/api/settings/mcp", json={"enabled": False})
    assert blocked_write.status_code == 403

    cookie_write = await client.put("/api/settings/mcp", headers={"X-CSRF-Token": csrf_token}, json={"enabled": False})
    assert cookie_write.status_code == 200
    assert cookie_write.json()["mcp_enabled"] is False

    bearer_write = await client.put(
        "/api/settings/mcp",
        headers={"Authorization": f"Bearer {access_token}"},
        json={"enabled": True},
    )
    assert bearer_write.status_code == 200
    assert bearer_write.json()["mcp_enabled"] is True

    logout = await client.post("/api/auth/logout", headers={"X-CSRF-Token": csrf_token})
    assert logout.status_code == 204
    assert not client.cookies.get(cfg.access_cookie_name)
    assert await services.settings_service.get_runtime_settings()


@pytest.mark.asyncio
async def test_admin_users_and_plugin_toggle(integration_env) -> None:
    client = integration_env["client"]
    cfg = integration_env["settings"]

    login = await client.post(
        "/api/auth/login",
        json={"username": cfg.root.username, "password": cfg.root.password.get_secret_value()},
    )
    assert login.status_code == 200
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}

    users = await client.get("/api/users", headers=headers)
    assert users.status_code == 200
    assert any(item["username"] == cfg.root.username for item in users.json())

    disabled = await client.put("/api/mcp/plugins/docker", headers=headers, json={"enabled": False})
    assert disabled.status_code == 200
    assert disabled.json()["enabled"] is False

    enabled = await client.put("/api/mcp/plugins/docker", headers=headers, json={"enabled": True})
    assert enabled.status_code == 200
    assert enabled.json()["enabled"] is True
