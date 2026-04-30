from __future__ import annotations

from urllib.parse import parse_qs, urlparse

import httpx
import pytest
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport

from server.core.database import SETTINGS
from server.core.security import build_pkce_challenge, totp_code


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
    assert tools["docker.list_containers"]["global_enabled"] is False
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

    enable_global_tool = await client.put(
        "/api/mcp/tools/docker.list_containers",
        headers=root_headers,
        json={"enabled": True},
    )
    assert enable_global_tool.status_code == 200
    assert enable_global_tool.json()["global_enabled"] is True

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

            connected = await client.get("/api/mcp/connected-services", headers=root_headers)
            assert connected.status_code == 200
            service = next(item for item in connected.json() if item["client_id"] == oauth_client_id)
            assert service["active_session_count"] == 1
            assert service["user_count"] == 1
            assert service["last_tool_call_at"] is not None

            denied_call = await mcp_client.call_tool(
                "docker.restart_container",
                {"container": "web"},
                raise_on_error=False,
            )
            assert denied_call.is_error is True
            assert any("Tool access denied" in getattr(item, "text", "") for item in denied_call.content)


@pytest.mark.asyncio
async def test_mcp_oauth_discovery_supports_bare_mcp_path(integration_env) -> None:
    client = integration_env["client"]
    cfg = integration_env["settings"]

    bare_get = await client.get("/mcp")
    assert bare_get.status_code == 401
    assert "resource_metadata=" in bare_get.headers["www-authenticate"]
    assert f"/.well-known/oauth-protected-resource{cfg.mcp_path}" in bare_get.headers["www-authenticate"]

    bare_post = await client.post("/mcp", json={})
    assert bare_post.status_code == 401
    assert "resource_metadata=" in bare_post.headers["www-authenticate"]

    slash_get = await client.get("/mcp/")
    assert slash_get.status_code == 401
    assert slash_get.headers["www-authenticate"] == bare_get.headers["www-authenticate"]


@pytest.mark.asyncio
async def test_oauth_metadata_includes_resource_scopes_and_path_aware_issuer(integration_env) -> None:
    client = integration_env["client"]
    cfg = integration_env["settings"]

    protected = await client.get("/.well-known/oauth-protected-resource/mcp")
    assert protected.status_code == 200
    assert protected.json() == {
        "resource": f"{cfg.public_base_url}{cfg.mcp_path}",
        "authorization_servers": [cfg.oauth_issuer],
        "bearer_methods_supported": ["header"],
        "scopes_supported": cfg.oauth.supported_scopes,
    }

    issuer_metadata = await client.get("/.well-known/oauth-authorization-server/api/oauth")
    assert issuer_metadata.status_code == 200
    assert issuer_metadata.json()["issuer"] == cfg.oauth_issuer

    mcp_path_metadata = await client.get("/.well-known/oauth-authorization-server/mcp")
    assert mcp_path_metadata.status_code == 200
    assert mcp_path_metadata.json()["authorization_endpoint"] == cfg.authorization_endpoint

    resource_nested_metadata = await client.get("/mcp/.well-known/oauth-authorization-server")
    assert resource_nested_metadata.status_code == 200
    assert resource_nested_metadata.json()["token_endpoint"] == cfg.token_endpoint

    missing = await client.get("/.well-known/oauth-authorization-server/other")
    assert missing.status_code == 404


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
async def test_health_details_requires_permission(integration_env) -> None:
    client = integration_env["client"]
    cfg = integration_env["settings"]

    public_health = await client.get("/api/health")
    assert public_health.status_code == 200
    assert set(public_health.json()) == {"status"}

    denied = await client.get("/api/health/details")
    assert denied.status_code == 401

    login = await client.post("/api/auth/login", json={"username": cfg.root.username, "password": cfg.root.password.get_secret_value()})
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}
    details = await client.get("/api/health/details", headers=headers)
    assert details.status_code == 200
    assert {"status", "mongodb", "redis", "mcp_enabled"} <= set(details.json())


@pytest.mark.asyncio
async def test_oauth_hardening_for_dynamic_registration_confidential_clients_and_pkce(integration_env) -> None:
    client = integration_env["client"]
    cfg = integration_env["settings"]

    anonymous_register = await client.post(
        "/api/oauth/register",
        json={"client_name": "Anonymous", "redirect_uris": ["https://example.test/callback"]},
    )
    assert anonymous_register.status_code == 401

    login = await client.post("/api/auth/login", json={"username": cfg.root.username, "password": cfg.root.password.get_secret_value()})
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}

    dynamic_register = await client.post(
        "/api/oauth/register",
        headers=headers,
        json={"client_name": "Dynamic", "redirect_uris": ["https://example.test/dynamic"], "scope": "mcp"},
    )
    assert dynamic_register.status_code == 201

    bad_redirect = await client.post(
        "/api/oauth/clients",
        headers=headers,
        json={"name": "Bad", "redirect_uris": ["javascript:alert(1)"], "allowed_scopes": ["mcp"]},
    )
    assert bad_redirect.status_code == 400

    confidential = await client.post(
        "/api/oauth/clients",
        headers=headers,
        json={
            "name": "Confidential",
            "redirect_uris": ["https://example.test/confidential"],
            "allowed_scopes": ["mcp"],
            "confidential": True,
        },
    )
    assert confidential.status_code == 201
    confidential_payload = confidential.json()
    client_id = confidential_payload["client_id"]
    client_secret = confidential_payload["client_secret"]
    assert client_secret

    verifier = "confidential-pkce-verifier-123456789"
    challenge = build_pkce_challenge(verifier)
    plain_page = await client.get(
        "/api/oauth/authorize",
        params={
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": "https://example.test/confidential",
            "scope": "mcp",
            "code_challenge": verifier,
            "code_challenge_method": "plain",
        },
    )
    assert plain_page.status_code == 400

    authorize = await client.post(
        "/api/oauth/authorize",
        data={
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": "https://example.test/confidential",
            "scope": "mcp",
            "state": "confidential",
            "code_challenge": challenge,
            "code_challenge_method": "S256",
            "username": cfg.root.username,
            "password": cfg.root.password.get_secret_value(),
            "approve": "true",
        },
        follow_redirects=False,
    )
    assert authorize.status_code == 302
    code = parse_qs(urlparse(authorize.headers["location"]).query)["code"][0]

    missing_secret = await client.post(
        "/api/oauth/token",
        data={
            "grant_type": "authorization_code",
            "client_id": client_id,
            "redirect_uri": "https://example.test/confidential",
            "code": code,
            "code_verifier": verifier,
        },
    )
    assert missing_secret.status_code == 400

    token = await client.post(
        "/api/oauth/token",
        data={
            "grant_type": "authorization_code",
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": "https://example.test/confidential",
            "code": code,
            "code_verifier": verifier,
        },
    )
    assert token.status_code == 200
    refresh_token = token.json()["refresh_token"]

    refresh_denied = await client.post("/api/oauth/token", data={"grant_type": "refresh_token", "client_id": client_id, "refresh_token": refresh_token})
    assert refresh_denied.status_code == 400

    rotate = await client.post(f"/api/oauth/clients/{client_id}/secret/rotate", headers=headers)
    assert rotate.status_code == 200
    next_secret = rotate.json()["client_secret"]
    assert next_secret != client_secret

    revoke = await client.post(
        "/api/oauth/revoke",
        data={"token": refresh_token, "client_id": client_id, "client_secret": next_secret},
    )
    assert revoke.status_code == 200


@pytest.mark.asyncio
async def test_two_factor_protects_api_login_and_mcp_oauth_authorize(integration_env) -> None:
    client = integration_env["client"]
    cfg = integration_env["settings"]

    login = await client.post(
        "/api/auth/login",
        json={"username": cfg.root.username, "password": cfg.root.password.get_secret_value()},
    )
    assert login.status_code == 200
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}

    setup = await client.post(
        "/api/auth/2fa/setup",
        headers=headers,
        json={"current_password": cfg.root.password.get_secret_value()},
    )
    assert setup.status_code == 200
    secret = setup.json()["secret"]
    assert setup.json()["qr_svg"].startswith("<svg")

    enable = await client.post("/api/auth/2fa/enable", headers=headers, json={"code": totp_code(secret)})
    assert enable.status_code == 200
    assert enable.json()["user"]["two_factor_enabled"] is True
    assert len(enable.json()["recovery_codes"]) == 8

    challenged = await client.post(
        "/api/auth/login",
        json={"username": cfg.root.username, "password": cfg.root.password.get_secret_value()},
    )
    assert challenged.status_code == 200
    assert challenged.json()["two_factor_required"] is True
    assert "access_token" not in challenged.json()

    denied = await client.post("/api/auth/login/2fa", json={"challenge_token": challenged.json()["challenge_token"], "code": "000000"})
    assert denied.status_code == 401

    completed = await client.post("/api/auth/login/2fa", json={"challenge_token": challenged.json()["challenge_token"], "code": totp_code(secret)})
    assert completed.status_code == 200
    assert completed.json()["user"]["two_factor_enabled"] is True

    oauth_client = await client.post(
        "/api/oauth/clients",
        headers=headers,
        json={
            "name": "2FA MCP Client",
            "redirect_uris": ["https://example.test/callback"],
            "allowed_scopes": ["mcp"],
        },
    )
    assert oauth_client.status_code == 201
    oauth_client_id = oauth_client.json()["client_id"]
    verifier = "two-factor-pkce-verifier-123456789"
    challenge = build_pkce_challenge(verifier)
    authorize_payload = {
        "response_type": "code",
        "client_id": oauth_client_id,
        "redirect_uri": "https://example.test/callback",
        "scope": "mcp",
        "state": "two-factor",
        "code_challenge": challenge,
        "code_challenge_method": "S256",
        "username": cfg.root.username,
        "password": cfg.root.password.get_secret_value(),
        "approve": "true",
    }

    oauth_denied = await client.post("/api/oauth/authorize", data=authorize_payload, follow_redirects=False)
    assert oauth_denied.status_code == 200
    assert "Invalid authenticator code" in oauth_denied.text

    oauth_allowed = await client.post(
        "/api/oauth/authorize",
        data={**authorize_payload, "totp_code": totp_code(secret)},
        follow_redirects=False,
    )
    assert oauth_allowed.status_code == 302


@pytest.mark.asyncio
async def test_admin_users_plugin_toggle_system_update_and_restart(integration_env, monkeypatch) -> None:
    client = integration_env["client"]
    cfg = integration_env["settings"]
    services = integration_env["services"]

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

    async def fake_update(command, **kwargs):
        return type("Result", (), {
            "command": command,
            "returncode": 0,
            "stdout": "updated",
            "stderr": "",
            "truncated": False,
            "duration_ms": 10,
            "to_dict": lambda self: {
                "command": self.command,
                "returncode": self.returncode,
                "stdout": self.stdout,
                "stderr": self.stderr,
                "truncated": self.truncated,
                "duration_ms": self.duration_ms,
            },
        })()

    monkeypatch.setattr(services.host_ops, "run", fake_update)
    update = await client.post("/api/system/update", headers=headers)
    assert update.status_code == 200
    assert update.json()["stdout"] == "updated"

    restart = await client.post("/api/system/restart", headers=headers)
    assert restart.status_code == 200
    assert restart.json()["stdout"] == "updated"
