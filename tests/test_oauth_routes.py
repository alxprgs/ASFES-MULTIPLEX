from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_oauth_endpoints_metadata_and_jwks(integration_env) -> None:
    client = integration_env["client"]

    # 1. Well-known oauth-authorization-server
    resp1 = await client.get("/.well-known/oauth-authorization-server")
    assert resp1.status_code == 200
    assert "authorization_endpoint" in resp1.json()
    assert "token_endpoint" in resp1.json()

    # 2. JWKS (available under /api prefix since oauth_router is included in api_router)
    resp2 = await client.get("/api/oauth/jwks")
    assert resp2.status_code == 200
    assert "keys" in resp2.json()


@pytest.mark.asyncio
async def test_oauth_client_registration_and_crud(integration_env) -> None:
    client = integration_env["client"]
    cfg = integration_env["settings"]

    # Login as root
    login = await client.post(
        "/api/auth/login",
        json={"username": cfg.root.username, "password": cfg.root.password.get_secret_value()},
    )
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}

    # 1. Register a client dynamically via POST /api/oauth/register
    reg_resp = await client.post(
        "/api/oauth/register",
        headers=headers,
        json={
            "client_name": "Test Client App",
            "redirect_uris": ["http://localhost:3000/callback"],
            "token_endpoint_auth_method": "none", # Public client
            "grant_types": ["authorization_code"],
            "response_types": ["code"],
        }
    )
    assert reg_resp.status_code == 201
    client_id = reg_resp.json()["client_id"]

    # 2. List clients
    list_resp = await client.get("/api/oauth/clients", headers=headers)
    assert list_resp.status_code == 200
    assert any(c["client_id"] == client_id for c in list_resp.json())


@pytest.mark.asyncio
async def test_oauth_authorization_endpoint_failures(integration_env) -> None:
    client = integration_env["client"]
    cfg = integration_env["settings"]

    # Login as root
    login = await client.post(
        "/api/auth/login",
        json={"username": cfg.root.username, "password": cfg.root.password.get_secret_value()},
    )
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}

    # Register a client
    reg_resp = await client.post(
        "/api/oauth/register",
        headers=headers,
        json={
            "client_name": "OAuth Test Client",
            "redirect_uris": ["https://app.example.com/callback"],
            "token_endpoint_auth_method": "none",
            "grant_types": ["authorization_code"],
            "response_types": ["code"],
        }
    )
    client_id = reg_resp.json()["client_id"]

    # Case 1: non-existing client_id -> 404 (LookupError is mapped to 404)
    resp1 = await client.get(
        "/api/oauth/authorize",
        params={
            "client_id": "non_existent_client",
            "redirect_uri": "https://app.example.com/callback",
            "response_type": "code",
            "scope": "mcp",
            "code_challenge": "challenge",
            "code_challenge_method": "S256",
        }
    )
    assert resp1.status_code == 404
    assert "Unknown OAuth client" in resp1.json()["detail"]

    # Case 2: invalid redirect_uri -> 400 (ValueError is mapped to 400)
    resp2 = await client.get(
        "/api/oauth/authorize",
        params={
            "client_id": client_id,
            "redirect_uri": "https://attacker.com/callback",
            "response_type": "code",
            "scope": "mcp",
            "code_challenge": "challenge",
            "code_challenge_method": "S256",
        }
    )
    assert resp2.status_code == 400
    assert "Redirect URI is not registered" in resp2.json()["detail"]

    # Case 3: invalid response_type -> 400
    resp3 = await client.get(
        "/api/oauth/authorize",
        params={
            "client_id": client_id,
            "redirect_uri": "https://app.example.com/callback",
            "response_type": "token",
            "scope": "mcp",
            "code_challenge": "challenge",
            "code_challenge_method": "S256",
        }
    )
    assert resp3.status_code == 400
    assert "Only response_type=code is supported" in resp3.json()["detail"]


@pytest.mark.asyncio
async def test_oauth_token_endpoint_failures(integration_env) -> None:
    client = integration_env["client"]
    cfg = integration_env["settings"]

    # Case 1: unsupported grant_type -> 400
    resp1 = await client.post(
        "/api/oauth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": "some_client",
        }
    )
    assert resp1.status_code == 400
    assert "Unsupported grant_type" in resp1.json()["detail"]

    # Case 2: invalid authorization code exchange -> 400
    resp2 = await client.post(
        "/api/oauth/token",
        data={
            "grant_type": "authorization_code",
            "client_id": "some_client",
            "code": "invalid_code",
            "redirect_uri": "https://app.example.com/callback",
            "code_verifier": "verifier",
        }
    )
    # The client isn't found during validation, so validate_client throws LookupError -> 404
    assert resp2.status_code == 404
