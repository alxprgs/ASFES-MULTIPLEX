from __future__ import annotations

import pytest

from server.core.security import totp_code


@pytest.mark.asyncio
async def test_auth_routes_failures(integration_env) -> None:
    client = integration_env["client"]
    integration_env["settings"]

    # 1. Registration status (initially registration is disabled by default)
    resp = await client.get("/api/auth/registration-status")
    assert resp.status_code == 200
    assert resp.json()["enabled"] is False

    # 2. Try registering while disabled -> 403
    reg_disabled = await client.post(
        "/api/auth/register",
        json={
            "username": "bob",
            "password": "BobPassword123!",
            "email": "bob@example.com",
        },
    )
    assert reg_disabled.status_code == 403
    assert "Registration is disabled" in reg_disabled.json()["detail"]

    # 3. Login failure -> 401
    bad_login = await client.post(
        "/api/auth/login",
        json={"username": "non_existent", "password": "WrongPassword123!"},
    )
    assert bad_login.status_code == 401
    assert "Invalid username or password" in bad_login.json()["detail"]

    # 4. Refresh token failure with missing token -> 401
    bad_refresh1 = await client.post("/api/auth/refresh")
    assert bad_refresh1.status_code == 401

    # 5. Refresh token failure with invalid token -> 401
    bad_refresh2 = await client.post(
        "/api/auth/refresh", json={"refresh_token": "invalid_refresh_token"}
    )
    assert bad_refresh2.status_code == 401


@pytest.mark.asyncio
async def test_auth_routes_registration_flow(integration_env) -> None:
    client = integration_env["client"]
    cfg = integration_env["settings"]

    # Login as root to enable registration
    root_login = await client.post(
        "/api/auth/login",
        json={
            "username": cfg.root.username,
            "password": cfg.root.password.get_secret_value(),
        },
    )
    assert root_login.status_code == 200
    root_token = root_login.json()["access_token"]
    root_headers = {"Authorization": f"Bearer {root_token}"}

    # Enable registration
    enable_reg = await client.put(
        "/api/settings/registration", headers=root_headers, json={"enabled": True}
    )
    assert enable_reg.status_code == 200
    assert enable_reg.json()["registration_enabled"] is True

    # Register new user successfully
    reg_success = await client.post(
        "/api/auth/register",
        json={
            "username": "bob",
            "password": "BobPassword123!",
            "email": "bob@example.com",
        },
    )
    assert reg_success.status_code == 201
    assert reg_success.json()["username"] == "bob"

    # Register duplicate user -> 409 Conflict
    reg_dup = await client.post(
        "/api/auth/register",
        json={
            "username": "bob",
            "password": "BobPassword123!",
            "email": "bob@example.com",
        },
    )
    assert reg_dup.status_code == 409


@pytest.mark.asyncio
async def test_auth_routes_2fa_management(integration_env) -> None:
    client = integration_env["client"]
    cfg = integration_env["settings"]

    # Login as root
    login = await client.post(
        "/api/auth/login",
        json={
            "username": cfg.root.username,
            "password": cfg.root.password.get_secret_value(),
        },
    )
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}

    # Check initial 2FA status
    status_resp = await client.get("/api/auth/2fa/status", headers=headers)
    assert status_resp.status_code == 200
    assert status_resp.json()["enabled"] is False

    # Setup 2FA - wrong password -> 401
    setup_fail = await client.post(
        "/api/auth/2fa/setup",
        headers=headers,
        json={"current_password": "wrong_password"},
    )
    assert setup_fail.status_code == 401

    # Setup 2FA - correct password -> 200
    setup_success = await client.post(
        "/api/auth/2fa/setup",
        headers=headers,
        json={"current_password": cfg.root.password.get_secret_value()},
    )
    assert setup_success.status_code == 200
    secret = setup_success.json()["secret"]

    # Enable 2FA with wrong code -> 400
    enable_fail = await client.post(
        "/api/auth/2fa/enable", headers=headers, json={"code": "000000"}
    )
    assert enable_fail.status_code == 400

    # Enable 2FA with correct code -> 200
    code = totp_code(secret)
    enable_success = await client.post(
        "/api/auth/2fa/enable", headers=headers, json={"code": code}
    )
    assert enable_success.status_code == 200
    assert enable_success.json()["user"]["two_factor_enabled"] is True

    # Disable 2FA with wrong code -> 400
    disable_fail = await client.post(
        "/api/auth/2fa/disable",
        headers=headers,
        json={
            "code": "000000",
            "current_password": cfg.root.password.get_secret_value(),
        },
    )
    assert disable_fail.status_code == 400

    # Disable 2FA with correct code -> 200
    code2 = totp_code(secret)
    disable_success = await client.post(
        "/api/auth/2fa/disable",
        headers=headers,
        json={"code": code2, "current_password": cfg.root.password.get_secret_value()},
    )
    assert disable_success.status_code == 200
    assert disable_success.json()["two_factor_enabled"] is False
