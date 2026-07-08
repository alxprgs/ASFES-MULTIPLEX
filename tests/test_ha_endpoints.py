from __future__ import annotations

from unittest.mock import patch

import pytest
from starlette import status



@pytest.mark.asyncio
async def test_ha_integration_flow(integration_env):
    client = integration_env["client"]
    services = integration_env["services"]

    # 1. Primary Auth (username/password)
    # Default root user password in conftest.py is IntegrationRootPass123!
    auth_data = {
        "username": "root",
        "password": "IntegrationRootPass123!",
        "account_label": "Home Assistant Test Instance",
    }

    # Auth disabled initially check
    with patch.object(services.settings.ha, "enabled", False):
        resp = await client.post("/api/ha/auth/token", json=auth_data)
        assert resp.status_code == status.HTTP_503_SERVICE_UNAVAILABLE

    # Valid Auth
    resp = await client.post("/api/ha/auth/token", json=auth_data)
    assert resp.status_code == status.HTTP_200_OK
    token_resp = resp.json()
    assert "access_token" in token_resp
    assert "refresh_token" in token_resp
    assert token_resp["token_type"] == "Bearer"
    assert token_resp["account_label"] == "Home Assistant Test Instance"

    access_token = token_resp["access_token"]
    refresh_token = token_resp["refresh_token"]

    # Re-verify with invalid credentials
    invalid_auth = dict(auth_data, password="WrongPassword")
    resp = await client.post("/api/ha/auth/token", json=invalid_auth)
    assert resp.status_code == status.HTTP_401_UNAUTHORIZED

    # 2. Access state with HA bearer token
    # Try unauthenticated first
    resp = await client.get("/api/ha/state")
    assert resp.status_code == status.HTTP_401_UNAUTHORIZED

    # Try with valid HA access token
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = await client.get("/api/ha/state", headers=headers)
    assert resp.status_code == status.HTTP_200_OK
    state_resp = resp.json()
    assert "sensors" in state_resp
    assert "binary_sensors" in state_resp
    assert "meta" in state_resp
    assert state_resp["meta"]["destructive_buttons_enabled"] is False

    # Try standard API routes with HA token (should fail because audience/type mismatch)
    resp = await client.get("/api/users", headers=headers)
    assert resp.status_code == status.HTTP_401_UNAUTHORIZED

    # 3. Diagnostics
    resp = await client.get("/api/ha/diagnostics", headers=headers)
    assert resp.status_code == status.HTTP_200_OK
    diag_resp = resp.json()
    assert "serial" in diag_resp
    assert "software_version" in diag_resp
    assert diag_resp["serial"].startswith("multiplex-")

    # 4. Connection Listing (standard user auth)
    # Log in standard user to view/manage connections
    login_resp = await client.post(
        "/api/auth/login",
        json={"username": "root", "password": "IntegrationRootPass123!"},
    )
    assert login_resp.status_code == status.HTTP_200_OK
    user_headers = {"Authorization": f"Bearer {login_resp.json()['access_token']}"}

    resp = await client.get("/api/ha/connections", headers=user_headers)
    assert resp.status_code == status.HTTP_200_OK
    conn_list = resp.json()["connections"]
    assert len(conn_list) >= 1

    # 5. Token Rotation (Refresh)
    refresh_req = {"refresh_token": refresh_token}
    resp = await client.post("/api/ha/auth/token/refresh", json=refresh_req)
    assert resp.status_code == status.HTTP_200_OK
    rotated_resp = resp.json()
    assert "access_token" in rotated_resp
    assert "refresh_token" in rotated_resp
    rotated_resp["access_token"]
    new_refresh_token = rotated_resp["refresh_token"]

    # Verify old access token is still usable (refreshing refresh token does not immediately revoke current access tokens)
    resp = await client.get("/api/ha/state", headers=headers)
    assert resp.status_code == status.HTTP_200_OK

    # Revoke new connection
    resp = await client.get("/api/ha/connections", headers=user_headers)
    assert resp.status_code == status.HTTP_200_OK
    conn_list = resp.json()["connections"]
    assert len(conn_list) == 1
    new_jti = conn_list[0]["jti"]

    resp = await client.delete(f"/api/ha/connections/{new_jti}", headers=user_headers)
    assert resp.status_code == status.HTTP_204_NO_CONTENT

    # Verify revoked connection cannot refresh
    resp = await client.post(
        "/api/ha/auth/token/refresh", json={"refresh_token": new_refresh_token}
    )
    assert resp.status_code == status.HTTP_401_UNAUTHORIZED

    # Verify replay attack triggers revocation
    resp = await client.post("/api/ha/auth/token/refresh", json=refresh_req)
    assert resp.status_code == status.HTTP_401_UNAUTHORIZED


@pytest.mark.asyncio
async def test_ha_switches_and_buttons(integration_env):
    client = integration_env["client"]
    services = integration_env["services"]

    # Create root HA token
    auth_data = {
        "username": "root",
        "password": "IntegrationRootPass123!",
        "account_label": "HA Admin Client",
    }
    resp = await client.post("/api/ha/auth/token", json=auth_data)
    assert resp.status_code == status.HTTP_200_OK
    token = resp.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    # 1. Switch tests
    # Disabled by default
    resp = await client.post(
        "/api/ha/switches/enable_registration",
        json={"value": True},
        headers=headers,
    )
    assert resp.status_code == status.HTTP_403_FORBIDDEN

    # Enable switches config
    with patch.object(services.settings.ha, "switches_enabled", True):
        # Successful toggle
        resp = await client.post(
            "/api/ha/switches/enable_registration",
            json={"value": True},
            headers=headers,
        )
        assert resp.status_code == status.HTTP_200_OK
        assert resp.json()["success"] is True

        # Verify state changes
        flags = await services.settings_service.get_runtime_settings()
        assert flags["registration_enabled"] is True

        # Toggle back
        resp = await client.post(
            "/api/ha/switches/enable_registration",
            json={"value": False},
            headers=headers,
        )
        assert resp.status_code == status.HTTP_200_OK
        flags = await services.settings_service.get_runtime_settings()
        assert flags["registration_enabled"] is False

        # Unknown switch check
        resp = await client.post(
            "/api/ha/switches/unknown_switch",
            json={"value": True},
            headers=headers,
        )
        assert resp.status_code == status.HTTP_404_NOT_FOUND

    # 2. Button tests
    # Safe buttons (reload_plugins, refresh_python_mirror, refresh_pypi)
    resp = await client.post("/api/ha/buttons/reload_plugins", headers=headers)
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["success"] is True

    # Destructive buttons disabled by default
    resp = await client.post("/api/ha/buttons/restart_multiplex", headers=headers)
    assert resp.status_code == status.HTTP_403_FORBIDDEN

    # Enable destructive buttons
    with patch.object(services.settings.ha, "destructive_buttons_enabled", True):
        # We mock Popen/systemctl so it doesn't actually restart host processes during pytest
        with patch("subprocess.Popen") as mock_popen:
            resp = await client.post("/api/ha/buttons/restart_multiplex", headers=headers)
            assert resp.status_code == status.HTTP_200_OK
            assert resp.json()["success"] is True
            mock_popen.assert_called_once()
