from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException, Request

from server.core.deps import (
    get_services,
    get_optional_api_user,
    get_current_api_user,
    get_current_mcp_user,
    require_permission,
    enforce_csrf_for_cookie_auth,
    enforce_api_rate_limit,
)
from server.core.ratelimit import RateLimitError
from server.models import UserPrincipal


def create_mock_request(method="GET", path="/api/test", app_services=None) -> Request:
    request = MagicMock(spec=Request)
    request.method = method
    request.url = MagicMock()
    request.url.path = path
    request.app = MagicMock()
    request.app.state = MagicMock()
    request.app.state.services = app_services
    request.cookies = {}
    request.headers = {}
    request.state = MagicMock()
    return request


def test_get_services() -> None:
    mock_services = MagicMock()
    request = create_mock_request(app_services=mock_services)
    assert get_services(request) == mock_services


@pytest.mark.asyncio
async def test_get_optional_api_user_static_key() -> None:
    mock_services = MagicMock()
    mock_services.api_key_service.verify_token = AsyncMock()
    request = create_mock_request(app_services=mock_services)

    credentials = MagicMock()
    credentials.credentials = "asfes_secret_key"

    # Case 1: invalid token
    mock_services.api_key_service.verify_token.return_value = None
    with pytest.raises(HTTPException) as exc:
        await get_optional_api_user(request, mock_services, credentials)
    assert exc.value.status_code == 401

    # Case 2: valid token
    user = UserPrincipal(user_id="1", username="root", is_root=True, permissions=[])
    mock_services.api_key_service.verify_token.return_value = user
    res = await get_optional_api_user(request, mock_services, credentials)
    assert res == user


@pytest.mark.asyncio
async def test_get_optional_api_user_cookie_and_token() -> None:
    mock_services = MagicMock()
    mock_services.settings.access_cookie_name = "access_token"
    mock_services.settings.csrf_cookie_name = "csrf_token"
    mock_services.users.get_user_by_id = AsyncMock()
    mock_services.auth.verify_api_access_token = AsyncMock()
    request = create_mock_request(app_services=mock_services)

    # Case 1: no token/cookies
    res = await get_optional_api_user(request, mock_services, None)
    assert res is None

    # Case 2: bearer token valid
    credentials = MagicMock()
    credentials.credentials = "bearer_token"
    mock_services.auth.verify_api_access_token.return_value = {"sub": "user_1"}
    mock_user = {"_id": "user_1", "username": "alice", "is_root": False, "permissions": []}
    mock_services.users.get_user_by_id.return_value = mock_user
    mock_services.users.to_principal = lambda u: UserPrincipal(user_id=u["_id"], username=u["username"], is_root=u["is_root"], permissions=u["permissions"])
    res = await get_optional_api_user(request, mock_services, credentials)
    assert res.user_id == "user_1"

    # Case 3: bearer token invalid (raises 401)
    mock_services.auth.verify_api_access_token.side_effect = Exception("Invalid token")
    with pytest.raises(HTTPException) as exc:
        await get_optional_api_user(request, mock_services, credentials)
    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_get_current_api_user() -> None:
    # None raises 401
    with pytest.raises(HTTPException) as exc:
        await get_current_api_user(None)
    assert exc.value.status_code == 401

    # Valid user returns user
    user = UserPrincipal(user_id="1", username="root", is_root=True, permissions=[])
    assert await get_current_api_user(user) == user


@pytest.mark.asyncio
async def test_get_current_mcp_user() -> None:
    mock_services = MagicMock()
    mock_services.api_key_service.verify_token = AsyncMock()
    mock_services.oauth.verify_access_token = MagicMock()
    mock_services.users.get_user_by_id = AsyncMock()

    # Case 1: no credentials
    with pytest.raises(HTTPException) as exc:
        await get_current_mcp_user(mock_services, None)
    assert exc.value.status_code == 401

    # Case 2: static API key
    credentials = MagicMock()
    credentials.credentials = "asfes_key"
    user = UserPrincipal(user_id="1", username="root", is_root=True, permissions=[])
    mock_services.api_key_service.verify_token.return_value = user
    assert await get_current_mcp_user(mock_services, credentials) == user

    # Case 3: oauth token valid
    credentials.credentials = "oauth_token"
    mock_services.oauth.verify_access_token.return_value = {"sub": "user_2"}
    mock_user = {"_id": "user_2", "username": "bob", "is_root": False, "permissions": []}
    mock_services.users.get_user_by_id.return_value = mock_user
    mock_services.users.to_principal = lambda u: UserPrincipal(user_id=u["_id"], username=u["username"], is_root=u["is_root"], permissions=u["permissions"])
    res = await get_current_mcp_user(mock_services, credentials)
    assert res.user_id == "user_2"


@pytest.mark.asyncio
async def test_require_permission() -> None:
    dep = require_permission("admin.write")

    # root user allowed
    root_user = UserPrincipal(user_id="1", username="root", is_root=True, permissions=[])
    assert await dep(root_user) == root_user

    # permitted user allowed
    permitted_user = UserPrincipal(user_id="2", username="bob", is_root=False, permissions=["admin.write"])
    assert await dep(permitted_user) == permitted_user

    # forbidden user raises 403
    regular_user = UserPrincipal(user_id="3", username="alice", is_root=False, permissions=[])
    with pytest.raises(HTTPException) as exc:
        await dep(regular_user)
    assert exc.value.status_code == 403


def test_enforce_csrf_for_cookie_auth() -> None:
    mock_services = MagicMock()
    mock_services.settings.csrf_cookie_name = "csrf_token"

    # GET/HEAD methods skipped
    request = create_mock_request("GET")
    enforce_csrf_for_cookie_auth(request, mock_services)

    # Not auth via cookie skipped
    request = create_mock_request("POST")
    request.state.auth_via_cookie = False
    enforce_csrf_for_cookie_auth(request, mock_services)

    # cookie auth with invalid CSRF token raises 403
    request = create_mock_request("POST")
    request.state.auth_via_cookie = True
    request.cookies = {"csrf_token": "token1"}
    request.headers = {"x-csrf-token": "token2"}
    with pytest.raises(HTTPException) as exc:
        enforce_csrf_for_cookie_auth(request, mock_services)
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_enforce_api_rate_limit() -> None:
    mock_services = MagicMock()
    mock_services.rate_limiter.enforce = AsyncMock()
    request = create_mock_request()

    request.client = MagicMock()
    request.client.host = "127.0.0.1"
    
    # Success case
    await enforce_api_rate_limit(request, mock_services)

    # Rate limit error raises 429
    mock_services.rate_limiter.enforce.side_effect = RateLimitError("policy", 60)
    with pytest.raises(HTTPException) as exc:
        await enforce_api_rate_limit(request, mock_services)
    assert exc.value.status_code == 429
    assert "Retry-After" in exc.value.headers
