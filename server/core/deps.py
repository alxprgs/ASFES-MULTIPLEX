from __future__ import annotations

import asyncio
import hmac
from typing import Callable

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from starlette.requests import HTTPConnection

from server.core.ratelimit import RateLimitError
from server.models import UserPrincipal
from server.services import ApplicationServices


bearer_scheme = HTTPBearer(auto_error=False)


def get_services(connection: HTTPConnection) -> ApplicationServices:
    return connection.app.state.services


async def get_optional_api_user(
    request: Request,
    services: ApplicationServices = Depends(get_services),
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> UserPrincipal | None:
    token = (
        credentials.credentials
        if credentials is not None
        else request.cookies.get(services.settings.access_cookie_name)
    )
    using_cookie = credentials is None and bool(token)

    # Static API key authentication
    if token and token.startswith("asfes_"):
        request.state.auth_via_api_key = True
        request.state.auth_via_cookie = False
        user = await services.api_key_service.verify_token(token)
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or revoked API key",
            )
        return user

    request.state.auth_via_cookie = using_cookie
    if token is None:
        return None
    enforce_csrf_for_cookie_auth(request, services)
    try:
        payload = await services.auth.verify_api_access_token(token)
        request.state.access_token_jti = payload.get("jti")
        request.state.access_token_exp = payload.get("exp")
    except Exception as exc:
        if using_cookie:
            return None
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API token"
        ) from exc
    user = await services.users.get_user_by_id(payload["sub"])
    if not user:
        if using_cookie:
            return None
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="User does not exist"
        )
    return services.users.to_principal(user)


async def get_current_api_user(
    user: UserPrincipal | None = Depends(get_optional_api_user),
) -> UserPrincipal:
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required"
        )
    return user


async def get_current_mcp_user(
    services: ApplicationServices = Depends(get_services),
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> UserPrincipal:
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="OAuth bearer token required",
        )

    # Static API key support for MCP
    if credentials.credentials.startswith("asfes_"):
        user = await services.api_key_service.verify_token(credentials.credentials)
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or revoked API key",
            )
        return user

    try:
        payload = services.oauth.verify_access_token(credentials.credentials)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid OAuth bearer token",
        ) from exc
    user = await services.users.get_user_by_id(payload["sub"])
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="User does not exist"
        )
    return services.users.to_principal(user)


def require_permission(permission: str) -> Callable[[UserPrincipal], UserPrincipal]:
    async def dependency(
        user: UserPrincipal = Depends(get_current_api_user),
    ) -> UserPrincipal:
        if not user.is_root and permission not in user.permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission '{permission}' is required",
            )
        return user

    return dependency


def enforce_csrf_for_cookie_auth(
    request: Request, services: ApplicationServices
) -> None:
    if request.method in {"GET", "HEAD", "OPTIONS"}:
        return
    if not getattr(request.state, "auth_via_cookie", False):
        return

    cookie_token = request.cookies.get(services.settings.csrf_cookie_name)
    header_token = request.headers.get("x-csrf-token")
    if (
        not cookie_token
        or not header_token
        or not hmac.compare_digest(cookie_token, header_token)
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Invalid CSRF token"
        )


async def enforce_api_rate_limit(
    request: Request,
    services: ApplicationServices,
    *,
    user: UserPrincipal | None = None,
    policy_name: str | None = None,
    suffix: str | None = None,
) -> None:
    ip = request.client.host if request.client else None
    identifier = user.user_id if user else ip or "anonymous"
    key = f"{identifier}:{suffix or request.url.path}"
    try:
        await services.rate_limiter.enforce(
            policy_name
            or ("rest_read" if request.method in {"GET", "HEAD"} else "rest_write"),
            key,
        )
    except RateLimitError as exc:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limit exceeded for {exc.policy_name}",
            headers={"Retry-After": str(exc.retry_after)},
        ) from exc


# ── Home Assistant Integration Dependencies ────────────────────────────────


async def get_current_ha_user(
    request: Request,
    services: ApplicationServices = Depends(get_services),
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> UserPrincipal:
    """Validate a HA-specific Bearer token.

    Accepts ONLY tokens with:
    - audience="home-assistant"
    - token_type="ha_access"
    - signed with HA__JWT_SECRET

    Rejects: standard API JWT, OAuth tokens, cookie auth, API keys.
    This ensures HA tokens cannot be used on regular API endpoints and vice versa.
    """
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="HA bearer token required",
        )

    from server.core.security import SecurityError  # noqa: PLC0415

    try:
        payload = services.ha_service.verify_ha_access_token(credentials.credentials)
    except SecurityError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired HA token",
        ) from exc

    jti = payload.get("jti", "")
    if await services.ha_service.is_access_token_revoked(jti):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="HA token has been revoked",
        )

    # Fire-and-forget: update last_used_at without blocking the request
    asyncio.create_task(services.ha_service.touch_access_token(jti))  # noqa: RUF006

    user = await services.users.get_user_by_id(payload["sub"])
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User does not exist",
        )

    request.state.ha_token_jti = jti
    return services.users.to_principal(user)


async def require_ha_write(
    request: Request,
    user: UserPrincipal = Depends(get_current_ha_user),
    services: ApplicationServices = Depends(get_services),
) -> UserPrincipal:
    """Require ha.write permission and HA__SWITCHES_ENABLED=true."""
    if not services.settings.ha.switches_enabled:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="HA write operations are disabled (HA__SWITCHES_ENABLED=false)",
        )
    if not user.is_root and "ha.write" not in user.permissions:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Permission 'ha.write' required",
        )
    return user


async def require_ha_admin(
    user: UserPrincipal = Depends(get_current_ha_user),
    services: ApplicationServices = Depends(get_services),
) -> UserPrincipal:
    """Require ha.admin permission and HA__DESTRUCTIVE_BUTTONS_ENABLED=true."""
    if not services.settings.ha.destructive_buttons_enabled:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Destructive HA operations are disabled",
        )
    if not user.is_root and "ha.admin" not in user.permissions:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Permission 'ha.admin' required",
        )
    return user
