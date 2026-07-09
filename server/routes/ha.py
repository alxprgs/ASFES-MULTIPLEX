"""Home Assistant integration REST API routes.

Endpoint structure:
- Auth endpoints (no HA auth required):
    POST  /api/ha/auth/token          Issue HA access + refresh tokens (or 2FA challenge)
    POST  /api/ha/auth/token/2fa      Complete 2FA challenge, receive tokens
    POST  /api/ha/auth/token/refresh  Exchange refresh → new access + refresh tokens
    DELETE /api/ha/auth/token         Revoke refresh token (logout) — requires HA token

- State / diagnostics (require HA Bearer token):
    GET   /api/ha/state               Single polling endpoint for DataUpdateCoordinator
    GET   /api/ha/diagnostics         Device diagnostics (on-demand, not polled)

- Write endpoints (require HA Bearer + ha.write permission):
    POST  /api/ha/switches/{name}     Toggle enable_registration / enable_mcp / enable_redis

- Button endpoints (require HA Bearer + ha.write or ha.admin):
    POST  /api/ha/buttons/{name}      Execute a button action

- Connection management (require standard API JWT, called from Profile UI):
    GET   /api/ha/connections         List HA connections for current user
    DELETE /api/ha/connections/{jti}  Revoke a specific HA connection
"""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials

from server.core.deps import (
    bearer_scheme,
    get_current_api_user,
    get_current_ha_user,
    get_services,
    require_ha_write,
)
from server.core.security import SecurityError
from server.models import (
    HAChallengeResponse,
    HAConnectionListResponse,
    HADiagnosticsResponse,
    HARevokeRequest,
    HARefreshRequest,
    HAStateResponse,
    HASwitchSetRequest,
    HATokenRequest,
    HATwoFactorRequest,
    HATokenResponse,
    HAButtonResponse,
    UserPrincipal,
)
from server.services import ApplicationServices

router = APIRouter(prefix="/ha", tags=["Home Assistant"])

# Buttons that require ha.admin + destructive_buttons_enabled
_ADMIN_BUTTONS = frozenset({"restart_multiplex", "restart_docker"})
# Buttons that require ha.write
_SAFE_BUTTONS = frozenset({"reload_plugins", "refresh_python_mirror", "refresh_pypi"})
_ALL_BUTTONS = _ADMIN_BUTTONS | _SAFE_BUTTONS

# Allowed switch names
_ALLOWED_SWITCHES = frozenset({"enable_registration", "enable_mcp", "enable_redis"})


# ── Auth Endpoints ──────────────────────────────────────────────────────────


@router.post(
    "/auth/token",
    response_model=HATokenResponse | HAChallengeResponse,
    summary="Authenticate for HA integration",
    response_model_exclude_none=True,
)
async def ha_auth_token(
    request: Request,
    body: HATokenRequest,
    services: ApplicationServices = Depends(get_services),
) -> HATokenResponse | HAChallengeResponse:
    """Issue HA access + refresh tokens, or return 2FA challenge if enabled."""
    if not services.settings.ha.enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="HA integration is disabled",
        )
    client_ip = request.client.host if request.client else None
    try:
        result = await services.ha_service.authenticate(
            username=body.username,
            password=body.password,
            label=body.account_label,
            client_ip=client_ip,
            users_service=services.users,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)
        ) from exc
    return result


@router.post(
    "/auth/token/2fa",
    response_model=HATokenResponse,
    summary="Complete 2FA challenge for HA auth",
)
async def ha_auth_2fa(
    request: Request,
    body: HATwoFactorRequest,
    services: ApplicationServices = Depends(get_services),
) -> HATokenResponse:
    """Complete TOTP challenge and receive HA access + refresh tokens."""
    if not services.settings.ha.enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="HA integration is disabled",
        )
    client_ip = request.client.host if request.client else None
    try:
        result = await services.ha_service.authenticate_2fa(
            challenge_token=body.challenge_token,
            totp_code=body.totp_code,
            client_ip=client_ip,
            users_service=services.users,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)
        ) from exc
    return result


@router.post(
    "/auth/token/refresh",
    response_model=HATokenResponse,
    summary="Refresh HA access token",
)
async def ha_auth_refresh(
    request: Request,
    body: HARefreshRequest,
    services: ApplicationServices = Depends(get_services),
) -> HATokenResponse:
    """Exchange a refresh token for a new access + refresh token pair (token rotation)."""
    client_ip = request.client.host if request.client else None
    try:
        result = await services.ha_service.refresh_access_token(
            refresh_token=body.refresh_token,
            client_ip=client_ip,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)
        ) from exc
    return result


@router.delete(
    "/auth/token",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Revoke HA refresh token (logout)",
)
async def ha_auth_revoke(
    body: HARevokeRequest,
    user: UserPrincipal = Depends(get_current_ha_user),
    services: ApplicationServices = Depends(get_services),
) -> None:
    """Revoke the provided HA refresh token, identified by its JTI."""
    await services.ha_service.revoke_refresh_token(
        jti=body.refresh_token,
        user_id=user.user_id,
    )


# ── State Endpoint ──────────────────────────────────────────────────────────


@router.get(
    "/state",
    response_model=HAStateResponse,
    summary="Get combined HA state (sensors + binary sensors + switches)",
)
async def ha_state(
    user: UserPrincipal = Depends(get_current_ha_user),
    services: ApplicationServices = Depends(get_services),
) -> HAStateResponse:
    """Single polling endpoint for DataUpdateCoordinator.

    Returns all sensor values, binary sensors, switch states, and metadata
    in one HTTP request to minimise polling overhead.
    """
    runtime_settings = await services.settings_service.get_runtime_settings()

    # Determine Redis client (optional)
    redis_client: object | None = None
    if services.settings.redis.mode != "disabled":
        redis_client = getattr(services.cache, "_redis", None)

    return await services.ha_service.get_full_state(
        redis_client=redis_client,
        ha_config=services.settings.ha,
        runtime_settings=runtime_settings,
        mcp_healthy=True,
        python_mirror_running=services.settings.python_mirror.enabled,
        pypi_mirror_running=services.settings.pypi.enabled,
    )


# ── Diagnostics Endpoint (not polled) ──────────────────────────────────────


@router.get(
    "/diagnostics",
    response_model=HADiagnosticsResponse,
    summary="Get device diagnostics (on-demand, not polled)",
)
async def ha_diagnostics(
    user: UserPrincipal = Depends(get_current_ha_user),
    services: ApplicationServices = Depends(get_services),
) -> HADiagnosticsResponse:
    """Device diagnostics — called only when the user opens Diagnostics in HA UI.

    Not included in periodic polling; the HA integration requests this separately.
    """
    return await services.ha_service.get_diagnostics()


# ── Switch Endpoints ────────────────────────────────────────────────────────


@router.post(
    "/switches/{name}",
    response_model=HAButtonResponse,
    summary="Set HA switch state",
)
async def ha_switch_set(
    name: str,
    body: HASwitchSetRequest,
    user: UserPrincipal = Depends(require_ha_write),
    services: ApplicationServices = Depends(get_services),
) -> HAButtonResponse:
    """Toggle enable_registration, enable_mcp, or enable_redis.

    Requires HA__SWITCHES_ENABLED=true and ha.write permission.
    """
    if name not in _ALLOWED_SWITCHES:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown switch '{name}'. Allowed: {sorted(_ALLOWED_SWITCHES)}",
        )
    try:
        await services.ha_service.set_switch(
            name=name,
            value=body.value,
            settings_service=services.settings_service,
            actor=user,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        ) from exc
    return HAButtonResponse(
        success=True, message=f"Switch '{name}' set to {body.value}"
    )


# ── Button Endpoints ────────────────────────────────────────────────────────


async def _resolve_ha_user(
    request: Request,
    services: ApplicationServices,
) -> UserPrincipal:
    """Resolve and validate HA user from Bearer token (for button auth)."""
    credentials: HTTPAuthorizationCredentials | None = await bearer_scheme(request)
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="HA bearer token required",
        )
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

    asyncio.create_task(services.ha_service.touch_access_token(jti))  # noqa: RUF006

    user = await services.users.get_user_by_id(payload["sub"])
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found"
        )
    return services.users.to_principal(user)


@router.post(
    "/buttons/{name}",
    response_model=HAButtonResponse,
    summary="Press an HA button",
)
async def ha_button_press(
    name: str,
    request: Request,
    services: ApplicationServices = Depends(get_services),
) -> HAButtonResponse:
    """Execute a button action.

    Permission model:
    - Safe buttons (reload_plugins, refresh_python_mirror, refresh_pypi):
        require ha.write permission.
    - Admin buttons (restart_multiplex, restart_docker):
        require ha.admin permission AND HA__DESTRUCTIVE_BUTTONS_ENABLED=true.
    """
    if name not in _ALL_BUTTONS:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown button '{name}'. Allowed: {sorted(_ALL_BUTTONS)}",
        )

    # Validate HA token and resolve user
    principal = await _resolve_ha_user(request, services)

    # Check permissions based on button type
    if name in _ADMIN_BUTTONS:
        if not services.settings.ha.destructive_buttons_enabled:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    "Destructive HA operations are disabled "
                    "(HA__DESTRUCTIVE_BUTTONS_ENABLED=false)"
                ),
            )
        if not principal.is_root and "ha.admin" not in principal.permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Permission 'ha.admin' required",
            )
    else:
        # Safe buttons require ha.write
        if not principal.is_root and "ha.write" not in principal.permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Permission 'ha.write' required",
            )

    try:
        result = await services.ha_service.press_button(
            name=name,
            plugin_manager=getattr(services, "plugins", None),
            python_mirror_service=getattr(services, "python_mirror", None),
            pypi_service=getattr(services, "pypi_mirror", None),
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        ) from exc

    return HAButtonResponse(**result)


# ── Connection Management (standard API JWT, Profile UI) ────────────────────


@router.get(
    "/connections",
    response_model=HAConnectionListResponse,
    summary="List HA connections for current user",
)
async def ha_list_connections(
    user: UserPrincipal = Depends(get_current_api_user),
    services: ApplicationServices = Depends(get_services),
) -> HAConnectionListResponse:
    """Return all active HA connections for the authenticated user (for Profile UI).

    Uses standard API JWT, not HA Bearer token.
    """
    connections = await services.ha_service.list_user_connections(user.user_id)
    return HAConnectionListResponse(connections=connections)


@router.delete(
    "/connections/{jti}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Revoke a specific HA connection",
)
async def ha_revoke_connection(
    jti: str,
    user: UserPrincipal = Depends(get_current_api_user),
    services: ApplicationServices = Depends(get_services),
) -> None:
    """Revoke a specific HA refresh token by JTI (from Profile UI).

    Uses standard API JWT, not HA Bearer token.
    """
    await services.ha_service.revoke_refresh_token(
        jti=jti,
        user_id=user.user_id,
    )
