from __future__ import annotations

from fastapi import APIRouter, Body, Depends, HTTPException, Request, Response, status

from server.core.deps import enforce_api_rate_limit, enforce_csrf_for_cookie_auth, get_current_api_user, get_optional_api_user, get_services
from server.core.ratelimit import RateLimitError
from server.core.security import random_token
from server.models import AuthTokensResponse, LoginRequest, LogoutRequest, ProfileUpdateRequest, RefreshRequest, RegisterRequest, RegistrationStatusResponse, UserResponse, UserPrincipal
from server.services import ApplicationServices, request_meta_from_request


router = APIRouter(prefix="/auth", tags=["auth"])


def _build_auth_response(services: ApplicationServices, tokens, user_doc: dict) -> AuthTokensResponse:
    return AuthTokensResponse(
        access_token=tokens.access_token,
        refresh_token=tokens.refresh_token,
        token_type=tokens.token_type,
        expires_in=tokens.expires_in,
        user=UserResponse.model_validate(services.users.to_response(user_doc)),
    )


def _set_auth_cookies(response: Response, services: ApplicationServices, tokens) -> str:
    csrf_token = random_token(24)
    same_site = services.settings.security.cookie_samesite
    secure = services.settings.security.cookie_secure
    response.set_cookie(
        services.settings.access_cookie_name,
        tokens.access_token,
        max_age=tokens.expires_in,
        httponly=True,
        secure=secure,
        samesite=same_site,
        path="/",
    )
    response.set_cookie(
        services.settings.refresh_cookie_name,
        tokens.refresh_token,
        max_age=services.settings.security.refresh_token_ttl_days * 24 * 60 * 60,
        httponly=True,
        secure=secure,
        samesite=same_site,
        path="/",
    )
    response.set_cookie(
        services.settings.csrf_cookie_name,
        csrf_token,
        max_age=services.settings.security.refresh_token_ttl_days * 24 * 60 * 60,
        httponly=False,
        secure=secure,
        samesite=same_site,
        path="/",
    )
    return csrf_token


def _clear_auth_cookies(response: Response, services: ApplicationServices) -> None:
    same_site = services.settings.security.cookie_samesite
    secure = services.settings.security.cookie_secure
    for cookie_name in (
        services.settings.access_cookie_name,
        services.settings.refresh_cookie_name,
        services.settings.csrf_cookie_name,
    ):
        response.delete_cookie(cookie_name, path="/", secure=secure, samesite=same_site)


@router.post("/login", response_model=AuthTokensResponse)
async def login(
    payload: LoginRequest,
    request: Request,
    response: Response,
    services: ApplicationServices = Depends(get_services),
) -> AuthTokensResponse:
    request_meta = request_meta_from_request(request)
    try:
        await services.rate_limiter.enforce("login", f"{request_meta['ip']}:{payload.username.lower()}")
    except RateLimitError as exc:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Too many login attempts", headers={"Retry-After": str(exc.retry_after)}) from exc
    user = await services.users.authenticate(payload.username, payload.password)
    if user is None:
        await services.audit.record(
            "auth.login.failed",
            actor=None,
            request_meta=request_meta,
            target={"username": payload.username},
            result="denied",
        )
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password")
    tokens = await services.auth.issue_api_tokens(user, request_meta)
    await services.audit.record("auth.login", actor=user, request_meta=request_meta, target={"user_id": user.user_id})
    user_doc = await services.users.get_user_by_id(user.user_id)
    assert user_doc is not None
    _set_auth_cookies(response, services, tokens)
    return _build_auth_response(services, tokens, user_doc)


@router.post("/refresh", response_model=AuthTokensResponse)
async def refresh_tokens(
    request: Request,
    response: Response,
    payload: RefreshRequest | None = Body(default=None),
    services: ApplicationServices = Depends(get_services),
) -> AuthTokensResponse:
    request_meta = request_meta_from_request(request)
    refresh_token = payload.refresh_token if payload is not None else request.cookies.get(services.settings.refresh_cookie_name)
    if not refresh_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Refresh token is required")
    if payload is None:
        request.state.auth_via_cookie = True
        enforce_csrf_for_cookie_auth(request, services)
    try:
        tokens = await services.auth.refresh_api_tokens(refresh_token, request_meta)
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc
    access_payload = services.auth.verify_api_access_token(tokens.access_token)
    user_doc = await services.users.get_user_by_id(access_payload["sub"])
    if not user_doc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User does not exist")
    _set_auth_cookies(response, services, tokens)
    return _build_auth_response(services, tokens, user_doc)


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(
    request: Request,
    response: Response,
    payload: LogoutRequest | None = Body(default=None),
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal | None = Depends(get_optional_api_user),
) -> None:
    refresh_token = payload.refresh_token if payload is not None else request.cookies.get(services.settings.refresh_cookie_name)
    if refresh_token:
        await services.auth.revoke_refresh_token(refresh_token)
    _clear_auth_cookies(response, services)
    if current_user is not None:
        await services.audit.record("auth.logout", actor=current_user, request_meta=request_meta_from_request(request), target={"user_id": current_user.user_id})


@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def register(payload: RegisterRequest, request: Request, services: ApplicationServices = Depends(get_services)) -> UserResponse:
    request_meta = request_meta_from_request(request)
    runtime = await services.settings_service.get_runtime_settings()
    if not runtime.get("registration_enabled", False):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Registration is disabled")
    try:
        await services.rate_limiter.enforce("register", request_meta["ip"] or "unknown")
    except RateLimitError as exc:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Too many registrations", headers={"Retry-After": str(exc.retry_after)}) from exc
    try:
        user_doc = await services.users.create_user(
            username=payload.username,
            password=payload.password,
            email=str(payload.email) if payload.email else None,
            tg_id=payload.tg_id,
            vk_id=payload.vk_id,
            actor=None,
            request_meta=request_meta,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return UserResponse.model_validate(services.users.to_response(user_doc))


@router.get("/registration-status", response_model=RegistrationStatusResponse)
async def registration_status(services: ApplicationServices = Depends(get_services)) -> RegistrationStatusResponse:
    runtime = await services.settings_service.get_runtime_settings()
    return RegistrationStatusResponse(enabled=bool(runtime.get("registration_enabled", False)))


@router.get("/me", response_model=UserResponse)
async def me(
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(get_current_api_user),
) -> UserResponse:
    await enforce_api_rate_limit(request, services, user=current_user)
    user_doc = await services.users.get_user_by_id(current_user.user_id)
    if not user_doc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return UserResponse.model_validate(services.users.to_response(user_doc))
