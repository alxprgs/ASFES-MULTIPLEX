from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status

from server.core.deps import enforce_api_rate_limit, get_current_api_user, get_optional_api_user, get_services
from server.core.ratelimit import RateLimitError
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


@router.post("/login", response_model=AuthTokensResponse)
async def login(payload: LoginRequest, request: Request, services: ApplicationServices = Depends(get_services)) -> AuthTokensResponse:
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
    return _build_auth_response(services, tokens, user_doc)


@router.post("/refresh", response_model=AuthTokensResponse)
async def refresh_tokens(payload: RefreshRequest, request: Request, services: ApplicationServices = Depends(get_services)) -> AuthTokensResponse:
    request_meta = request_meta_from_request(request)
    tokens = await services.auth.refresh_api_tokens(payload.refresh_token, request_meta)
    access_payload = services.auth.verify_api_access_token(tokens.access_token)
    user_doc = await services.users.get_user_by_id(access_payload["sub"])
    if not user_doc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User does not exist")
    return _build_auth_response(services, tokens, user_doc)


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(
    payload: LogoutRequest,
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal | None = Depends(get_optional_api_user),
) -> None:
    await services.auth.revoke_refresh_token(payload.refresh_token)
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
    user_doc = await services.users.create_user(
        username=payload.username,
        password=payload.password,
        email=str(payload.email) if payload.email else None,
        tg_id=payload.tg_id,
        vk_id=payload.vk_id,
        actor=None,
        request_meta=request_meta,
    )
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
