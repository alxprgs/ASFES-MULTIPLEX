from __future__ import annotations

from urllib.parse import urlparse

from fastapi import APIRouter, Body, Depends, HTTPException, Request, Response, status

from server.core.deps import enforce_api_rate_limit, enforce_csrf_for_cookie_auth, get_current_api_user, get_optional_api_user, get_services
from server.core.ratelimit import RateLimitError
from server.core.security import random_token
from server.models import AuthTokensResponse, LoginRequest, LoginTwoFactorRequest, LogoutRequest, PasskeyBeginAuthenticationRequest, PasskeyBeginRegistrationRequest, PasskeyFinishAuthenticationRequest, PasskeyFinishRegistrationRequest, PasskeyOptionsResponse, PasskeyResponse, PasskeyUpdateRequest, RefreshRequest, RegisterRequest, RegistrationStatusResponse, TwoFactorChallengeResponse, TwoFactorDisableRequest, TwoFactorEnableRequest, TwoFactorEnableResponse, TwoFactorSetupRequest, TwoFactorSetupResponse, TwoFactorStatusResponse, UserResponse, UserPrincipal
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


def _webauthn_relying_party(request: Request, services: ApplicationServices) -> tuple[str, str, str]:
    origin = request.headers.get("origin")
    if not origin:
        public = urlparse(services.settings.public_base_url)
        origin = f"{public.scheme}://{public.netloc}"
    parsed = urlparse(origin)
    if parsed.scheme not in {"https", "http"} or not parsed.hostname:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid WebAuthn origin")
    rp_id = parsed.hostname
    return rp_id, services.settings.app.name, f"{parsed.scheme}://{parsed.netloc}"


@router.post("/login", response_model=AuthTokensResponse | TwoFactorChallengeResponse)
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
    user_doc = await services.users.get_user_by_id(user.user_id)
    assert user_doc is not None
    if services.users.two_factor_enabled(user_doc):
        await services.audit.record("auth.login.2fa_required", actor=user, request_meta=request_meta, target={"user_id": user.user_id})
        return TwoFactorChallengeResponse(
            challenge_token=services.auth.issue_2fa_challenge(user),
            expires_in=300,
            user_id=user.user_id,
            username=user.username,
        )
    tokens = await services.auth.issue_api_tokens(user, request_meta)
    await services.audit.record("auth.login", actor=user, request_meta=request_meta, target={"user_id": user.user_id})
    _set_auth_cookies(response, services, tokens)
    return _build_auth_response(services, tokens, user_doc)


@router.post("/login/2fa", response_model=AuthTokensResponse)
async def login_two_factor(
    payload: LoginTwoFactorRequest,
    request: Request,
    response: Response,
    services: ApplicationServices = Depends(get_services),
) -> AuthTokensResponse:
    request_meta = request_meta_from_request(request)
    try:
        challenge = services.auth.verify_2fa_challenge(payload.challenge_token)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Two-factor challenge is invalid or expired") from exc
    try:
        await services.rate_limiter.enforce("login", f"{request_meta['ip']}:{challenge['sub']}:2fa")
    except RateLimitError as exc:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Too many two-factor attempts", headers={"Retry-After": str(exc.retry_after)}) from exc
    user_doc = await services.users.get_user_by_id(challenge["sub"])
    if not user_doc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User does not exist")
    if not await services.users.verify_second_factor(user_doc, payload.code):
        await services.audit.record(
            "auth.login.2fa_failed",
            actor=services.users.to_principal(user_doc),
            request_meta=request_meta,
            target={"user_id": user_doc["_id"]},
            result="denied",
        )
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid two-factor code")
    user = services.users.to_principal(user_doc)
    tokens = await services.auth.issue_api_tokens(user, request_meta)
    await services.audit.record("auth.login", actor=user, request_meta=request_meta, target={"user_id": user.user_id}, metadata={"two_factor": True})
    _set_auth_cookies(response, services, tokens)
    return _build_auth_response(services, tokens, user_doc)


@router.post("/passkeys/authentication/options", response_model=PasskeyOptionsResponse)
async def passkey_authentication_options(
    payload: PasskeyBeginAuthenticationRequest,
    request: Request,
    services: ApplicationServices = Depends(get_services),
) -> PasskeyOptionsResponse:
    request_meta = request_meta_from_request(request)
    try:
        await services.rate_limiter.enforce("login", f"{request_meta['ip']}:{payload.username or 'discoverable'}:passkey")
    except RateLimitError as exc:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Too many passkey attempts", headers={"Retry-After": str(exc.retry_after)}) from exc
    rp_id, rp_name, origin = _webauthn_relying_party(request, services)
    try:
        options = await services.users.begin_passkey_authentication(
            username=payload.username,
            rp_id=rp_id,
            origin=origin,
            request_meta=request_meta,
        )
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc
    return PasskeyOptionsResponse.model_validate(options)


@router.post("/passkeys/authentication/verify", response_model=AuthTokensResponse)
async def passkey_authentication_verify(
    payload: PasskeyFinishAuthenticationRequest,
    request: Request,
    response: Response,
    services: ApplicationServices = Depends(get_services),
) -> AuthTokensResponse:
    request_meta = request_meta_from_request(request)
    try:
        user, user_doc = await services.users.finish_passkey_authentication(
            challenge_id=payload.challenge_id,
            credential=payload.credential,
            request_meta=request_meta,
        )
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc
    tokens = await services.auth.issue_api_tokens(user, request_meta)
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


@router.get("/passkeys", response_model=list[PasskeyResponse])
async def list_passkeys(
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(get_current_api_user),
) -> list[PasskeyResponse]:
    await enforce_api_rate_limit(request, services, user=current_user)
    return [PasskeyResponse.model_validate(item) for item in await services.users.list_passkeys(current_user)]


@router.post("/passkeys/registration/options", response_model=PasskeyOptionsResponse)
async def passkey_registration_options(
    payload: PasskeyBeginRegistrationRequest,
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(get_current_api_user),
) -> PasskeyOptionsResponse:
    await enforce_api_rate_limit(request, services, user=current_user, policy_name="rest_write")
    rp_id, rp_name, origin = _webauthn_relying_party(request, services)
    try:
        options = await services.users.begin_passkey_registration(
            current_user,
            current_password=payload.current_password,
            name=payload.name,
            rp_id=rp_id,
            rp_name=rp_name,
            origin=origin,
            request_meta=request_meta_from_request(request),
        )
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc
    return PasskeyOptionsResponse.model_validate(options)


@router.post("/passkeys/registration/verify", response_model=PasskeyResponse, status_code=status.HTTP_201_CREATED)
async def passkey_registration_verify(
    payload: PasskeyFinishRegistrationRequest,
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(get_current_api_user),
) -> PasskeyResponse:
    await enforce_api_rate_limit(request, services, user=current_user, policy_name="rest_write")
    try:
        passkey = await services.users.finish_passkey_registration(
            current_user,
            challenge_id=payload.challenge_id,
            name=payload.name,
            credential=payload.credential,
            request_meta=request_meta_from_request(request),
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return PasskeyResponse.model_validate(passkey)


@router.put("/passkeys/{passkey_id}", response_model=PasskeyResponse)
async def rename_passkey(
    passkey_id: str,
    payload: PasskeyUpdateRequest,
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(get_current_api_user),
) -> PasskeyResponse:
    await enforce_api_rate_limit(request, services, user=current_user, policy_name="rest_write")
    try:
        passkey = await services.users.rename_passkey(current_user, passkey_id, payload.name, request_meta=request_meta_from_request(request))
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return PasskeyResponse.model_validate(passkey)


@router.delete("/passkeys/{passkey_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_passkey(
    passkey_id: str,
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(get_current_api_user),
) -> None:
    await enforce_api_rate_limit(request, services, user=current_user, policy_name="rest_write")
    try:
        await services.users.delete_passkey(current_user, passkey_id, request_meta=request_meta_from_request(request))
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.get("/2fa/status", response_model=TwoFactorStatusResponse)
async def two_factor_status(
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(get_current_api_user),
) -> TwoFactorStatusResponse:
    await enforce_api_rate_limit(request, services, user=current_user)
    user_doc = await services.users.get_user_by_id(current_user.user_id)
    if not user_doc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    two_factor = user_doc.get("two_factor", {})
    return TwoFactorStatusResponse(enabled=bool(two_factor.get("enabled")), pending=bool(two_factor.get("pending_secret")))


@router.post("/2fa/setup", response_model=TwoFactorSetupResponse)
async def two_factor_setup(
    payload: TwoFactorSetupRequest,
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(get_current_api_user),
) -> TwoFactorSetupResponse:
    await enforce_api_rate_limit(request, services, user=current_user, policy_name="rest_write")
    if not await services.users.verify_password_for_user(current_user, payload.current_password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Current password is invalid")
    setup = await services.users.begin_two_factor_setup(current_user, request_meta=request_meta_from_request(request))
    return TwoFactorSetupResponse.model_validate(setup)


@router.post("/2fa/enable", response_model=TwoFactorEnableResponse)
async def two_factor_enable(
    payload: TwoFactorEnableRequest,
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(get_current_api_user),
) -> TwoFactorEnableResponse:
    await enforce_api_rate_limit(request, services, user=current_user, policy_name="rest_write")
    try:
        user_doc, recovery_codes = await services.users.enable_two_factor(current_user, payload.code, request_meta=request_meta_from_request(request))
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return TwoFactorEnableResponse(user=UserResponse.model_validate(services.users.to_response(user_doc)), recovery_codes=recovery_codes)


@router.post("/2fa/disable", response_model=UserResponse)
async def two_factor_disable(
    payload: TwoFactorDisableRequest,
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(get_current_api_user),
) -> UserResponse:
    await enforce_api_rate_limit(request, services, user=current_user, policy_name="rest_write")
    try:
        user_doc = await services.users.disable_two_factor(
            current_user,
            payload.code,
            current_password=payload.current_password,
            request_meta=request_meta_from_request(request),
        )
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return UserResponse.model_validate(services.users.to_response(user_doc))
