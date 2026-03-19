from __future__ import annotations

from typing import Callable

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from server.core.ratelimit import RateLimitError
from server.models import UserPrincipal
from server.services import ApplicationServices, request_meta_from_request


bearer_scheme = HTTPBearer(auto_error=False)


def get_services(request: Request) -> ApplicationServices:
    return request.app.state.services


async def get_optional_api_user(
    services: ApplicationServices = Depends(get_services),
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> UserPrincipal | None:
    if credentials is None:
        return None
    try:
        payload = services.auth.verify_api_access_token(credentials.credentials)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API bearer token") from exc
    user = await services.users.get_user_by_id(payload["sub"])
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User does not exist")
    return services.users.to_principal(user)


async def get_current_api_user(user: UserPrincipal | None = Depends(get_optional_api_user)) -> UserPrincipal:
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required")
    return user


async def get_current_mcp_user(
    services: ApplicationServices = Depends(get_services),
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> UserPrincipal:
    if credentials is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="OAuth bearer token required")
    try:
        payload = services.oauth.verify_access_token(credentials.credentials)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid OAuth bearer token") from exc
    user = await services.users.get_user_by_id(payload["sub"])
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User does not exist")
    return services.users.to_principal(user)


def require_permission(permission: str) -> Callable[[UserPrincipal], UserPrincipal]:
    async def dependency(user: UserPrincipal = Depends(get_current_api_user)) -> UserPrincipal:
        if not user.is_root and permission not in user.permissions:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Permission '{permission}' is required")
        return user

    return dependency


async def enforce_api_rate_limit(
    request: Request,
    services: ApplicationServices,
    *,
    user: UserPrincipal | None = None,
    policy_name: str | None = None,
    suffix: str | None = None,
) -> None:
    request_meta = request_meta_from_request(request)
    identifier = user.user_id if user else request_meta["ip"] or "anonymous"
    key = f"{identifier}:{suffix or request.url.path}"
    try:
        await services.rate_limiter.enforce(policy_name or ("rest_read" if request.method in {"GET", "HEAD"} else "rest_write"), key)
    except RateLimitError as exc:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limit exceeded for {exc.policy_name}",
            headers={"Retry-After": str(exc.retry_after)},
        ) from exc
