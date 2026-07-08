from __future__ import annotations

from server.audit import audit_context_from_request
from fastapi import APIRouter, Depends, HTTPException, Request, status

from server.core.deps import enforce_api_rate_limit, get_current_api_user, get_services
from server.models import (
    ApiKeyCreateRequest,
    ApiKeyCreateResponse,
    ApiKeyResponse,
    ApiKeyUpdateRequest,
    UserPrincipal,
)
from server.services import ApplicationServices

router = APIRouter(prefix="/auth/api-keys", tags=["api-keys"])


@router.get("", response_model=list[ApiKeyResponse])
async def list_api_keys(
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(get_current_api_user),
) -> list[ApiKeyResponse]:
    await enforce_api_rate_limit(request, services, user=current_user)
    items = await services.api_key_service.list_keys(current_user)
    return [ApiKeyResponse.model_validate(item) for item in items]


@router.post(
    "", response_model=ApiKeyCreateResponse, status_code=status.HTTP_201_CREATED
)
async def create_api_key(
    payload: ApiKeyCreateRequest,
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(get_current_api_user),
) -> ApiKeyCreateResponse:
    await enforce_api_rate_limit(
        request, services, user=current_user, policy_name="rest_write"
    )
    try:
        token, document = await services.api_key_service.create_key(
            current_user,
            name=payload.name,
            expires_in_days=payload.expires_in_days,
            audit_ctx=audit_context_from_request(request),
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        ) from exc

    response_data = services.api_key_service.to_response(document)
    response_data["token"] = token
    return ApiKeyCreateResponse.model_validate(response_data)


@router.delete("/{key_id}", status_code=status.HTTP_204_NO_CONTENT)
async def revoke_api_key(
    key_id: str,
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(get_current_api_user),
) -> None:
    await enforce_api_rate_limit(
        request, services, user=current_user, policy_name="rest_write"
    )
    try:
        await services.api_key_service.revoke_key(
            current_user, key_id, audit_ctx=audit_context_from_request(request)
        )
    except LookupError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)
        ) from exc


@router.patch("/{key_id}", response_model=ApiKeyResponse)
async def update_api_key(
    key_id: str,
    payload: ApiKeyUpdateRequest,
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(get_current_api_user),
) -> ApiKeyResponse:
    await enforce_api_rate_limit(
        request, services, user=current_user, policy_name="rest_write"
    )
    try:
        updated = await services.api_key_service.update_key(
            current_user,
            key_id,
            name=payload.name,
            expires_in_days=payload.expires_in_days,
            audit_ctx=audit_context_from_request(request),
        )
    except LookupError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)
        ) from exc
    return ApiKeyResponse.model_validate(updated)
