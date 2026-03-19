from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status

from server.core.database import TOOL_POLICIES
from server.core.deps import enforce_api_rate_limit, get_current_api_user, get_services, require_permission
from server.models import AuditEventListResponse, AuditEventResponse, PermissionDefinition, PermissionMutationRequest, PluginInfoResponse, PluginReloadRequest, ProfileUpdateRequest, RuntimeSettingsResponse, ToggleRequest, ToolInfoResponse, UserResponse, UserToolPolicyResponse, UserPrincipal
from server.services import ApplicationServices, request_meta_from_request


router = APIRouter(tags=["admin"])


def _runtime_response(services: ApplicationServices, runtime: dict) -> RuntimeSettingsResponse:
    return RuntimeSettingsResponse(
        registration_enabled=bool(runtime.get("registration_enabled", False)),
        mcp_enabled=bool(runtime.get("mcp_enabled", True)),
        redis_runtime_enabled=bool(runtime.get("redis_runtime_enabled", False)),
        redis_mode=services.settings.redis.mode,
    )


@router.put("/account/profile", response_model=UserResponse)
async def update_profile(
    payload: ProfileUpdateRequest,
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(get_current_api_user),
) -> UserResponse:
    await enforce_api_rate_limit(request, services, user=current_user, policy_name="rest_write")
    user_doc = await services.users.update_profile(
        current_user,
        email=str(payload.email) if payload.email else None,
        tg_id=payload.tg_id,
        vk_id=payload.vk_id,
        request_meta=request_meta_from_request(request),
    )
    return UserResponse.model_validate(services.users.to_response(user_doc))


@router.get("/permissions", response_model=list[PermissionDefinition])
async def list_permissions(
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(get_current_api_user),
) -> list[PermissionDefinition]:
    await enforce_api_rate_limit(request, services, user=current_user)
    return services.permissions.list()


@router.put("/users/{user_id}/permissions", response_model=UserResponse)
async def mutate_permissions(
    user_id: str,
    payload: PermissionMutationRequest,
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(require_permission("users.permission.grant")),
) -> UserResponse:
    await enforce_api_rate_limit(request, services, user=current_user, policy_name="rest_write")
    user_doc = await services.users.mutate_permissions(user_id, payload.permissions, payload.mode, actor=current_user, request_meta=request_meta_from_request(request))
    return UserResponse.model_validate(services.users.to_response(user_doc))


@router.get("/settings/registration", response_model=RuntimeSettingsResponse)
async def get_registration_settings(
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(get_current_api_user),
) -> RuntimeSettingsResponse:
    await enforce_api_rate_limit(request, services, user=current_user)
    runtime = await services.settings_service.get_runtime_settings()
    return _runtime_response(services, runtime)


@router.put("/settings/registration", response_model=RuntimeSettingsResponse)
async def set_registration_settings(
    payload: ToggleRequest,
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(require_permission("settings.registration.update")),
) -> RuntimeSettingsResponse:
    await enforce_api_rate_limit(request, services, user=current_user, policy_name="rest_write")
    runtime = await services.settings_service.set_registration(payload.enabled, actor=current_user, request_meta=request_meta_from_request(request))
    return _runtime_response(services, runtime)


@router.get("/settings/mcp", response_model=RuntimeSettingsResponse)
async def get_mcp_settings(
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(get_current_api_user),
) -> RuntimeSettingsResponse:
    await enforce_api_rate_limit(request, services, user=current_user)
    runtime = await services.settings_service.get_runtime_settings()
    return _runtime_response(services, runtime)


@router.put("/settings/mcp", response_model=RuntimeSettingsResponse)
async def set_mcp_settings(
    payload: ToggleRequest,
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(require_permission("mcp.enable")),
) -> RuntimeSettingsResponse:
    await enforce_api_rate_limit(request, services, user=current_user, policy_name="rest_write")
    runtime = await services.settings_service.set_mcp(payload.enabled, actor=current_user, request_meta=request_meta_from_request(request))
    return _runtime_response(services, runtime)


@router.get("/settings/redis", response_model=RuntimeSettingsResponse)
async def get_redis_settings(
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(get_current_api_user),
) -> RuntimeSettingsResponse:
    await enforce_api_rate_limit(request, services, user=current_user)
    runtime = await services.settings_service.get_runtime_settings()
    return _runtime_response(services, runtime)


@router.put("/settings/redis", response_model=RuntimeSettingsResponse)
async def set_redis_settings(
    payload: ToggleRequest,
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(require_permission("settings.redis.update")),
) -> RuntimeSettingsResponse:
    await enforce_api_rate_limit(request, services, user=current_user, policy_name="rest_write")
    try:
        runtime = await services.settings_service.set_redis_runtime(payload.enabled, actor=current_user, request_meta=request_meta_from_request(request))
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return _runtime_response(services, runtime)


@router.get("/audit/logs", response_model=AuditEventListResponse)
async def audit_logs(
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(require_permission("audit.read")),
) -> AuditEventListResponse:
    await enforce_api_rate_limit(request, services, user=current_user)
    items = [AuditEventResponse(
        event_id=item["_id"],
        event_type=item["event_type"],
        actor_user_id=item.get("actor_user_id"),
        actor_username=item.get("actor_username"),
        target=item.get("target", {}),
        result=item.get("result", "success"),
        ip=item.get("ip"),
        user_agent=item.get("user_agent"),
        metadata=item.get("metadata", {}),
        created_at=item["created_at"],
    ) for item in await services.audit.list_events()]
    return AuditEventListResponse(items=items)


@router.get("/mcp/plugins", response_model=list[PluginInfoResponse])
async def list_plugins(
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(require_permission("mcp.plugin.manage")),
) -> list[PluginInfoResponse]:
    await enforce_api_rate_limit(request, services, user=current_user)
    return [PluginInfoResponse.model_validate(item) for item in await services.plugins.list_plugins()]


@router.post("/mcp/plugins/reload")
async def reload_plugins(
    payload: PluginReloadRequest,
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(require_permission("mcp.plugin.manage")),
) -> dict[str, list[str]]:
    await enforce_api_rate_limit(request, services, user=current_user, policy_name="rest_write")
    loaded = await services.plugins.reload_plugins(payload.plugin_keys)
    await services.users.ensure_root_user()
    mcp_gateway = getattr(request.app.state, "mcp_gateway", None)
    if mcp_gateway is not None:
        await mcp_gateway.refresh_tools()
    await services.audit.record(
        "mcp.plugins.reload",
        actor=current_user,
        request_meta=request_meta_from_request(request),
        target={"plugin_keys": loaded},
    )
    return {"reloaded": loaded}


@router.get("/mcp/tools", response_model=list[ToolInfoResponse])
async def list_tools(
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(get_current_api_user),
) -> list[ToolInfoResponse]:
    await enforce_api_rate_limit(request, services, user=current_user)
    return [ToolInfoResponse.model_validate(item) for item in await services.plugins.list_tools()]


@router.get("/mcp/tools/{tool_key}", response_model=ToolInfoResponse)
async def get_tool_info(
    tool_key: str,
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(get_current_api_user),
) -> ToolInfoResponse:
    await enforce_api_rate_limit(request, services, user=current_user)
    tool = next((item for item in await services.plugins.list_tools() if item["key"] == tool_key), None)
    if tool is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tool not found")
    return ToolInfoResponse.model_validate(tool)


@router.put("/mcp/tools/{tool_key}", response_model=ToolInfoResponse)
async def set_tool_global_state(
    tool_key: str,
    payload: ToggleRequest,
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(require_permission("mcp.tool.toggle")),
) -> ToolInfoResponse:
    await enforce_api_rate_limit(request, services, user=current_user, policy_name="rest_write")
    await services.plugins.set_global_tool_enabled(tool_key, payload.enabled, actor=current_user, request_meta=request_meta_from_request(request))
    tool = next((item for item in await services.plugins.list_tools() if item["key"] == tool_key), None)
    if tool is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tool not found")
    return ToolInfoResponse.model_validate(tool)


@router.get("/mcp/users/{user_id}/tools/{tool_key}", response_model=UserToolPolicyResponse)
async def get_user_tool_state(
    user_id: str,
    tool_key: str,
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(get_current_api_user),
) -> UserToolPolicyResponse:
    await enforce_api_rate_limit(request, services, user=current_user)
    target_user_doc = await services.users.get_user_by_id(user_id)
    if not target_user_doc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Target user not found")
    user_policy = await services.db.collection(TOOL_POLICIES).find_one({"tool_key": tool_key, "scope": "user", "subject_id": user_id})
    effective = await services.plugins.is_tool_enabled_for_user(services.users.to_principal(target_user_doc), tool_key)
    return UserToolPolicyResponse(key=tool_key, user_enabled=user_policy.get("enabled") if user_policy else None, effective_enabled=effective)


@router.put("/mcp/users/{user_id}/tools/{tool_key}", response_model=UserToolPolicyResponse)
async def set_user_tool_state(
    user_id: str,
    tool_key: str,
    payload: ToggleRequest,
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(require_permission("mcp.tool.toggle")),
) -> UserToolPolicyResponse:
    await enforce_api_rate_limit(request, services, user=current_user, policy_name="rest_write")
    target_user_doc = await services.users.get_user_by_id(user_id)
    if not target_user_doc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Target user not found")
    await services.plugins.set_user_tool_enabled(user_id, tool_key, payload.enabled, actor=current_user, request_meta=request_meta_from_request(request))
    effective = await services.plugins.is_tool_enabled_for_user(services.users.to_principal(target_user_doc), tool_key)
    return UserToolPolicyResponse(key=tool_key, user_enabled=payload.enabled, effective_enabled=effective)
