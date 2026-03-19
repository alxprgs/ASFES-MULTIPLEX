from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from pydantic import BaseModel, EmailStr, Field

if TYPE_CHECKING:
    from server.services import ApplicationServices


class PermissionDefinition(BaseModel):
    key: str
    description: str


class UserPrincipal(BaseModel):
    user_id: str
    username: str
    is_root: bool = False
    permissions: list[str] = Field(default_factory=list)
    email: EmailStr | None = None
    tg_id: str | None = None
    vk_id: str | None = None


class UserResponse(BaseModel):
    user_id: str
    username: str
    is_root: bool = False
    permissions: list[str] = Field(default_factory=list)
    email: EmailStr | None = None
    tg_id: str | None = None
    vk_id: str | None = None
    created_at: str
    updated_at: str


class HealthResponse(BaseModel):
    status: str
    mongodb: str
    redis: str
    mcp_enabled: bool


class LoginRequest(BaseModel):
    username: str
    password: str


class RegisterRequest(BaseModel):
    username: str
    password: str
    email: EmailStr | None = None
    tg_id: str | None = None
    vk_id: str | None = None


class RefreshRequest(BaseModel):
    refresh_token: str


class LogoutRequest(BaseModel):
    refresh_token: str


class AuthTokensResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "Bearer"
    expires_in: int
    user: UserResponse


class RegistrationStatusResponse(BaseModel):
    enabled: bool


class ProfileUpdateRequest(BaseModel):
    email: EmailStr | None = None
    tg_id: str | None = None
    vk_id: str | None = None


class PermissionMutationRequest(BaseModel):
    permissions: list[str]
    mode: str = "grant"


class ToggleRequest(BaseModel):
    enabled: bool


class RuntimeSettingsResponse(BaseModel):
    registration_enabled: bool
    mcp_enabled: bool
    redis_runtime_enabled: bool
    redis_mode: str


class AuditEventResponse(BaseModel):
    event_id: str
    event_type: str
    actor_user_id: str | None = None
    actor_username: str | None = None
    target: dict[str, Any] = Field(default_factory=dict)
    result: str
    ip: str | None = None
    user_agent: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: str


class AuditEventListResponse(BaseModel):
    items: list[AuditEventResponse]


class OAuthClientCreateRequest(BaseModel):
    name: str
    redirect_uris: list[str]
    allowed_scopes: list[str] = Field(default_factory=lambda: ["mcp"])
    client_id: str | None = None
    confidential: bool = False


class OAuthClientResponse(BaseModel):
    client_id: str
    name: str
    redirect_uris: list[str]
    allowed_scopes: list[str]
    confidential: bool
    created_at: str
    client_secret: str | None = None


class PluginReloadRequest(BaseModel):
    plugin_keys: list[str] | None = None


class PluginInfoResponse(BaseModel):
    key: str
    name: str
    version: str
    description: str
    enabled: bool
    os_support: list[str]
    tool_keys: list[str]


class ToolInfoResponse(BaseModel):
    key: str
    plugin_key: str
    name: str
    description: str
    read_only: bool
    permissions: list[str]
    tags: list[str]
    global_enabled: bool


class UserToolPolicyResponse(BaseModel):
    key: str
    user_enabled: bool | None = None
    effective_enabled: bool


@dataclass(slots=True)
class ToolExecutionContext:
    user: UserPrincipal
    services: "ApplicationServices"
    request_meta: dict[str, Any]


ToolHandler = Callable[[ToolExecutionContext, dict[str, Any]], Awaitable[Any]]


@dataclass(slots=True)
class MCPToolManifest:
    key: str
    name: str
    description: str
    input_schema: dict[str, Any]
    permissions: list[str]
    tags: list[str] = field(default_factory=list)
    read_only: bool = False
    default_global_enabled: bool = True


@dataclass(slots=True)
class PluginManifest:
    key: str
    name: str
    version: str
    description: str
    os_support: list[str] = field(default_factory=lambda: ["linux", "windows"])
    enabled_by_default: bool = True
    permissions: list[PermissionDefinition] = field(default_factory=list)


@dataclass(slots=True)
class MCPTool:
    manifest: MCPToolManifest
    handler: ToolHandler


@dataclass(slots=True)
class PluginDefinition:
    manifest: PluginManifest
    tools: dict[str, MCPTool]
    startup: Callable[["ApplicationServices"], Awaitable[None]] | None = None
    shutdown: Callable[["ApplicationServices"], Awaitable[None]] | None = None
