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
    two_factor_enabled: bool = False
    created_at: str
    updated_at: str


class HealthResponse(BaseModel):
    status: str


class HealthDetailsResponse(BaseModel):
    status: str
    mongodb: str
    redis: str
    mcp_enabled: bool


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginTwoFactorRequest(BaseModel):
    challenge_token: str
    code: str


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


class PasskeyBeginRegistrationRequest(BaseModel):
    current_password: str
    name: str | None = None


class PasskeyFinishRegistrationRequest(BaseModel):
    challenge_id: str
    name: str | None = None
    credential: dict[str, Any]


class PasskeyBeginAuthenticationRequest(BaseModel):
    username: str | None = None


class PasskeyFinishAuthenticationRequest(BaseModel):
    challenge_id: str
    credential: dict[str, Any]


class PasskeyUpdateRequest(BaseModel):
    name: str


class PasskeyResponse(BaseModel):
    passkey_id: str
    name: str
    created_at: str
    last_used_at: str | None = None
    transports: list[str] = Field(default_factory=list)
    authenticator_attachment: str | None = None
    credential_device_type: str | None = None
    credential_backed_up: bool = False


class PasskeyOptionsResponse(BaseModel):
    challenge_id: str
    options: dict[str, Any]


class AuthTokensResponse(BaseModel):
    two_factor_required: bool = False
    access_token: str
    refresh_token: str
    token_type: str = "Bearer"
    expires_in: int
    user: UserResponse


class TwoFactorChallengeResponse(BaseModel):
    two_factor_required: bool = True
    challenge_token: str
    expires_in: int
    user_id: str
    username: str


class TwoFactorStatusResponse(BaseModel):
    enabled: bool
    pending: bool = False


class TwoFactorSetupRequest(BaseModel):
    current_password: str


class TwoFactorSetupResponse(BaseModel):
    secret: str
    otpauth_uri: str
    qr_svg: str


class TwoFactorEnableRequest(BaseModel):
    code: str


class TwoFactorEnableResponse(BaseModel):
    user: UserResponse
    recovery_codes: list[str]


class TwoFactorDisableRequest(BaseModel):
    current_password: str
    code: str


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


class SystemUpdateResponse(BaseModel):
    command: list[str]
    returncode: int
    stdout: str
    stderr: str
    truncated: bool = False
    duration_ms: int = 0


class SystemUpdateOptions(BaseModel):
    stages: list[str] = Field(default_factory=list)
    force_stages: list[str] = Field(default_factory=list)


class SystemUpdateSessionStartResponse(BaseModel):
    session_id: str


class SystemUpdateStageResponse(BaseModel):
    key: str
    title: str
    status: str = "pending"
    needed: bool = False
    forced: bool = False
    detail: str | None = None
    returncode: int | None = None


class SystemUpdateSessionResponse(BaseModel):
    session_id: str
    kind: str
    status: str
    stages: list[SystemUpdateStageResponse] = Field(default_factory=list)
    result: SystemUpdateResponse | None = None
    error: str | None = None
    requires_restart: bool = False
    created_at: str
    updated_at: str


class BootstrapResponse(BaseModel):
    app_name: str
    app_version: str
    api_prefix: str
    mcp_path: str
    public_base_url: str
    access_cookie_name: str
    refresh_cookie_name: str
    csrf_cookie_name: str
    user: UserResponse | None = None
    runtime: RuntimeSettingsResponse | None = None


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


class OAuthDynamicClientRegistrationRequest(BaseModel):
    client_name: str = "MCP Client"
    redirect_uris: list[str]
    grant_types: list[str] = Field(default_factory=lambda: ["authorization_code", "refresh_token"])
    response_types: list[str] = Field(default_factory=lambda: ["code"])
    token_endpoint_auth_method: str = "none"
    scope: str | None = None


class OAuthClientResponse(BaseModel):
    client_id: str
    name: str
    redirect_uris: list[str]
    allowed_scopes: list[str]
    confidential: bool
    created_at: str
    client_secret: str | None = None


class OAuthClientSecretRotateResponse(BaseModel):
    client_id: str
    client_secret: str


class MCPConnectedServiceUser(BaseModel):
    user_id: str
    username: str | None = None


class MCPConnectedServiceResponse(BaseModel):
    client_id: str
    client_name: str
    confidential: bool
    allowed_scopes: list[str]
    active_session_count: int
    user_count: int
    users: list[MCPConnectedServiceUser]
    last_token_issued_at: str | None = None
    last_tool_call_at: str | None = None


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
    available: bool = True
    availability_reason: str | None = None
    required_backends: list[str] = Field(default_factory=list)
    providers: list[str] = Field(default_factory=list)


class ToolInfoResponse(BaseModel):
    key: str
    plugin_key: str
    name: str
    description: str
    read_only: bool
    permissions: list[str]
    tags: list[str]
    global_enabled: bool
    available: bool = True
    availability_reason: str | None = None
    os_support: list[str] = Field(default_factory=lambda: ["linux", "windows"])
    required_backends: list[str] = Field(default_factory=list)
    providers: list[str] = Field(default_factory=list)


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
AvailabilityHandler = Callable[["ApplicationServices"], Awaitable["RuntimeAvailability"]]


@dataclass(slots=True)
class RuntimeAvailability:
    available: bool
    reason: str | None = None
    required_backends: list[str] = field(default_factory=list)
    providers: list[str] = field(default_factory=list)


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
    os_support: list[str] = field(default_factory=lambda: ["linux", "windows"])
    required_backends: list[str] = field(default_factory=list)
    providers: list[str] = field(default_factory=list)
    audit_redact_fields: list[str] = field(default_factory=list)
    audit_max_string_length: int = 512


@dataclass(slots=True)
class PluginManifest:
    key: str
    name: str
    version: str
    description: str
    os_support: list[str] = field(default_factory=lambda: ["linux", "windows"])
    enabled_by_default: bool = True
    permissions: list[PermissionDefinition] = field(default_factory=list)
    required_backends: list[str] = field(default_factory=list)
    providers: list[str] = field(default_factory=list)


@dataclass(slots=True)
class MCPTool:
    manifest: MCPToolManifest
    handler: ToolHandler
    availability: AvailabilityHandler | None = None


@dataclass(slots=True)
class PluginDefinition:
    manifest: PluginManifest
    tools: dict[str, MCPTool]
    startup: Callable[["ApplicationServices"], Awaitable[None]] | None = None
    shutdown: Callable[["ApplicationServices"], Awaitable[None]] | None = None
    availability: AvailabilityHandler | None = None
