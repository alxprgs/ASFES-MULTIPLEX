from __future__ import annotations

from contextlib import asynccontextmanager, suppress
from typing import Callable

from fastmcp import FastMCP
from fastmcp.exceptions import NotFoundError, ToolError
from fastmcp.server.auth import AccessToken, TokenVerifier
from fastmcp.server.dependencies import get_http_request
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from fastmcp.tools.tool import Tool, ToolResult
from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser
from mcp.shared.exceptions import McpError
from mcp.types import CallToolRequestParams, ErrorData, ListToolsRequest, ToolAnnotations
from pydantic import AnyHttpUrl, PrivateAttr
from starlette.requests import Request

from server.core.config import Settings
from server.core.ratelimit import RateLimitError
from server.models import MCPTool, UserPrincipal
from server.services import ApplicationServices, request_meta_from_request


ServiceGetter = Callable[[], ApplicationServices]


def _mcp_error(message: str, *, code: int = -32000) -> McpError:
    return McpError(ErrorData(code=code, message=message))


def _build_tool_annotations(tool: MCPTool) -> ToolAnnotations:
    return ToolAnnotations(
        readOnlyHint=tool.manifest.read_only,
        destructiveHint=not tool.manifest.read_only,
        idempotentHint=tool.manifest.read_only,
        openWorldHint=True,
        title=tool.manifest.name,
    )


def _principal_from_access_token(access_token: AccessToken) -> UserPrincipal:
    claims = access_token.claims or {}
    permissions = claims.get("resolved_permissions") or claims.get("permissions") or []
    return UserPrincipal(
        user_id=str(claims.get("sub") or access_token.resource_owner or ""),
        username=str(claims.get("resolved_username") or claims.get("username") or access_token.client_id),
        is_root=bool(claims.get("resolved_is_root", claims.get("is_root", False))),
        permissions=sorted(str(permission) for permission in permissions),
        email=claims.get("resolved_email"),
        tg_id=claims.get("resolved_tg_id"),
        vk_id=claims.get("resolved_vk_id"),
    )


def _resolve_request_user(request: Request) -> UserPrincipal:
    cached = request.scope.get("multiplex.user")
    if isinstance(cached, UserPrincipal):
        return cached

    auth_user = request.scope.get("user")
    if not isinstance(auth_user, AuthenticatedUser):
        raise _mcp_error("Authenticated MCP user is not available", code=-32001)

    access_token = auth_user.access_token
    if not isinstance(access_token, AccessToken):
        raise _mcp_error("Authenticated MCP access token is not available", code=-32001)

    user = _principal_from_access_token(access_token)
    if not user.user_id:
        raise _mcp_error("Authenticated MCP user is missing an identifier", code=-32001)

    request.scope["multiplex.user"] = user
    return user


class MultiplexTokenVerifier(TokenVerifier):
    def __init__(self, settings: Settings, services_getter: ServiceGetter) -> None:
        super().__init__(base_url=settings.public_base_url, required_scopes=["mcp"])
        self._settings = settings
        self._services_getter = services_getter

    def _get_resource_url(self, path: str | None = None) -> AnyHttpUrl | None:
        if self.base_url is None:
            return None
        return AnyHttpUrl(f"{str(self.base_url).rstrip('/')}{self._settings.mcp_path}")

    async def verify_token(self, token: str) -> AccessToken | None:
        try:
            services = self._services_getter()
        except Exception:
            return None

        try:
            payload = services.oauth.verify_access_token(token)
        except Exception:
            return None

        user_id = str(payload.get("sub", ""))
        if not user_id:
            return None

        user_doc = await services.users.get_user_by_id(user_id)
        if not user_doc:
            return None

        principal = services.users.to_principal(user_doc)
        claims = dict(payload)
        claims.update(
            {
                "resolved_username": principal.username,
                "resolved_is_root": principal.is_root,
                "resolved_permissions": principal.permissions,
                "resolved_email": str(principal.email) if principal.email else None,
                "resolved_tg_id": principal.tg_id,
                "resolved_vk_id": principal.vk_id,
            }
        )

        scopes = [str(scope) for scope in payload.get("scopes", ["mcp"]) if scope]
        return AccessToken(
            token=token,
            client_id=str(payload.get("client_id") or "multiplex-client"),
            scopes=scopes,
            expires_at=int(payload["exp"]) if payload.get("exp") is not None else None,
            resource=f"{self._settings.public_base_url}{self._settings.mcp_path}",
            resource_owner=principal.user_id,
            claims=claims,
        )


class MultiplexAccessMiddleware(Middleware):
    def __init__(self, services_getter: ServiceGetter) -> None:
        self._services_getter = services_getter

    async def on_request(
        self,
        context: MiddlewareContext,
        call_next: CallNext,
    ) -> object:
        request = get_http_request()
        _resolve_request_user(request)
        return await call_next(context)

    async def on_list_tools(
        self,
        context: MiddlewareContext[ListToolsRequest],
        call_next: CallNext[ListToolsRequest, list[Tool]],
    ) -> list[Tool]:
        request = get_http_request()
        user = _resolve_request_user(request)
        services = self._services_getter()

        try:
            await services.rate_limiter.enforce("mcp_read", f"{user.user_id}:tools/list")
        except RateLimitError as exc:
            raise _mcp_error(
                f"Rate limit exceeded for {exc.policy_name}. Retry after {exc.retry_after} seconds.",
                code=-32002,
            ) from exc

        tools = list(await call_next(context))
        visible_tools: list[Tool] = []
        for tool in tools:
            if services.plugins.get_tool(tool.key) is None:
                visible_tools.append(tool)
                continue
            if await services.plugins.is_tool_enabled_for_user(user, tool.key):
                visible_tools.append(tool)
        return visible_tools

    async def on_call_tool(
        self,
        context: MiddlewareContext[CallToolRequestParams],
        call_next: CallNext[CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        request = get_http_request()
        _resolve_request_user(request)
        return await call_next(context)


class ManagedPluginTool(Tool):
    _tool_key: str = PrivateAttr()
    _services_getter: ServiceGetter = PrivateAttr()

    def __init__(
        self,
        *,
        plugin_key: str,
        tool: MCPTool,
        services_getter: ServiceGetter,
    ) -> None:
        manifest = tool.manifest
        super().__init__(
            key=manifest.key,
            name=manifest.key,
            title=manifest.name,
            description=manifest.description,
            parameters=manifest.input_schema,
            output_schema={"type": "object", "additionalProperties": True},
            annotations=_build_tool_annotations(tool),
            tags=set(manifest.tags),
            meta={
                "multiplex": {
                    "plugin_key": plugin_key,
                    "permissions": manifest.permissions,
                    "tags": manifest.tags,
                }
            },
            enabled=True,
        )
        self._tool_key = manifest.key
        self._services_getter = services_getter

    async def run(self, arguments: dict[str, object]) -> ToolResult:
        request = get_http_request()
        services = self._services_getter()
        user = _resolve_request_user(request)

        try:
            result = await services.plugins.call_tool(
                user,
                self._tool_key,
                arguments,
                request_meta_from_request(request),
            )
        except Exception as exc:
            raise ToolError(str(exc)) from exc

        if not isinstance(result, dict):
            result = {"result": result}
        return ToolResult(content=result, structured_content=result)


class MultiplexMCPGateway:
    def __init__(self, settings: Settings, services_getter: ServiceGetter) -> None:
        self.settings = settings
        self._services_getter = services_getter
        self.server = FastMCP(
            name=settings.app.name,
            version=settings.app.version,
            instructions=(
                "Multiplex MCP server. Authenticate with OAuth, then use only the tools "
                "enabled for the current account."
            ),
            auth=MultiplexTokenVerifier(settings, services_getter),
            middleware=[MultiplexAccessMiddleware(services_getter)],
            include_fastmcp_meta=True,
        )
        self.http_app = self.server.http_app(path="/", transport="streamable-http")
        self._registered_tool_keys: set[str] = set()

    @asynccontextmanager
    async def lifespan(self):
        async with self.http_app.lifespan(self.http_app):
            yield

    async def refresh_tools(self) -> list[str]:
        services = self._services_getter()
        desired_tools: dict[str, ManagedPluginTool] = {}
        for plugin in services.plugins.plugins.values():
            for tool in plugin.tools.values():
                desired_tools[tool.manifest.key] = ManagedPluginTool(
                    plugin_key=plugin.manifest.key,
                    tool=tool,
                    services_getter=self._services_getter,
                )

        stale_keys = self._registered_tool_keys.difference(desired_tools)
        for tool_key in stale_keys:
            with suppress(NotFoundError):
                self.server.remove_tool(tool_key)

        for tool_key, tool in desired_tools.items():
            if tool_key in self._registered_tool_keys:
                with suppress(NotFoundError):
                    self.server.remove_tool(tool_key)
            self.server.add_tool(tool)

        self._registered_tool_keys = set(desired_tools)
        return sorted(self._registered_tool_keys)


def create_mcp_gateway(settings: Settings, services_getter: ServiceGetter) -> MultiplexMCPGateway:
    return MultiplexMCPGateway(settings, services_getter)
