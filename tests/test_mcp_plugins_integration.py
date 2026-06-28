from __future__ import annotations

import httpx
import pytest

from fastmcp import Client as FastMCPClient
from fastmcp.client.transports import StreamableHttpTransport


@pytest.mark.asyncio
async def test_mcp_gateway_anonymous(integration_env) -> None:
    app = integration_env["app"]

    def mcp_httpx_client_factory(**kwargs):
        return httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
            **kwargs,
        )

    # 1. Transport without authorization token
    mcp_transport = StreamableHttpTransport(
        "http://testserver/mcp",
        auth=None,
        httpx_client_factory=mcp_httpx_client_factory,
    )

    # Connecting without auth should fail with 401 Unauthorized
    with pytest.raises(Exception) as exc:
        async with FastMCPClient(mcp_transport) as mcp_client:
            await mcp_client.list_tools()
    assert "401" in str(exc.value)


@pytest.mark.asyncio
async def test_mcp_gateway_authorized_flows(integration_env) -> None:
    app = integration_env["app"]
    services = integration_env["services"]
    cfg = integration_env["settings"]

    # Create static API key for root user to authorize MCP client
    root_principal = services.users.to_principal(await services.users.get_user_by_username(cfg.root.username))
    mcp_token, api_key_doc = await services.api_key_service.create_key(
        root_principal,
        name="test_mcp_key",
        expires_in_days=30,
        request_meta={},
    )

    def mcp_httpx_client_factory(**kwargs):
        return httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
            **kwargs,
        )

    mcp_transport = StreamableHttpTransport(
        "http://testserver/mcp",
        auth=mcp_token,
        httpx_client_factory=mcp_httpx_client_factory,
    )

    # Globally enable a tool (e.g. system_stats.get_snapshot) to list it
    await services.plugins.set_global_tool_enabled(
        "system_stats.get_snapshot",
        enabled=True,
        actor=root_principal,
        request_meta={},
    )

    async with integration_env["app"].state.mcp_gateway.lifespan():
        async with FastMCPClient(mcp_transport) as mcp_client:
            # 1. List tools
            tools = await mcp_client.list_tools()
            tool_names = {tool.name for tool in tools}
            assert "system_stats.get_snapshot" in tool_names

            # 2. Call non-existent tool -> raises error
            with pytest.raises(Exception) as exc_nonexistent:
                await mcp_client.call_tool("non_existent.tool", {})
            assert "Unknown tool" in str(exc_nonexistent.value) or "Tool not found" in str(exc_nonexistent.value)

            # 3. Disable the tool globally
            await services.plugins.set_global_tool_enabled(
                "system_stats.get_snapshot",
                enabled=False,
                actor=root_principal,
                request_meta={},
            )

            # 4. Call globally disabled tool -> raises error
            with pytest.raises(Exception) as exc_disabled:
                await mcp_client.call_tool("system_stats.get_snapshot", {})
            assert "access denied" in str(exc_disabled.value).lower()
