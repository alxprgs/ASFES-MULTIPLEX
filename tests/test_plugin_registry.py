from __future__ import annotations

import pytest

from server.models import MCPToolManifest, PluginDefinition, PluginManifest, RuntimeAvailability


@pytest.mark.asyncio
async def test_list_tools_reports_unavailable_metadata(integration_env) -> None:
    services = integration_env["services"]

    async def unavailable(_services):
        return RuntimeAvailability(available=False, reason="Missing backend")

    services.plugins.plugins["docker"].tools["docker.list_containers"].availability = unavailable
    tools = {item["key"]: item for item in await services.plugins.list_tools()}
    assert tools["docker.list_containers"]["available"] is False
    assert tools["docker.list_containers"]["availability_reason"] == "Missing backend"


@pytest.mark.asyncio
async def test_call_tool_redacts_arguments_in_audit(integration_env) -> None:
    services = integration_env["services"]
    cfg = integration_env["settings"]
    tool = services.plugins.plugins["docker"].tools["docker.restart_container"]

    async def available(_services):
        return RuntimeAvailability(available=True)

    async def fake_handler(context, arguments):
        return {"ok": True, "arguments": arguments}

    tool.availability = available
    tool.handler = fake_handler
    tool.manifest.audit_redact_fields = ["container"]
    user_doc = await services.users.get_user_by_username(cfg.root.username)
    assert user_doc is not None
    user = services.users.to_principal(user_doc)

    result = await services.plugins.call_tool(
        user,
        "docker.restart_container",
        {"container": "secret-container"},
        {"ip": "127.0.0.1", "user_agent": "pytest"},
    )
    assert result["ok"] is True

    events = await services.audit.list_events()
    tool_event = next(item for item in events if item["event_type"] == "mcp.tool.call" and item["target"]["tool_key"] == "docker.restart_container")
    assert tool_event["metadata"]["arguments"]["container"] == "[REDACTED]"


@pytest.mark.asyncio
async def test_set_plugin_enabled_records_detailed_audit_metadata(integration_env) -> None:
    services = integration_env["services"]
    cfg = integration_env["settings"]
    user_doc = await services.users.get_user_by_username(cfg.root.username)
    assert user_doc is not None
    user = services.users.to_principal(user_doc)

    await services.plugins.set_plugin_enabled(
        "docker",
        False,
        actor=user,
        request_meta={"ip": "127.0.0.1", "user_agent": "pytest"},
    )

    events = await services.audit.list_events()
    event = next(item for item in events if item["event_type"] == "mcp.plugin.update" and item["target"]["plugin_key"] == "docker")
    assert event["metadata"]["enabled"] is False
    assert event["metadata"]["previous_enabled"] is True
    assert event["metadata"]["changed"] is True
    assert event["metadata"]["plugin_name"] == "Docker"


@pytest.mark.asyncio
async def test_set_global_tool_enabled_records_detailed_audit_metadata(integration_env) -> None:
    services = integration_env["services"]
    cfg = integration_env["settings"]
    user_doc = await services.users.get_user_by_username(cfg.root.username)
    assert user_doc is not None
    user = services.users.to_principal(user_doc)

    await services.plugins.set_global_tool_enabled(
        "docker.list_containers",
        False,
        actor=user,
        request_meta={"ip": "127.0.0.1", "user_agent": "pytest"},
    )

    events = await services.audit.list_events()
    event = next(item for item in events if item["event_type"] == "mcp.tool.global.update" and item["target"]["tool_key"] == "docker.list_containers")
    assert event["metadata"]["enabled"] is False
    assert event["metadata"]["previous_enabled"] is True
    assert event["metadata"]["changed"] is True
    assert event["metadata"]["tool_name"] == "Список Docker-контейнеров"
    assert event["metadata"]["plugin_key"] == "docker"


@pytest.mark.asyncio
async def test_reload_plugins_calls_old_shutdown_before_reload(integration_env, monkeypatch) -> None:
    registry = integration_env["services"].plugins
    order: list[str] = []

    async def fake_shutdown(_services):
        order.append("shutdown")

    async def fake_load(module_name: str, reload_existing: bool = False):
        order.append(f"load:{module_name}:{reload_existing}")

    registry.plugins["dummy"] = PluginDefinition(
        manifest=PluginManifest(key="dummy", name="Dummy", version="1.0.0", description="dummy"),
        tools={},
        shutdown=fake_shutdown,
    )
    monkeypatch.setattr(registry, "_load_plugin_module", fake_load)

    loaded = await registry.reload_plugins(["dummy"])
    assert loaded == ["dummy"]
    assert order == ["shutdown", "load:server.mcp.plugins.dummy:True"]
