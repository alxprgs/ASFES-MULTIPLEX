from __future__ import annotations

from typing import Any

from server.models import MCPTool, MCPToolManifest, PermissionDefinition, PluginDefinition, PluginManifest, ToolExecutionContext


async def alerts_startup(services) -> None:
    await services.alerts.start()


async def alerts_shutdown(services) -> None:
    await services.alerts.stop()


async def list_rules(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    items = await context.services.alerts.list_rules()
    return {"rules": items, "count": len(items)}


async def upsert_rule(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    return await context.services.alerts.upsert_rule(arguments)


async def delete_rule(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    rule_id = str(arguments.get("rule_id") or "")
    if not rule_id:
        raise RuntimeError("The 'rule_id' argument is required")
    return await context.services.alerts.delete_rule(rule_id)


async def list_events(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    limit = max(1, int(arguments.get("limit") or 100))
    items = await context.services.alerts.list_events(limit=limit)
    return {"events": items, "count": len(items)}


async def evaluate_now(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    return await context.services.alerts.evaluate_rules_once()


async def send_test_notification(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    recipients = [str(item) for item in arguments.get("recipients", []) if str(item).strip()]
    if not recipients:
        raise RuntimeError("The 'recipients' argument must contain at least one email address")
    return await context.services.alerts.send_test_notification(
        recipients,
        subject=str(arguments.get("subject") or "Multiplex alert test"),
        body=str(arguments.get("body") or "This is a test notification."),
    )


PLUGIN = PluginDefinition(
    manifest=PluginManifest(
        key="alerts",
        name="Alerts",
        version="1.0.0",
        description="Manage background alert rules and notification delivery for local host signals.",
        permissions=[
            PermissionDefinition(key="alerts.read", description="Read alert rules and alert events."),
            PermissionDefinition(key="alerts.write", description="Create, delete and evaluate alert rules."),
        ],
    ),
    tools={
        "alerts.list_rules": MCPTool(
            manifest=MCPToolManifest(
                key="alerts.list_rules",
                name="List Alert Rules",
                description="List stored alert rules and their recent state.",
                input_schema={"type": "object", "properties": {}, "additionalProperties": False},
                permissions=["alerts.read"],
                tags=["alerts", "read"],
                read_only=True,
            ),
            handler=list_rules,
        ),
        "alerts.upsert_rule": MCPTool(
            manifest=MCPToolManifest(
                key="alerts.upsert_rule",
                name="Upsert Alert Rule",
                description="Create or update an alert rule with source, selector, condition, threshold, cooldown and recipients.",
                input_schema={
                    "type": "object",
                    "required": ["name", "source", "condition"],
                    "properties": {
                        "rule_id": {"type": "string"},
                        "name": {"type": "string"},
                        "source": {"type": "string"},
                        "selector": {"type": "object"},
                        "condition": {"type": "string"},
                        "threshold": {},
                        "window_seconds": {"type": "integer"},
                        "cooldown_seconds": {"type": "integer"},
                        "severity": {"type": "string"},
                        "enabled": {"type": "boolean"},
                        "recipients": {"type": "array", "items": {"type": "string"}},
                    },
                    "additionalProperties": False,
                },
                permissions=["alerts.write"],
                tags=["alerts", "write"],
                read_only=False,
            ),
            handler=upsert_rule,
        ),
        "alerts.delete_rule": MCPTool(
            manifest=MCPToolManifest(
                key="alerts.delete_rule",
                name="Delete Alert Rule",
                description="Delete an alert rule by its rule_id.",
                input_schema={
                    "type": "object",
                    "required": ["rule_id"],
                    "properties": {"rule_id": {"type": "string"}},
                    "additionalProperties": False,
                },
                permissions=["alerts.write"],
                tags=["alerts", "write"],
                read_only=False,
            ),
            handler=delete_rule,
        ),
        "alerts.list_events": MCPTool(
            manifest=MCPToolManifest(
                key="alerts.list_events",
                name="List Alert Events",
                description="Read recent alert events in reverse chronological order.",
                input_schema={
                    "type": "object",
                    "properties": {"limit": {"type": "integer"}},
                    "additionalProperties": False,
                },
                permissions=["alerts.read"],
                tags=["alerts", "read"],
                read_only=True,
            ),
            handler=list_events,
        ),
        "alerts.evaluate_now": MCPTool(
            manifest=MCPToolManifest(
                key="alerts.evaluate_now",
                name="Evaluate Alerts Now",
                description="Run one immediate pass across all enabled alert rules.",
                input_schema={"type": "object", "properties": {}, "additionalProperties": False},
                permissions=["alerts.write"],
                tags=["alerts", "write"],
                read_only=False,
            ),
            handler=evaluate_now,
        ),
        "alerts.send_test_notification": MCPTool(
            manifest=MCPToolManifest(
                key="alerts.send_test_notification",
                name="Send Alert Test Notification",
                description="Send a test notification email through the configured alerting channel.",
                input_schema={
                    "type": "object",
                    "required": ["recipients"],
                    "properties": {
                        "recipients": {"type": "array", "items": {"type": "string"}},
                        "subject": {"type": "string"},
                        "body": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
                permissions=["alerts.write"],
                tags=["alerts", "write"],
                read_only=False,
                audit_redact_fields=["body"],
            ),
            handler=send_test_notification,
        ),
    },
    startup=alerts_startup,
    shutdown=alerts_shutdown,
)
