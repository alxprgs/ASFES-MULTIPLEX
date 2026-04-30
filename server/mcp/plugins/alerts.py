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
        name="Оповещения",
        version="1.0.0",
        description="Управляет фоновыми правилами оповещений и доставкой уведомлений по сигналам локального хоста.",
        permissions=[
            PermissionDefinition(key="alerts.read", description="Читать правила оповещений и события оповещений."),
            PermissionDefinition(key="alerts.write", description="Создавать, удалять и проверять правила оповещений."),
        ],
    ),
    tools={
        "alerts.list_rules": MCPTool(
            manifest=MCPToolManifest(
                key="alerts.list_rules",
                name="Список правил оповещений",
                description="Показывает сохранённые правила оповещений и их последнее состояние.",
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
                name="Создать или обновить правило оповещения",
                description="Создаёт или обновляет правило оповещения с источником, селектором, условием, порогом, паузой и получателями.",
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
                name="Удалить правило оповещения",
                description="Удаляет правило оповещения по rule_id.",
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
                name="Список событий оповещений",
                description="Показывает последние события оповещений в обратном хронологическом порядке.",
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
                name="Проверить оповещения сейчас",
                description="Запускает немедленную проверку всех включённых правил оповещений.",
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
                name="Отправить тестовое уведомление",
                description="Отправляет тестовое письмо через настроенный канал оповещений.",
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
