from __future__ import annotations

from typing import Any

from server.models import MCPTool, MCPToolManifest, PermissionDefinition, PluginDefinition, PluginManifest, ToolExecutionContext


async def send_test_email(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    recipient = arguments.get("recipient")
    subject = arguments.get("subject") or "Multiplex test email"
    body = arguments.get("body") or "This is a test email sent from Multiplex."
    if not recipient:
        raise RuntimeError("The 'recipient' argument is required")
    sent = await context.services.mailer.send_email(str(recipient), str(subject), str(body))
    if not sent:
        raise RuntimeError("SMTP delivery is disabled or unavailable")
    return {"recipient": recipient, "subject": subject, "sent": True}


PLUGIN = PluginDefinition(
    manifest=PluginManifest(
        key="mail",
        name="Mail",
        version="1.0.0",
        description="Operational email helpers for SMTP validation and notifications.",
        permissions=[PermissionDefinition(key="mail.send", description="Send test emails through the configured SMTP service.")],
    ),
    tools={
        "mail.send_test_email": MCPTool(
            manifest=MCPToolManifest(
                key="mail.send_test_email",
                name="Send Test Email",
                description="Use this to send a test email through the configured SMTP transport and verify notification delivery.",
                input_schema={
                    "type": "object",
                    "required": ["recipient"],
                    "properties": {
                        "recipient": {"type": "string", "description": "Email address that should receive the test message."},
                        "subject": {"type": "string", "description": "Optional custom subject line."},
                        "body": {"type": "string", "description": "Optional plain-text email body."},
                    },
                    "additionalProperties": False,
                },
                permissions=["mail.send"],
                tags=["mail", "smtp", "write"],
                read_only=False,
                default_global_enabled=False,
            ),
            handler=send_test_email,
        )
    },
)
