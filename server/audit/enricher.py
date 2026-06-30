from typing import Any

from server.audit.models import BaseAuditEventEnvelope


class AuditEnricher:
    """Pure function layer to enrich events with additional context before saving."""

    def enrich(self, event: BaseAuditEventEnvelope) -> BaseAuditEventEnvelope:
        # Currently, the parsing logic can go here. 
        # For example, interpreting User-Agent to determine AI Assistant names
        if event.actor.user_agent:
            ua = event.actor.user_agent.lower()
            if "claude" in ua:
                event.actor.ai_assistant_name = "Claude"
            elif "cursor" in ua:
                event.actor.ai_assistant_name = "Cursor"
            elif "windsurf" in ua:
                event.actor.ai_assistant_name = "Windsurf"
            elif "rooc" in ua:
                event.actor.ai_assistant_name = "Rooc"
            elif "antigravity" in ua:
                event.actor.ai_assistant_name = "Antigravity"
            
            # Simple connection type fallback if not set
            if not event.actor.connection_type:
                if "mozilla" in ua or "chrome" in ua or "safari" in ua:
                    event.actor.connection_type = "Browser"
                else:
                    event.actor.connection_type = "API / CLI"
        return event

