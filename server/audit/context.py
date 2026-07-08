from ipaddress import ip_address
from typing import Any
import uuid

from fastapi import Request

from server.audit.models import AuditActor, AuditContext, AuditSource
from server.core.config import Settings


def client_ip_from_request(request: Request, settings: Settings) -> str | None:
    peer_ip = request.client.host if request.client else None
    forwarded = request.headers.get("x-forwarded-for")
    if not forwarded or not peer_ip:
        return peer_ip
    try:
        trusted = {str(ip_address(item)) for item in settings.app.trusted_proxy_ips}
        if str(ip_address(peer_ip)) not in trusted:
            return peer_ip
    except ValueError:
        return peer_ip
    return forwarded.split(",")[0].strip() or peer_ip


def audit_context_from_request(
    request: Request, settings: Settings | None = None
) -> AuditContext:
    if settings is None:
        state = getattr(getattr(request, "app", None), "state", None)
        settings = getattr(getattr(state, "services", None), "settings", None)

    # Check if a correlation_id is already assigned by middleware
    correlation_id = getattr(request.state, "correlation_id", None)
    if not correlation_id:
        correlation_id = str(uuid.uuid4())

    actor = AuditActor(
        ip=client_ip_from_request(request, settings) if settings else None,
        user_agent=request.headers.get("user-agent"),
        connection_type="Browser",  # Default, could be enriched later
    )

    source = AuditSource(
        module="web.api",
        hostname=request.url.hostname,
    )

    return AuditContext(
        correlation_id=correlation_id,
        actor=actor,
        source=source,
        oauth_client_id=request.scope.get("multiplex.oauth_client_id"),
    )


def ensure_audit_context(ctx: Any) -> AuditContext:
    if isinstance(ctx, AuditContext):
        return ctx
    if isinstance(ctx, dict) or ctx is None:
        d = ctx or {}
        actor = AuditActor(
            ip=d.get("ip"),
            user_agent=d.get("user-agent") or d.get("user_agent"),
            user_id=d.get("user_id"),
            username=d.get("username"),
            ai_assistant_name=d.get("ai_assistant_name"),
            connection_type=d.get("connection_type"),
        )
        source = AuditSource(
            module=d.get("module") or "system",
            hostname=d.get("hostname"),
        )
        return AuditContext(
            correlation_id=d.get("correlation_id") or str(uuid.uuid4()),
            actor=actor,
            source=source,
            parent_event_id=d.get("parent_event_id"),
        )
    return AuditContext(
        correlation_id=str(uuid.uuid4()),
        actor=AuditActor(),
        source=AuditSource(module="system"),
    )
