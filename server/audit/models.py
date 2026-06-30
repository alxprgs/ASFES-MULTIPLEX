from __future__ import annotations

import datetime
import uuid
from typing import Any, Literal

from pydantic import BaseModel, Field


class AuditActor(BaseModel):
    user_id: str | None = None
    username: str | None = None
    ip: str | None = None
    user_agent: str | None = None
    ai_assistant_name: str | None = None
    connection_type: str | None = None


class AuditSource(BaseModel):
    module: str
    hostname: str | None = None


class BaseAuditEventEnvelope(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    schema_version: int = 1
    timestamp: str = Field(default_factory=lambda: datetime.datetime.now(datetime.UTC).isoformat())
    event_type: str
    correlation_id: str
    parent_event_id: str | None = None
    actor: AuditActor = Field(default_factory=AuditActor)
    source: AuditSource
    target: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    result: str = "success"
    payload: dict[str, Any] = Field(default_factory=dict)
    
    # Internal archiving fields
    archived: bool = False
    archive_file: str | None = None
    archive_hash: str | None = None


class McpCallAuditEvent(BaseAuditEventEnvelope):
    event_type: Literal["mcp.tool.call"] = "mcp.tool.call"
    
    @classmethod
    def create(
        cls, 
        correlation_id: str, 
        source: AuditSource,
        actor: AuditActor,
        tool_key: str, 
        plugin_key: str | None,
        read_only: bool,
        arguments: dict[str, Any],
        oauth_client_id: str | None = None,
        result: str = "success",
        parent_event_id: str | None = None
    ) -> "McpCallAuditEvent":
        return cls(
            correlation_id=correlation_id,
            parent_event_id=parent_event_id,
            source=source,
            actor=actor,
            result=result,
            target={"tool_key": tool_key},
            metadata={
                "arguments": arguments,
                "read_only": read_only,
                "oauth_client_id": oauth_client_id,
                "plugin_key": plugin_key,
            }
        )


class SystemAuditEvent(BaseAuditEventEnvelope):
    @classmethod
    def create(
        cls, 
        event_type: str,
        correlation_id: str, 
        source: AuditSource,
        actor: AuditActor,
        target: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        result: str = "success",
        parent_event_id: str | None = None
    ) -> "SystemAuditEvent":
        return cls(
            event_type=event_type,
            correlation_id=correlation_id,
            parent_event_id=parent_event_id,
            source=source,
            actor=actor,
            result=result,
            target=target or {},
            metadata=metadata or {},
        )


class AuthAuditEvent(BaseAuditEventEnvelope):
    @classmethod
    def create(
        cls, 
        event_type: str,
        correlation_id: str, 
        source: AuditSource,
        actor: AuditActor,
        target: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        result: str = "success",
        parent_event_id: str | None = None
    ) -> "AuthAuditEvent":
        return cls(
            event_type=event_type,
            correlation_id=correlation_id,
            parent_event_id=parent_event_id,
            source=source,
            actor=actor,
            result=result,
            target=target or {},
            metadata=metadata or {},
        )


class AuditContext(BaseModel):
    correlation_id: str
    actor: AuditActor
    source: AuditSource
    parent_event_id: str | None = None
    oauth_client_id: str | None = None

    def child(self, parent_event_id: str) -> "AuditContext":
        return AuditContext(
            correlation_id=self.correlation_id,
            actor=self.actor,
            source=self.source,
            parent_event_id=parent_event_id,
            oauth_client_id=self.oauth_client_id
        )
