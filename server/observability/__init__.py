"""Observability package: Prometheus, Loki, OpenTelemetry."""

from __future__ import annotations

from server.observability.metrics import (
    REGISTRY,
    inc_alert_fire,
    inc_audit_event,
    inc_auth_attempt,
    inc_log_integrity_violation,
    inc_pypi_download,
    inc_rate_limit_hit,
    observe_http_request,
)
from server.observability.service import ObservabilityService

__all__ = [
    "ObservabilityService",
    "REGISTRY",
    "observe_http_request",
    "inc_audit_event",
    "inc_auth_attempt",
    "inc_rate_limit_hit",
    "inc_alert_fire",
    "inc_pypi_download",
    "inc_log_integrity_violation",
]
