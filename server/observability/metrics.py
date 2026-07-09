"""Prometheus metrics registry and helper functions for ASFES Multiplex.

All metrics are registered in a custom CollectorRegistry (not the global default)
to avoid conflicts with tests and other tools. The registry is exposed via
/api/metrics endpoint.
"""

from __future__ import annotations

import re
import time
from typing import TYPE_CHECKING

from server.core.logging import get_logger

if TYPE_CHECKING:
    from server.core.config import ObservabilityConfig

LOGGER = get_logger("multiplex.observability.metrics")

# ---------------------------------------------------------------------------
# Custom registry — avoid global state, safe for tests
# ---------------------------------------------------------------------------

try:
    from prometheus_client import (
        CollectorRegistry,
        Counter,
        Gauge,
        Histogram,
    )
    from prometheus_client import gc_collector, platform_collector, process_collector

    REGISTRY = CollectorRegistry()

    # Standard process / GC collectors attached to our custom registry
    process_collector.ProcessCollector(registry=REGISTRY)
    platform_collector.PlatformCollector(registry=REGISTRY)
    gc_collector.GCCollector(registry=REGISTRY)

    # ── HTTP metrics ──────────────────────────────────────────────────────────
    HTTP_REQUESTS_TOTAL = Counter(
        "multiplex_http_requests",
        "Total number of HTTP requests processed",
        ["method", "path_template", "status_code"],
        registry=REGISTRY,
    )

    HTTP_REQUEST_DURATION = Histogram(
        "multiplex_http_request_duration_seconds",
        "HTTP request duration in seconds",
        ["method", "path_template", "status_code"],
        buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
        registry=REGISTRY,
    )

    HTTP_REQUESTS_IN_FLIGHT = Gauge(
        "multiplex_http_requests_in_flight",
        "Number of HTTP requests currently being processed",
        registry=REGISTRY,
    )

    # ── System metrics (updated by background task) ───────────────────────────
    SYSTEM_CPU_PERCENT = Gauge(
        "multiplex_system_cpu_percent",
        "System CPU usage percent",
        registry=REGISTRY,
    )

    SYSTEM_MEMORY_USED_BYTES = Gauge(
        "multiplex_system_memory_used_bytes",
        "System memory used in bytes",
        registry=REGISTRY,
    )

    SYSTEM_MEMORY_TOTAL_BYTES = Gauge(
        "multiplex_system_memory_total_bytes",
        "Total system memory in bytes",
        registry=REGISTRY,
    )

    SYSTEM_DISK_USED_BYTES = Gauge(
        "multiplex_system_disk_used_bytes",
        "Disk space used in bytes",
        ["mountpoint"],
        registry=REGISTRY,
    )

    SYSTEM_DISK_TOTAL_BYTES = Gauge(
        "multiplex_system_disk_total_bytes",
        "Total disk space in bytes",
        ["mountpoint"],
        registry=REGISTRY,
    )

    PROCESS_UPTIME_SECONDS = Gauge(
        "multiplex_process_uptime_seconds",
        "Process uptime in seconds",
        registry=REGISTRY,
    )

    # ── Business metrics ──────────────────────────────────────────────────────
    AUDIT_EVENTS_TOTAL = Counter(
        "multiplex_audit_events",
        "Total number of audit events recorded",
        ["action"],
        registry=REGISTRY,
    )

    ALERT_RULE_FIRES_TOTAL = Counter(
        "multiplex_alert_rule_fires",
        "Total number of alert rule fires",
        ["rule_id"],
        registry=REGISTRY,
    )

    AUTH_ATTEMPTS_TOTAL = Counter(
        "multiplex_auth_attempts",
        "Total number of authentication attempts",
        ["method", "result"],
        registry=REGISTRY,
    )

    RATE_LIMIT_HITS_TOTAL = Counter(
        "multiplex_rate_limit_hits",
        "Total number of rate limit rejections",
        ["policy_name"],
        registry=REGISTRY,
    )

    PYPI_DOWNLOADS_TOTAL = Counter(
        "multiplex_pypi_downloads",
        "Total number of PyPI package downloads from upstream",
        registry=REGISTRY,
    )

    # ── Infrastructure metrics ────────────────────────────────────────────────
    REDIS_CONNECTED = Gauge(
        "multiplex_redis_connected",
        "1 if Redis is connected and in use, 0 otherwise",
        registry=REGISTRY,
    )

    LOG_INTEGRITY_VIOLATIONS_TOTAL = Counter(
        "multiplex_log_integrity_violations",
        "Total number of log integrity violations detected",
        registry=REGISTRY,
    )

    _PROMETHEUS_AVAILABLE = True

except ImportError:
    # prometheus-client not installed — all helpers become no-ops
    _PROMETHEUS_AVAILABLE = False
    REGISTRY = None  # type: ignore[assignment]
    LOGGER.debug("prometheus-client not installed; Prometheus metrics disabled")

# ---------------------------------------------------------------------------
# Process start time for uptime calculation
# ---------------------------------------------------------------------------
_PROCESS_START_TIME = time.time()

# ---------------------------------------------------------------------------
# Path normalization — keep label cardinality low
# ---------------------------------------------------------------------------

# Segments that look like IDs (UUID, hex, long numeric strings, ObjectId, etc.)
_ID_PATTERN = re.compile(
    r"^[0-9a-f]{8,}(-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})?$",
    re.IGNORECASE,
)

# Pure numeric segments
_NUMERIC_PATTERN = re.compile(r"^\d+$")

# Maximum number of unique normalized paths to track (safety cap)
_MAX_PATH_LABELS = 100
_path_label_count = 0
_path_label_cache: dict[str, str] = {}


def normalize_path(path: str) -> str:
    """Normalize a URL path to avoid high-cardinality Prometheus labels.

    Replaces ID-like segments with ``{id}`` and caps the number of unique
    labels at ``_MAX_PATH_LABELS``.
    """
    global _path_label_count

    cached = _path_label_cache.get(path)
    if cached is not None:
        return cached

    # Safety: too many unique paths → collapse to /other
    if _path_label_count >= _MAX_PATH_LABELS:
        return "/other"

    segments = path.strip("/").split("/")
    normalized: list[str] = []
    for seg in segments:
        if not seg:
            continue
        if _NUMERIC_PATTERN.match(seg) or _ID_PATTERN.match(seg) or len(seg) > 30:
            normalized.append("{id}")
        else:
            normalized.append(seg)

    result = "/" + "/".join(normalized) if normalized else "/"
    _path_label_cache[path] = result
    _path_label_count += 1
    return result


# ---------------------------------------------------------------------------
# Public helper functions (no-op when prometheus-client is not available)
# ---------------------------------------------------------------------------


def init_metrics(config: "ObservabilityConfig") -> bool:
    """Initialize metrics subsystem.

    Returns True if Prometheus is enabled and available, False otherwise.
    """
    if not config.prometheus_enabled:
        return False
    if not _PROMETHEUS_AVAILABLE:
        LOGGER.warning(
            "OBSERVABILITY__PROMETHEUS_ENABLED=true but prometheus-client is not installed. "
            "Run: pip install prometheus-client"
        )
        return False
    LOGGER.info("Prometheus metrics enabled; endpoint: /api/metrics")
    return True


def observe_http_request(
    method: str,
    path: str,
    status: str,
    duration: float,
) -> None:
    """Record HTTP request metrics. No-op if Prometheus is unavailable."""
    if not _PROMETHEUS_AVAILABLE:
        return
    path_label = normalize_path(path)
    try:
        HTTP_REQUESTS_TOTAL.labels(
            method=method,
            path_template=path_label,
            status_code=status,
        ).inc()
        HTTP_REQUEST_DURATION.labels(
            method=method,
            path_template=path_label,
            status_code=status,
        ).observe(duration)
    except Exception:
        pass


def inc_http_in_flight(delta: int = 1) -> None:
    """Increment or decrement the in-flight request gauge."""
    if not _PROMETHEUS_AVAILABLE:
        return
    try:
        HTTP_REQUESTS_IN_FLIGHT.inc(delta)
    except Exception:
        pass


def inc_audit_event(action: str) -> None:
    """Increment the audit events counter."""
    if not _PROMETHEUS_AVAILABLE:
        return
    try:
        # Truncate action to avoid cardinality explosion from unknown actions
        AUDIT_EVENTS_TOTAL.labels(action=action[:64]).inc()
    except Exception:
        pass


def inc_alert_fire(rule_id: str) -> None:
    """Increment the alert rule fires counter."""
    if not _PROMETHEUS_AVAILABLE:
        return
    try:
        ALERT_RULE_FIRES_TOTAL.labels(rule_id=rule_id[:64]).inc()
    except Exception:
        pass


def inc_auth_attempt(method: str, result: str) -> None:
    """Increment the authentication attempts counter.

    Args:
        method: authentication method, e.g. ``password``, ``passkey``, ``api_key``
        result: ``success`` or ``failure``
    """
    if not _PROMETHEUS_AVAILABLE:
        return
    try:
        AUTH_ATTEMPTS_TOTAL.labels(method=method, result=result).inc()
    except Exception:
        pass


def inc_rate_limit_hit(policy_name: str) -> None:
    """Increment the rate limit hits counter."""
    if not _PROMETHEUS_AVAILABLE:
        return
    try:
        RATE_LIMIT_HITS_TOTAL.labels(policy_name=policy_name).inc()
    except Exception:
        pass


def inc_pypi_download() -> None:
    """Increment the PyPI downloads counter."""
    if not _PROMETHEUS_AVAILABLE:
        return
    try:
        PYPI_DOWNLOADS_TOTAL.inc()
    except Exception:
        pass


def set_redis_connected(connected: bool) -> None:
    """Set the Redis connected gauge."""
    if not _PROMETHEUS_AVAILABLE:
        return
    try:
        REDIS_CONNECTED.set(1 if connected else 0)
    except Exception:
        pass


def inc_log_integrity_violation() -> None:
    """Increment the log integrity violations counter."""
    if not _PROMETHEUS_AVAILABLE:
        return
    try:
        LOG_INTEGRITY_VIOLATIONS_TOTAL.inc()
    except Exception:
        pass


def update_system_metrics() -> None:
    """Update all system-level Prometheus gauges using psutil.

    Called periodically by the ObservabilityService background task.
    """
    if not _PROMETHEUS_AVAILABLE:
        return

    try:
        import psutil  # type: ignore[import-untyped]

        SYSTEM_CPU_PERCENT.set(psutil.cpu_percent(interval=None))

        vm = psutil.virtual_memory()
        SYSTEM_MEMORY_USED_BYTES.set(vm.used)
        SYSTEM_MEMORY_TOTAL_BYTES.set(vm.total)

        for part in psutil.disk_partitions(all=False):
            try:
                usage = psutil.disk_usage(part.mountpoint)
                mp = part.mountpoint[:64]  # cap label length
                SYSTEM_DISK_USED_BYTES.labels(mountpoint=mp).set(usage.used)
                SYSTEM_DISK_TOTAL_BYTES.labels(mountpoint=mp).set(usage.total)
            except (PermissionError, OSError):
                pass

        PROCESS_UPTIME_SECONDS.set(time.time() - _PROCESS_START_TIME)

    except Exception as exc:
        LOGGER.debug("Failed to update system metrics: %s", exc)
