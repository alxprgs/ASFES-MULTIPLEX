"""Optional OpenTelemetry integration for ASFES Multiplex.

This module is designed to be entirely optional:
- If ``otel_enabled=false`` in config, none of this code runs.
- If ``opentelemetry-sdk`` is not installed, the setup function returns ``None``
  and logs a warning instead of raising an exception.

Supported signals (when enabled):
- **Traces** via OTLP HTTP exporter + BatchSpanProcessor
- **Metrics** via OTLP exporter (optional, usually Prometheus is preferred)
- **Logs**  via OTLP log exporter bridge (optional, usually Loki is preferred)
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from server.core.logging import get_logger

if TYPE_CHECKING:
    from server.core.config import ObservabilityConfig

LOGGER = get_logger("multiplex.observability.otel")


@dataclass
class OTelShutdownHandle:
    """Holds references needed for graceful OpenTelemetry shutdown."""

    _providers: list[Any] = field(default_factory=list)

    async def shutdown(self) -> None:
        """Flush and shut down all OTel providers."""
        for provider in self._providers:
            try:
                await asyncio.to_thread(provider.shutdown)
            except Exception as exc:  # pragma: no cover
                LOGGER.warning("OTel provider shutdown error: %s", exc)


def setup_otel(
    config: "ObservabilityConfig",
    app_version: str,
    app_env: str,
) -> OTelShutdownHandle | None:
    """Initialize OpenTelemetry SDK.

    Returns an ``OTelShutdownHandle`` on success, or ``None`` if OTel is
    disabled or the required packages are not installed.
    """
    if not config.otel_enabled:
        return None

    try:
        from opentelemetry import trace
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError:
        LOGGER.warning(
            "OBSERVABILITY__OTEL_ENABLED=true but opentelemetry-sdk is not installed. "
            "Run: pip install opentelemetry-sdk opentelemetry-exporter-otlp-proto-http"
        )
        return None

    handle = OTelShutdownHandle()

    resource = Resource.create(
        {
            "service.name": "asfes-multiplex",
            "service.version": app_version,
            "deployment.environment": app_env,
        }
    )

    # ── Traces ────────────────────────────────────────────────────────────────
    if config.otel_traces_enabled:
        try:
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
                OTLPSpanExporter,
            )

            tracer_provider = TracerProvider(resource=resource)

            exporter = OTLPSpanExporter(
                endpoint=f"{config.otlp_endpoint.rstrip('/')}/v1/traces",
                headers=config.otlp_headers,
                timeout=int(config.otlp_export_timeout_seconds),
            )
            tracer_provider.add_span_processor(BatchSpanProcessor(exporter))
            trace.set_tracer_provider(tracer_provider)
            handle._providers.append(tracer_provider)
            LOGGER.info("OTel traces enabled → %s", config.otlp_endpoint)

            # Sampling
            if config.otel_sample_rate < 1.0:
                try:
                    from opentelemetry.sdk.trace.sampling import TraceIdRatioBased

                    tracer_provider._active_span_processor  # type: ignore[attr-defined]  # noqa: B018
                    sampler = TraceIdRatioBased(config.otel_sample_rate)
                    # Re-create provider with sampler
                    tracer_provider2 = TracerProvider(
                        resource=resource, sampler=sampler
                    )
                    tracer_provider2.add_span_processor(BatchSpanProcessor(exporter))
                    trace.set_tracer_provider(tracer_provider2)
                    handle._providers.append(tracer_provider2)
                    LOGGER.info(
                        "OTel trace sampling rate: %.2f", config.otel_sample_rate
                    )
                except Exception as exc:
                    LOGGER.warning("Failed to configure OTel sampler: %s", exc)

        except ImportError:
            LOGGER.warning(
                "OTel traces enabled but opentelemetry-exporter-otlp-proto-http "
                "is not installed. Run: pip install opentelemetry-exporter-otlp-proto-http"
            )

        # FastAPI auto-instrumentation (optional, best-effort)
        try:
            from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

            FastAPIInstrumentor.instrument()  # type: ignore[no-untyped-call]
            LOGGER.info("OTel FastAPI auto-instrumentation enabled")
        except ImportError:
            pass  # opentelemetry-instrumentation-fastapi not installed

    # ── Metrics ───────────────────────────────────────────────────────────────
    if config.otel_metrics_enabled:
        try:
            from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
                OTLPMetricExporter,
            )
            from opentelemetry.sdk.metrics import MeterProvider
            from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

            metric_exporter = OTLPMetricExporter(
                endpoint=f"{config.otlp_endpoint.rstrip('/')}/v1/metrics",
                headers=config.otlp_headers,
                timeout=int(config.otlp_export_timeout_seconds),
            )
            reader = PeriodicExportingMetricReader(metric_exporter)
            meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
            from opentelemetry import metrics as otel_metrics

            otel_metrics.set_meter_provider(meter_provider)
            handle._providers.append(meter_provider)
            LOGGER.info("OTel metrics enabled → %s", config.otlp_endpoint)
        except ImportError:
            LOGGER.warning(
                "OTel metrics enabled but OTLP metric exporter is not installed."
            )

    # ── Logs ──────────────────────────────────────────────────────────────────
    if config.otel_logs_enabled:
        try:
            from opentelemetry._logs import set_logger_provider
            from opentelemetry.exporter.otlp.proto.http._log_exporter import (
                OTLPLogExporter,
            )
            from opentelemetry.sdk._logs import LoggerProvider
            from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

            log_exporter = OTLPLogExporter(
                endpoint=f"{config.otlp_endpoint.rstrip('/')}/v1/logs",
                headers=config.otlp_headers,
                timeout=int(config.otlp_export_timeout_seconds),
            )
            log_provider = LoggerProvider(resource=resource)
            log_provider.add_log_record_processor(
                BatchLogRecordProcessor(log_exporter)
            )
            set_logger_provider(log_provider)
            handle._providers.append(log_provider)
            LOGGER.info("OTel logs enabled → %s", config.otlp_endpoint)
        except ImportError:
            LOGGER.warning(
                "OTel logs enabled but OTLP log exporter is not installed."
            )

    return handle
