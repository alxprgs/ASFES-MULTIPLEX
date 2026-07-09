"""ObservabilityService — coordinator for Prometheus, Loki, and OTel subsystems.

Responsible for:
- Starting/stopping the Loki push loop
- Running the background system metrics collection task
- Initiating OTel shutdown on application exit
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from server.core.logging import get_logger

if TYPE_CHECKING:
    from server.core.config import ObservabilityConfig
    from server.observability.loki import LokiHandler
    from server.observability.otel import OTelShutdownHandle

LOGGER = get_logger("multiplex.observability.service")


@dataclass(slots=True)
class ObservabilityService:
    """Central coordinator for all observability subsystems.

    Created during ``build_application_services()`` and stored on
    ``ApplicationServices.observability``.
    """

    config: "ObservabilityConfig"
    prometheus_enabled: bool
    loki_handler: "LokiHandler | None"
    otel_handle: "OTelShutdownHandle | None"

    _system_metrics_task: asyncio.Task[Any] | None = field(default=None, repr=False)

    async def start(self) -> None:
        """Start all background observability tasks.

        Called from the FastAPI ``lifespan`` context manager after the event
        loop is running.
        """
        if self.prometheus_enabled:
            self._system_metrics_task = asyncio.create_task(
                self._system_metrics_loop(),
                name="observability-system-metrics",
            )
            LOGGER.debug(
                "System metrics collection started "
                "(interval=%ds)",
                self.config.system_metrics_interval_seconds,
            )

        if self.loki_handler is not None:
            await self.loki_handler.start()
            LOGGER.debug("Loki push loop started → %s", self.config.loki_url)

    async def stop(self) -> None:
        """Gracefully shut down all observability subsystems.

        Called from ``shutdown_application_services()``.
        """
        if self._system_metrics_task is not None:
            self._system_metrics_task.cancel()
            with asyncio.timeout(5.0):
                try:
                    await self._system_metrics_task
                except (asyncio.CancelledError, Exception):
                    pass

        if self.loki_handler is not None:
            LOGGER.debug("Flushing Loki buffer before shutdown…")
            await self.loki_handler.stop()

        if self.otel_handle is not None:
            await self.otel_handle.shutdown()

    # ── Private helpers ───────────────────────────────────────────────────────

    async def _system_metrics_loop(self) -> None:
        """Periodically collect system metrics via psutil and update Prometheus."""
        from server.observability.metrics import update_system_metrics  # lazy

        # Initial update immediately on start
        try:
            update_system_metrics()
        except Exception as exc:
            LOGGER.debug("Initial system metrics update failed: %s", exc)

        while True:
            await asyncio.sleep(self.config.system_metrics_interval_seconds)
            try:
                update_system_metrics()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.debug("System metrics update failed: %s", exc)
