"""Loki log forwarding for ASFES Multiplex.

Implements a thread-safe, non-blocking ``logging.Handler`` that batches log
records and pushes them asynchronously to Grafana Loki via the HTTP Push API.

Key design decisions:
- ``asyncio.Queue`` decouples the synchronous ``Handler.emit()`` from the
  async push coroutine; avoids blocking the calling thread.
- Records are batched (by count or flush interval) to reduce HTTP overhead.
- When Loki is unavailable, batches are retried with exponential back-off;
  after ``max_retries`` failures the batch is discarded and a warning is
  logged to stderr (not via the logging system, to avoid recursion).
- When the buffer queue is full, new records are silently dropped — this is
  preferable to blocking the application or raising exceptions.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from server.core.config import ObservabilityConfig

# Minimal log-name prefix to group by (e.g. "multiplex.services" → "multiplex")
_LOGGER_GROUP_DEPTH = 2

# Log level names that map to Loki "level" label values (kept low-cardinality)
_LEVEL_NAMES: frozenset[str] = frozenset(
    {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
)


def _logger_group(name: str) -> str:
    """Return the first ``_LOGGER_GROUP_DEPTH`` segments of a logger name."""
    parts = name.split(".", _LOGGER_GROUP_DEPTH)
    return ".".join(parts[:_LOGGER_GROUP_DEPTH])


def _record_to_loki_entry(
    record: logging.LogRecord,
    extra_labels: dict[str, str],
    env: str,
) -> dict[str, Any]:
    """Convert a ``LogRecord`` to a Loki stream entry dict.

    Returns:
        {
            "stream": {"app": ..., "env": ..., "level": ..., "logger": ...},
            "values": [["<nanoseconds>", "<json-line>"]]
        }
    """
    # Timestamp in nanoseconds as string (Loki requirement)
    ts_ns = str(int(record.created * 1e9))

    level = record.levelname if record.levelname in _LEVEL_NAMES else "INFO"

    stream_labels: dict[str, str] = {
        "app": "multiplex",
        "env": env,
        "level": level,
        "logger": _logger_group(record.name),
        **extra_labels,
    }

    # Build the log line JSON (mirrors what IntegrityLogHandler already writes)
    from datetime import datetime, UTC
    dt = datetime.fromtimestamp(record.created, UTC)

    payload = getattr(record, "payload", {}) or {}
    event_type = getattr(record, "event_type", record.levelname.lower())
    line_data: dict[str, Any] = {
        "timestamp": dt.isoformat(),
        "level": level,
        "logger": record.name,
        "event_type": event_type,
        "message": record.getMessage(),
    }
    if payload:
        line_data["payload"] = payload
    if record.exc_info:
        line_data["exception"] = logging.Formatter().formatException(record.exc_info)

    # Structured metadata as third element (Loki ≥ 3.0 feature)
    correlation_id = getattr(record, "correlation_id", None)
    structured_metadata: dict[str, str] | None = None
    if correlation_id:
        structured_metadata = {"correlation_id": str(correlation_id)}

    line_json = json.dumps(line_data, ensure_ascii=False, separators=(",", ":"))

    value: list[Any] = [ts_ns, line_json]
    if structured_metadata:
        value.append(structured_metadata)

    return {"stream": stream_labels, "values": [value]}


def build_loki_payload(
    entries: list[dict[str, Any]],
) -> bytes:
    """Merge stream entries with identical labels into single Loki streams.

    Loki expects one stream per unique label set; merging reduces HTTP overhead.
    """
    streams: dict[str, dict[str, Any]] = {}
    for entry in entries:
        # Use frozen label string as key for deduplication
        key = json.dumps(entry["stream"], sort_keys=True)
        if key in streams:
            streams[key]["values"].extend(entry["values"])
        else:
            streams[key] = {"stream": entry["stream"], "values": list(entry["values"])}

    payload = {"streams": list(streams.values())}
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


class LokiHandler(logging.Handler):
    """Asynchronous, buffered logging handler that pushes records to Loki.

    Usage:
        handler = LokiHandler(config)
        logging.getLogger().addHandler(handler)
        # In async context:
        await handler.start()
        # On shutdown:
        await handler.stop()
    """

    def __init__(self, config: "ObservabilityConfig") -> None:
        super().__init__()
        self._config = config
        self._env = "development"  # overridden by set_env()
        self._queue: asyncio.Queue[logging.LogRecord | None] = asyncio.Queue(
            maxsize=config.loki_buffer_max_size
        )
        self._pusher_task: asyncio.Task[Any] | None = None
        self._dropped_count = 0

        # Set a filter for minimum level
        min_level = getattr(logging, config.loki_min_level, logging.INFO)
        self.setLevel(min_level)

    def set_env(self, env: str) -> None:
        """Set the deployment environment label (e.g. 'production', 'development')."""
        self._env = env

    # ── logging.Handler interface ─────────────────────────────────────────────

    def emit(self, record: logging.LogRecord) -> None:
        """Enqueue a log record for async push. Never raises; never blocks."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Thread-safe enqueue; drops silently if queue is full
                try:
                    loop.call_soon_threadsafe(self._queue.put_nowait, record)
                except asyncio.QueueFull:
                    self._dropped_count += 1
        except RuntimeError:
            # No event loop available (e.g. during early startup)
            pass
        except Exception:
            pass

    # ── Async lifecycle ───────────────────────────────────────────────────────

    async def start(self) -> None:
        """Start the background push loop. Call once from async context."""
        if self._pusher_task is not None and not self._pusher_task.done():
            return
        self._pusher_task = asyncio.create_task(self._push_loop(), name="loki-pusher")

    async def stop(self) -> None:
        """Flush remaining records and shut down the push loop."""
        # Signal the push loop to finish
        try:
            self._queue.put_nowait(None)  # sentinel
        except asyncio.QueueFull:
            pass

        if self._pusher_task is not None:
            try:
                await asyncio.wait_for(self._pusher_task, timeout=10.0)
            except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                self._pusher_task.cancel()

        if self._dropped_count > 0:
            print(  # noqa: T201
                f"[Loki] Warning: {self._dropped_count} log record(s) were dropped "
                "due to buffer overflow.",
                file=sys.stderr,
            )

    # ── Internal push loop ────────────────────────────────────────────────────

    async def _push_loop(self) -> None:
        """Main background coroutine: collect batches and push to Loki."""
        import aiohttp  # already in requirements.txt

        connector = aiohttp.TCPConnector(limit=4)
        async with aiohttp.ClientSession(connector=connector) as session:
            while True:
                batch, stopping = await self._collect_batch()
                if batch:
                    await self._push_batch(session, batch)
                if stopping:
                    # Push whatever remains in the queue and exit
                    remaining = self._drain_queue()
                    if remaining:
                        await self._push_batch(session, remaining)
                    break

    def _drain_queue(self) -> list[logging.LogRecord]:
        """Drain all remaining records from the queue synchronously."""
        records: list[logging.LogRecord] = []
        while True:
            try:
                item = self._queue.get_nowait()
                if item is None:
                    continue  # skip sentinel
                records.append(item)
            except asyncio.QueueEmpty:
                break
        return records

    async def _collect_batch(
        self,
    ) -> tuple[list[logging.LogRecord], bool]:
        """Collect up to ``batch_size`` records or wait up to ``flush_interval``.

        Returns:
            A tuple of (collected_batch, received_sentinel_flag)
        """
        batch: list[logging.LogRecord] = []
        deadline = asyncio.get_event_loop().time() + self._config.loki_flush_interval_seconds

        while len(batch) < self._config.loki_batch_size:
            remaining_time = deadline - asyncio.get_event_loop().time()
            if remaining_time <= 0:
                break
            try:
                item = await asyncio.wait_for(
                    self._queue.get(), timeout=max(remaining_time, 0.001)
                )
                if item is None:
                    # Sentinel received — stop collecting and signal loop to exit
                    return batch, True
                batch.append(item)
            except asyncio.TimeoutError:
                break

        return batch, False

    async def _push_batch(
        self,
        session: Any,
        records: list[logging.LogRecord],
    ) -> None:
        """Push a batch of records to Loki with retry logic."""
        if not records:
            return

        entries = [
            _record_to_loki_entry(r, self._config.loki_extra_labels, self._env)
            for r in records
        ]
        payload_bytes = build_loki_payload(entries)
        url = self._config.loki_url.rstrip("/") + self._config.loki_push_path

        interval = self._config.loki_retry_interval_seconds
        for attempt in range(self._config.loki_max_retries + 1):
            try:
                async with session.post(
                    url,
                    data=payload_bytes,
                    headers={"Content-Type": "application/json"},
                    timeout=self._config.loki_timeout_seconds,
                ) as resp:
                    if resp.status in (200, 204):
                        return
                    body = await resp.text()
                    print(  # noqa: T201
                        f"[Loki] Push failed: HTTP {resp.status} — {body[:200]}",
                        file=sys.stderr,
                    )
            except Exception as exc:
                print(  # noqa: T201
                    f"[Loki] Push error (attempt {attempt + 1}): {exc}",
                    file=sys.stderr,
                )

            if attempt < self._config.loki_max_retries:
                await asyncio.sleep(interval)
                interval = min(interval * 2, 30.0)  # exponential back-off, cap 30s
        # All retries exhausted — batch is dropped
