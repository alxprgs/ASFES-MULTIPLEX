import asyncio
import logging
import json
import pytest
from unittest.mock import AsyncMock, patch
from server.observability.loki import (
    _record_to_loki_entry,
    build_loki_payload,
    LokiHandler,
)
from server.core.config import ObservabilityConfig


def test_record_to_loki_entry():
    record = logging.LogRecord(
        name="multiplex.services.test",
        level=logging.INFO,
        pathname="test.py",
        lineno=10,
        msg="Test message with payload",
        args=None,
        exc_info=None,
    )
    record.payload = {"user_id": "123"}
    record.event_type = "user.action"
    record.correlation_id = "corr-1"

    entry = _record_to_loki_entry(record, {"host": "my-host"}, "production")

    assert entry["stream"]["app"] == "multiplex"
    assert entry["stream"]["env"] == "production"
    assert entry["stream"]["level"] == "INFO"
    assert entry["stream"]["logger"] == "multiplex.services"
    assert entry["stream"]["host"] == "my-host"

    value = entry["values"][0]
    ts, val_json, meta = value[0], value[1], value[2]
    assert ts.isdigit()

    data = json.loads(val_json)
    assert data["message"] == "Test message with payload"
    assert data["payload"] == {"user_id": "123"}
    assert data["event_type"] == "user.action"
    assert meta == {"correlation_id": "corr-1"}


def test_build_loki_payload():
    entries = [
        {
            "stream": {"app": "multiplex", "env": "prod"},
            "values": [["123", "line1"]]
        },
        {
            "stream": {"app": "multiplex", "env": "prod"},
            "values": [["456", "line2"]]
        },
        {
            "stream": {"app": "multiplex", "env": "dev"},
            "values": [["789", "line3"]]
        }
    ]

    payload_bytes = build_loki_payload(entries)
    payload = json.loads(payload_bytes.decode("utf-8"))

    assert len(payload["streams"]) == 2
    # Verify dev stream
    dev_stream = next(s for s in payload["streams"] if s["stream"]["env"] == "dev")
    assert dev_stream["values"] == [["789", "line3"]]

    # Verify prod stream (merged)
    prod_stream = next(s for s in payload["streams"] if s["stream"]["env"] == "prod")
    assert len(prod_stream["values"]) == 2
    assert ["123", "line1"] in prod_stream["values"]
    assert ["456", "line2"] in prod_stream["values"]


@pytest.mark.asyncio
async def test_loki_handler_lifecycle():
    config = ObservabilityConfig(
        loki_enabled=True,
        loki_batch_size=2,
        loki_flush_interval_seconds=0.1,
        loki_buffer_max_size=100,
    )
    handler = LokiHandler(config)
    handler.set_env("test")

    # Mock the push batch function to avoid actual network requests but let the loop run
    with patch.object(handler, "_push_batch", new_callable=AsyncMock) as mock_push:
        await handler.start()
        # Emit a record
        record = logging.LogRecord("test", logging.INFO, "x.py", 1, "msg", None, None)
        handler.emit(record)
        
        await asyncio.sleep(0.01) # yield to event loop so call_soon_threadsafe executes
        await handler.stop()
        assert mock_push.called
        assert handler._queue.empty()
