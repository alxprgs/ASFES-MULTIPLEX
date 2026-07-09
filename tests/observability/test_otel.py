from unittest.mock import patch
from server.observability.otel import setup_otel
from server.core.config import ObservabilityConfig


def test_setup_otel_disabled():
    config = ObservabilityConfig(otel_enabled=False)
    handle = setup_otel(config, "1.0.0", "production")
    assert handle is None


def test_setup_otel_no_packages():
    config = ObservabilityConfig(otel_enabled=True)
    # Mock import failure for opentelemetry-sdk
    with patch("builtins.__import__", side_effect=ImportError):
        handle = setup_otel(config, "1.0.0", "production")
        assert handle is None
