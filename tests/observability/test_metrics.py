from prometheus_client import generate_latest
from server.observability.metrics import (
    normalize_path,
    REGISTRY,
    observe_http_request,
    inc_audit_event,
    inc_auth_attempt,
    inc_rate_limit_hit,
    inc_pypi_download,
    inc_alert_fire,
    init_metrics,
)
from server.core.config import ObservabilityConfig


def test_normalize_path():
    assert normalize_path("/api/v1/users") == "/api/v1/users"
    assert normalize_path("/api/users/123") == "/api/users/{id}"
    assert normalize_path("/api/items/550e8400-e29b-41d4-a716-446655440000") == "/api/items/{id}"
    assert normalize_path("/") == "/"
    assert normalize_path("") == "/"


def test_metrics_collection():
    config = ObservabilityConfig(prometheus_enabled=True)
    assert init_metrics(config) is True

    # Record some metrics
    observe_http_request("GET", "/api/users/123", "200", 0.052)
    inc_audit_event("user.login")
    inc_auth_attempt("password", "success")
    inc_rate_limit_hit("login_limit")
    inc_pypi_download()
    inc_alert_fire("cpu_usage_high")

    # Generate output
    content = generate_latest(REGISTRY).decode("utf-8")

    assert 'multiplex_http_requests_total{method="GET",path_template="/api/users/{id}",status_code="200"} 1.0' in content
    assert 'multiplex_http_request_duration_seconds_count{method="GET",path_template="/api/users/{id}",status_code="200"} 1.0' in content
    assert 'multiplex_audit_events_total{action="user.login"} 1.0' in content
    assert 'multiplex_auth_attempts_total{method="password",result="success"} 1.0' in content
    assert 'multiplex_rate_limit_hits_total{policy_name="login_limit"} 1.0' in content
    assert 'multiplex_pypi_downloads_total 1.0' in content
    assert 'multiplex_alert_rule_fires_total{rule_id="cpu_usage_high"} 1.0' in content
