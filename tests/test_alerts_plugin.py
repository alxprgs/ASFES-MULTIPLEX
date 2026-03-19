from __future__ import annotations

import socket

import pytest


@pytest.mark.asyncio
async def test_alert_service_evaluates_rule_and_records_event(integration_env) -> None:
    services = integration_env["services"]
    await services.alerts.stop()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    host, port = server.getsockname()
    try:
        rule = await services.alerts.upsert_rule(
            {
                "name": "reachable-port",
                "source": "port.tcp_reachable",
                "selector": {"host": host, "port": port},
                "condition": "present",
                "cooldown_seconds": 0,
                "enabled": True,
                "recipients": [],
            }
        )
        assert rule["name"] == "reachable-port"

        result = await services.alerts.evaluate_rules_once()
        assert result["triggered"] >= 1

        events = await services.alerts.list_events(limit=10)
        assert any(event["rule_id"] == rule["rule_id"] for event in events)
    finally:
        server.close()
