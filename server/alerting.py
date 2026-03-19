from __future__ import annotations

import asyncio
import contextlib
import socket
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from server.core.database import ALERT_EVENTS, ALERT_RULES, DatabaseManager
from server.core.logging import Mailer, get_logger
from server.core.security import now_utc
from server.host_ops import HostOpsService


LOGGER = get_logger("multiplex.alerts")


def serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    normalized = value if value.tzinfo else value.replace(tzinfo=UTC)
    return normalized.astimezone(UTC).isoformat()


@dataclass(slots=True)
class AlertEvaluation:
    matched: bool
    value: Any
    summary: str


class AlertingService:
    def __init__(self, db: DatabaseManager, host_ops: HostOpsService, mailer: Mailer, poll_interval_seconds: int) -> None:
        self.db = db
        self.host_ops = host_ops
        self.mailer = mailer
        self.poll_interval_seconds = poll_interval_seconds
        self._task: asyncio.Task[Any] | None = None

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def list_rules(self) -> list[dict[str, Any]]:
        cursor = self.db.collection(ALERT_RULES).find().sort("name", 1)
        return [self._serialize_rule(item) async for item in cursor]

    async def get_rule(self, rule_id: str) -> dict[str, Any] | None:
        document = await self.db.collection(ALERT_RULES).find_one({"_id": rule_id})
        return self._serialize_rule(document) if document else None

    async def upsert_rule(self, payload: dict[str, Any]) -> dict[str, Any]:
        rule = self._normalize_rule(payload)
        rule_id = str(payload.get("rule_id") or payload.get("_id") or f"alert_{uuid4().hex}")
        current = await self.db.collection(ALERT_RULES).find_one({"_id": rule_id})
        created_at = current.get("created_at") if current else now_utc()
        document = {
            "_id": rule_id,
            **rule,
            "created_at": created_at,
            "updated_at": now_utc(),
            "last_triggered_at": current.get("last_triggered_at") if current else None,
        }
        await self.db.collection(ALERT_RULES).replace_one({"_id": rule_id}, document, upsert=True)
        return self._serialize_rule(document)

    async def delete_rule(self, rule_id: str) -> dict[str, Any]:
        deleted = await self.db.collection(ALERT_RULES).find_one_and_delete({"_id": rule_id})
        if not deleted:
            raise LookupError("Alert rule not found")
        return {"rule_id": rule_id, "deleted": True}

    async def list_events(self, limit: int = 100) -> list[dict[str, Any]]:
        cursor = self.db.collection(ALERT_EVENTS).find().sort("created_at", -1).limit(limit)
        items: list[dict[str, Any]] = []
        async for item in cursor:
            items.append(
                {
                    "event_id": item["_id"],
                    "rule_id": item["rule_id"],
                    "name": item["name"],
                    "severity": item["severity"],
                    "summary": item["summary"],
                    "value": item["value"],
                    "notified": bool(item.get("notified")),
                    "created_at": serialize_datetime(item.get("created_at")),
                }
            )
        return items

    async def evaluate_rules_once(self) -> dict[str, Any]:
        triggered = 0
        checked = 0
        cursor = self.db.collection(ALERT_RULES).find({"enabled": True})
        async for rule in cursor:
            checked += 1
            evaluation = await self._evaluate_rule(rule)
            if not evaluation.matched:
                continue
            if not await self._cooldown_elapsed(rule):
                continue
            await self._trigger_rule(rule, evaluation)
            triggered += 1
        return {"checked": checked, "triggered": triggered}

    async def send_test_notification(self, recipients: list[str], *, subject: str | None = None, body: str | None = None) -> dict[str, Any]:
        if not recipients:
            raise ValueError("At least one recipient is required")
        sent_to: list[str] = []
        for recipient in recipients:
            sent = await self.mailer.send_email(
                recipient,
                subject or "Multiplex alert test notification",
                body or "This is a test alert notification from Multiplex.",
            )
            if sent:
                sent_to.append(recipient)
        return {"requested": recipients, "sent_to": sent_to, "sent": bool(sent_to)}

    async def _run_loop(self) -> None:
        while True:
            try:
                await self.evaluate_rules_once()
            except Exception:
                LOGGER.exception("Alert evaluation loop failed")
            await asyncio.sleep(self.poll_interval_seconds)

    async def _evaluate_rule(self, rule: dict[str, Any]) -> AlertEvaluation:
        source = str(rule["source"])
        selector = rule.get("selector") or {}
        if not isinstance(selector, dict):
            raise ValueError("selector must be an object")
        value = await self._resolve_source_value(source, selector)
        matched = self._compare(value, str(rule["condition"]), rule.get("threshold"))
        return AlertEvaluation(matched=matched, value=value, summary=f"{source} {rule['condition']} {rule.get('threshold')}")

    async def _resolve_source_value(self, source: str, selector: dict[str, Any]) -> Any:
        if source == "system.cpu_percent":
            if not self.host_ops.psutil_available():
                raise RuntimeError("psutil is required for system.cpu_percent alerts")
            from server.host_ops import _psutil

            assert _psutil is not None
            return _psutil.cpu_percent(interval=0.1)
        if source == "system.memory_percent":
            if not self.host_ops.psutil_available():
                raise RuntimeError("psutil is required for system.memory_percent alerts")
            from server.host_ops import _psutil

            assert _psutil is not None
            return _psutil.virtual_memory().percent
        if source == "system.disk_percent":
            if not self.host_ops.psutil_available():
                raise RuntimeError("psutil is required for system.disk_percent alerts")
            from server.host_ops import _psutil

            assert _psutil is not None
            path = str(selector.get("path") or ".")
            return _psutil.disk_usage(path).percent
        if source == "process.exists":
            if not self.host_ops.psutil_available():
                raise RuntimeError("psutil is required for process.exists alerts")
            from server.host_ops import _psutil

            assert _psutil is not None
            expected = str(selector.get("name") or "").strip().lower()
            if not expected:
                raise ValueError("selector.name is required for process.exists")
            return any((proc.info.get("name") or "").lower() == expected for proc in _psutil.process_iter(["name"]))
        if source == "port.tcp_reachable":
            host = str(selector.get("host") or "127.0.0.1")
            port = int(selector.get("port"))
            timeout = float(selector.get("timeout_seconds") or 2.0)
            return await asyncio.to_thread(self._check_tcp_port, host, port, timeout)
        raise ValueError(f"Unsupported alert source: {source}")

    def _compare(self, value: Any, condition: str, threshold: Any) -> bool:
        if condition == "gt":
            return value > threshold
        if condition == "gte":
            return value >= threshold
        if condition == "lt":
            return value < threshold
        if condition == "lte":
            return value <= threshold
        if condition == "eq":
            return value == threshold
        if condition == "neq":
            return value != threshold
        if condition == "present":
            return bool(value)
        if condition == "missing":
            return not bool(value)
        raise ValueError(f"Unsupported alert condition: {condition}")

    async def _cooldown_elapsed(self, rule: dict[str, Any]) -> bool:
        last_triggered_at = rule.get("last_triggered_at")
        if not isinstance(last_triggered_at, datetime):
            return True
        cooldown = int(rule.get("cooldown_seconds") or 0)
        return (now_utc() - last_triggered_at).total_seconds() >= cooldown

    async def _trigger_rule(self, rule: dict[str, Any], evaluation: AlertEvaluation) -> None:
        created_at = now_utc()
        recipients = [str(item) for item in rule.get("recipients", []) if item]
        notified = False
        for recipient in recipients:
            sent = await self.mailer.send_email(
                recipient,
                f"[Multiplex][{rule['severity'].upper()}] {rule['name']}",
                f"Alert {rule['name']} fired.\n\nSource: {rule['source']}\nValue: {evaluation.value}\nSummary: {evaluation.summary}",
            )
            notified = notified or sent
        event = {
            "_id": uuid4().hex,
            "rule_id": rule["_id"],
            "name": rule["name"],
            "severity": rule["severity"],
            "summary": evaluation.summary,
            "value": evaluation.value,
            "notified": notified,
            "created_at": created_at,
        }
        await self.db.collection(ALERT_EVENTS).insert_one(event)
        await self.db.collection(ALERT_RULES).update_one(
            {"_id": rule["_id"]},
            {"$set": {"last_triggered_at": created_at, "updated_at": created_at}},
        )

    def _normalize_rule(self, payload: dict[str, Any]) -> dict[str, Any]:
        recipients = [str(item).strip() for item in payload.get("recipients", []) if str(item).strip()]
        selector = payload.get("selector") or {}
        if not isinstance(selector, dict):
            raise ValueError("selector must be an object")
        source = str(payload.get("source") or "").strip()
        condition = str(payload.get("condition") or "").strip()
        name = str(payload.get("name") or source or "alert").strip()
        if not source:
            raise ValueError("source is required")
        if not condition:
            raise ValueError("condition is required")
        return {
            "name": name,
            "source": source,
            "selector": selector,
            "condition": condition,
            "threshold": payload.get("threshold"),
            "window_seconds": int(payload.get("window_seconds") or 0),
            "cooldown_seconds": int(payload.get("cooldown_seconds") or 0),
            "severity": str(payload.get("severity") or "warning"),
            "enabled": bool(payload.get("enabled", True)),
            "recipients": recipients,
        }

    def _serialize_rule(self, rule: dict[str, Any]) -> dict[str, Any]:
        return {
            "rule_id": rule["_id"],
            "name": rule["name"],
            "source": rule["source"],
            "selector": rule.get("selector", {}),
            "condition": rule["condition"],
            "threshold": rule.get("threshold"),
            "window_seconds": int(rule.get("window_seconds") or 0),
            "cooldown_seconds": int(rule.get("cooldown_seconds") or 0),
            "severity": rule["severity"],
            "enabled": bool(rule.get("enabled", True)),
            "recipients": list(rule.get("recipients", [])),
            "last_triggered_at": serialize_datetime(rule.get("last_triggered_at")),
            "created_at": serialize_datetime(rule.get("created_at")),
            "updated_at": serialize_datetime(rule.get("updated_at")),
        }

    @staticmethod
    def _check_tcp_port(host: str, port: int, timeout: float) -> bool:
        with socket.create_connection((host, port), timeout=timeout):
            return True
