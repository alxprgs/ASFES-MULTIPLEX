from __future__ import annotations

import asyncio
import json
import logging
import smtplib
import sqlite3
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from email.message import EmailMessage
from hashlib import sha256
from pathlib import Path
from typing import Any

from rich.logging import RichHandler

from server.core.config import LoggingConfig, SMTPConfig


def utc_now() -> datetime:
    return datetime.now(UTC)


class Mailer:
    def __init__(self, smtp: SMTPConfig) -> None:
        self.smtp = smtp

    async def send_email(self, recipient: str, subject: str, body: str) -> bool:
        if not self.smtp.enabled:
            return False
        return await asyncio.to_thread(self._send_email_sync, recipient, subject, body)

    def _send_email_sync(self, recipient: str, subject: str, body: str) -> bool:
        message = EmailMessage()
        message["From"] = str(self.smtp.from_email)
        message["To"] = recipient
        message["Subject"] = subject
        message.set_content(body)
        smtp_client: smtplib.SMTP | smtplib.SMTP_SSL
        if self.smtp.use_ssl:
            smtp_client = smtplib.SMTP_SSL(self.smtp.host, self.smtp.port, timeout=self.smtp.timeout_seconds)
        else:
            smtp_client = smtplib.SMTP(self.smtp.host, self.smtp.port, timeout=self.smtp.timeout_seconds)
        with smtp_client as client:
            if self.smtp.starttls and not self.smtp.use_ssl:
                client.starttls()
            if self.smtp.username:
                client.login(self.smtp.username, self.smtp.password.get_secret_value() if self.smtp.password else "")
            client.send_message(message)
        return True


@dataclass(slots=True)
class TamperDetection:
    file_path: str
    line_number: int | None
    reason: str
    raw_line: str | None


class IntegrityLogHandler(logging.Handler):
    def __init__(self, manager: "IntegrityLogManager") -> None:
        super().__init__()
        self.manager = manager

    def emit(self, record: logging.LogRecord) -> None:
        self.manager.write_record(record)


class IntegrityLogManager:
    def __init__(self, config: LoggingConfig, mailer: Mailer, root_email: str) -> None:
        self.config = config
        self.mailer = mailer
        self.root_email = root_email
        self._lock = threading.RLock()
        self._db: sqlite3.Connection | None = None
        self._current_hour_key: str | None = None
        self._current_file_path: Path | None = None
        self._last_line_hash_by_file: dict[str, str] = {}

    def initialize(self) -> None:
        self.config.directory.mkdir(parents=True, exist_ok=True)
        self.config.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(self.config.sqlite_path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._initialize_schema()
        self._load_last_hashes()
        self.install_logging()

    def _initialize_schema(self) -> None:
        assert self._db is not None
        cursor = self._db.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS log_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                level TEXT NOT NULL,
                logger_name TEXT NOT NULL,
                event_type TEXT NOT NULL,
                message TEXT NOT NULL,
                file_path TEXT NOT NULL,
                prev_hash TEXT NOT NULL,
                line_hash TEXT NOT NULL,
                payload_json TEXT NOT NULL
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS log_files (
                file_path TEXT PRIMARY KEY,
                hour_key TEXT NOT NULL,
                sealed_hash TEXT,
                status TEXT NOT NULL,
                sealed_at TEXT,
                last_verified_at TEXT,
                tamper_reason TEXT
            )
            """
        )
        self._db.commit()

    def _load_last_hashes(self) -> None:
        assert self._db is not None
        cursor = self._db.execute(
            """
            SELECT file_path, line_hash
            FROM log_entries
            WHERE id IN (
                SELECT MAX(id) FROM log_entries GROUP BY file_path
            )
            """
        )
        for row in cursor.fetchall():
            self._last_line_hash_by_file[row["file_path"]] = row["line_hash"]

    def install_logging(self) -> None:
        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        root_logger.setLevel(self.config.level)
        rich_handler = RichHandler(
            rich_tracebacks=self.config.console_rich_tracebacks,
            markup=False,
            show_level=True,
            show_path=False,
        )
        rich_handler.setLevel(self.config.level)
        integrity_handler = IntegrityLogHandler(self)
        integrity_handler.setLevel(self.config.level)
        root_logger.addHandler(rich_handler)
        root_logger.addHandler(integrity_handler)

    def _hour_key_for(self, created_at: datetime) -> str:
        return created_at.strftime("%Y%m%d%H")

    def _path_for_hour(self, hour_key: str) -> Path:
        return self.config.directory / f"multiplex-{hour_key}.log"

    def _ensure_current_file(self, created_at: datetime) -> Path:
        hour_key = self._hour_key_for(created_at)
        if self._current_hour_key == hour_key and self._current_file_path is not None:
            return self._current_file_path
        if self._current_file_path is not None and self._current_hour_key is not None:
            self._seal_file(self._current_file_path, self._current_hour_key)
        self._current_hour_key = hour_key
        self._current_file_path = self._path_for_hour(hour_key)
        self._current_file_path.parent.mkdir(parents=True, exist_ok=True)
        return self._current_file_path

    def write_record(self, record: logging.LogRecord) -> None:
        created_at = datetime.fromtimestamp(record.created, UTC)
        payload = getattr(record, "payload", {}) or {}
        event_type = getattr(record, "event_type", record.levelname.lower())
        serialized: dict[str, Any] = {
            "timestamp": created_at.isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "event_type": event_type,
            "payload": payload,
        }
        if record.exc_info:
            serialized["exception"] = logging.Formatter().formatException(record.exc_info)

        with self._lock:
            file_path = self._ensure_current_file(created_at)
            prev_hash = self._last_line_hash_by_file.get(str(file_path), "")
            canonical = json.dumps(serialized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            line_hash = sha256(f"{prev_hash}|{canonical}".encode("utf-8")).hexdigest()
            serialized["prev_hash"] = prev_hash
            serialized["line_hash"] = line_hash
            with file_path.open("a", encoding="utf-8") as log_file:
                log_file.write(json.dumps(serialized, ensure_ascii=False, sort_keys=True) + "\n")
            self._last_line_hash_by_file[str(file_path)] = line_hash
            self._mirror_to_sqlite(created_at, record.levelname, record.name, event_type, record.getMessage(), str(file_path), prev_hash, line_hash, payload)

    def _mirror_to_sqlite(
        self,
        created_at: datetime,
        level: str,
        logger_name: str,
        event_type: str,
        message: str,
        file_path: str,
        prev_hash: str,
        line_hash: str,
        payload: dict[str, Any],
    ) -> None:
        assert self._db is not None
        self._db.execute(
            """
            INSERT INTO log_entries (
                created_at, level, logger_name, event_type, message, file_path, prev_hash, line_hash, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                created_at.isoformat(),
                level,
                logger_name,
                event_type,
                message,
                file_path,
                prev_hash,
                line_hash,
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
            ),
        )
        self._db.execute(
            """
            INSERT INTO log_files (file_path, hour_key, status)
            VALUES (?, ?, ?)
            ON CONFLICT(file_path) DO NOTHING
            """,
            (file_path, Path(file_path).stem.split("-")[-1], "open"),
        )
        self._db.commit()

    def _seal_file(self, file_path: Path, hour_key: str) -> None:
        assert self._db is not None
        if not file_path.exists():
            return
        sealed_hash = self.compute_file_hash(file_path)
        self._db.execute(
            """
            INSERT INTO log_files (file_path, hour_key, sealed_hash, status, sealed_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(file_path) DO UPDATE SET
                sealed_hash=excluded.sealed_hash,
                status=excluded.status,
                sealed_at=excluded.sealed_at
            """,
            (str(file_path), hour_key, sealed_hash, "sealed", utc_now().isoformat()),
        )
        self._db.commit()

    @staticmethod
    def compute_file_hash(file_path: Path) -> str:
        digest = sha256()
        with file_path.open("rb") as handle:
            while chunk := handle.read(65536):
                digest.update(chunk)
        return digest.hexdigest()

    async def verify_integrity(self) -> list[TamperDetection]:
        with self._lock:
            assert self._db is not None
            rows = self._db.execute("SELECT file_path, sealed_hash FROM log_files WHERE status='sealed'").fetchall()
        detections: list[TamperDetection] = []
        for row in rows:
            file_path = Path(row["file_path"])
            if not file_path.exists():
                detection = TamperDetection(str(file_path), None, "log file missing", None)
                detections.append(detection)
                await self._mark_tampered(detection)
                continue
            current_hash = await asyncio.to_thread(self.compute_file_hash, file_path)
            if current_hash == row["sealed_hash"]:
                with self._lock:
                    assert self._db is not None
                    self._db.execute(
                        "UPDATE log_files SET last_verified_at=? WHERE file_path=?",
                        (utc_now().isoformat(), str(file_path)),
                    )
                    self._db.commit()
                continue
            detection = await asyncio.to_thread(self._inspect_tampered_file, file_path)
            detections.append(detection)
            await self._mark_tampered(detection)
        return detections

    def _inspect_tampered_file(self, file_path: Path) -> TamperDetection:
        previous_hash = ""
        with file_path.open("r", encoding="utf-8") as handle:
            for index, raw_line in enumerate(handle, start=1):
                try:
                    data = json.loads(raw_line)
                except json.JSONDecodeError:
                    return TamperDetection(str(file_path), index, "invalid JSON line", raw_line.rstrip())
                stored_prev_hash = data.get("prev_hash", "")
                stored_line_hash = data.get("line_hash", "")
                canonical_payload = {key: value for key, value in data.items() if key not in {"prev_hash", "line_hash"}}
                canonical = json.dumps(canonical_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                computed_line_hash = sha256(f"{stored_prev_hash}|{canonical}".encode("utf-8")).hexdigest()
                if stored_prev_hash != previous_hash:
                    return TamperDetection(str(file_path), index, "hash chain mismatch", raw_line.rstrip())
                if stored_line_hash != computed_line_hash:
                    return TamperDetection(str(file_path), index, "line hash mismatch", raw_line.rstrip())
                previous_hash = stored_line_hash
        return TamperDetection(str(file_path), None, "sealed file hash mismatch", None)

    async def _mark_tampered(self, detection: TamperDetection) -> None:
        with self._lock:
            assert self._db is not None
            self._db.execute(
                "UPDATE log_files SET status=?, tamper_reason=?, last_verified_at=? WHERE file_path=?",
                ("tampered", detection.reason, utc_now().isoformat(), detection.file_path),
            )
            self._db.commit()
        logging.getLogger("multiplex.integrity").critical(
            "Log integrity violation detected",
            extra={
                "event_type": "log.integrity.violation",
                "payload": {
                    "file_path": detection.file_path,
                    "line_number": detection.line_number,
                    "reason": detection.reason,
                    "raw_line": detection.raw_line,
                },
            },
        )
        if self.root_email:
            await self.mailer.send_email(
                self.root_email,
                "[Multiplex] Log integrity violation detected",
                json.dumps(
                    {
                        "file_path": detection.file_path,
                        "line_number": detection.line_number,
                        "reason": detection.reason,
                        "raw_line": detection.raw_line,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
            )

    def finalize(self) -> None:
        with self._lock:
            if self._current_file_path is not None and self._current_hour_key is not None:
                self._seal_file(self._current_file_path, self._current_hour_key)
            if self._db is not None:
                self._db.close()
                self._db = None


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
