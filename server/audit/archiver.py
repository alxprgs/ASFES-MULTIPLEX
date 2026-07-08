import asyncio
import gzip
import hashlib
import json
import logging
from datetime import datetime, timedelta, UTC
from pathlib import Path

from pymongo import ASCENDING

from server.core.config import Settings
from server.core.database import AUDIT_EVENTS, DatabaseManager

LOGGER = logging.getLogger("multiplex.audit.archiver")


class AuditArchiverJob:
    def __init__(
        self, db: DatabaseManager, settings: Settings, retention_days: int = 30
    ) -> None:
        self.db = db
        self.settings = settings
        self.retention_days = retention_days
        self.archive_dir = Path(settings.logging.directory).parent / "audit_archives"
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        self._running = False
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_periodic())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run_periodic(self) -> None:
        while self._running:
            try:
                await self.run_now()
                # Run cleanup of old archived events
                await self.run_cleanup(days_old=90)
            except asyncio.CancelledError:
                break
            except Exception as e:
                LOGGER.error(f"AuditArchiverJob error: {e}", exc_info=True)

            # Sleep for 24 hours
            try:
                await asyncio.sleep(86400)
            except asyncio.CancelledError:
                break

    async def run_now(self) -> None:
        cutoff = (datetime.now(UTC) - timedelta(days=self.retention_days)).isoformat()
        collection = self.db.collection(AUDIT_EVENTS)

        while self._running:
            query = {"archived": False, "timestamp": {"$lt": cutoff}}
            # Process in batches
            cursor = collection.find(query).sort("timestamp", ASCENDING).limit(500)
            batch = await cursor.to_list(length=500)

            if not batch:
                break

            await self._archive_batch(batch, collection)

            # Yield / Throttle to avoid starving production traffic
            await asyncio.sleep(0.1)

    async def _archive_batch(self, batch: list[dict], collection) -> None:
        if not batch:
            return

        first_ts = batch[0].get("timestamp", "unknown")[:10]  # YYYY-MM-DD
        last_ts = batch[-1].get("timestamp", "unknown")[:10]

        # We append to a daily or monthly file, or just create a batch file.
        # Let's create a specific file for this batch based on current time to avoid concurrency issues.
        now_str = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        filename = f"audit_{first_ts}_to_{last_ts}_{now_str}.jsonl.gz"
        filepath = self.archive_dir / filename

        hasher = hashlib.sha256()

        # CPU bound, could run in executor if batches are huge, but 500 is fast enough
        with gzip.open(filepath, "wt", encoding="utf-8") as f:
            for doc in batch:
                doc.pop("_id", None)
                line = json.dumps(doc, ensure_ascii=False) + "\n"
                f.write(line)
                hasher.update(line.encode("utf-8"))

        archive_hash = hasher.hexdigest()
        event_ids = [doc["event_id"] for doc in batch]

        # Atomic update
        result = await collection.update_many(
            {"event_id": {"$in": event_ids}},
            {
                "$set": {
                    "archived": True,
                    "archive_file": filename,
                    "archive_hash": archive_hash,
                }
            },
        )

        LOGGER.info(
            f"Archived {result.modified_count} events to {filename} (hash: {archive_hash[:8]})"
        )

    async def run_cleanup(self, days_old: int = 90) -> None:
        cutoff = (datetime.now(UTC) - timedelta(days=days_old)).isoformat()
        collection = self.db.collection(AUDIT_EVENTS)

        # Delete only events that have been archived and are older than X days
        query = {"archived": True, "timestamp": {"$lt": cutoff}}

        result = await collection.delete_many(query)
        if result.deleted_count > 0:
            LOGGER.info(
                f"Cleaned up {result.deleted_count} archived events older than {days_old} days from MongoDB"
            )
