"""
Distributed Execution Engine (v3.3)
Handles Redis Streams queueing, MongoDB leasing, exactly-once guards, and ghost worker protection.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Coroutine

from server.core.cache import CacheManager
from server.core.database import DatabaseManager

logger = logging.getLogger("multiplex.distributed")


def create_fingerprint(*args: Any) -> str:
    """Generate immutable job fingerprint."""
    raw = "|".join(str(a) for a in args)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class DistributedWorkerContext:
    def __init__(self, db: DatabaseManager, cache: CacheManager, worker_id: str):
        self.db = db
        self.cache = cache
        self.worker_id = worker_id
        self.jobs_collection = "distributed_jobs"

    async def create_job(
        self,
        kind: str,
        target: str,
        fingerprint: str,
        extra: dict[str, Any] | None = None,
    ) -> str:
        """Create a job in MongoDB (Source of Truth)."""
        doc = {
            "kind": kind,
            "target": target,
            "fingerprint": fingerprint,
            "status": "pending",
            "lock_owner": None,
            "lock_expires_at": None,
            "created_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
            "done": 0,
            "total": 0,
            "failed": 0,
            "extra": extra or {},
        }
        res = await self.db.collection(self.jobs_collection).insert_one(doc)
        job_id = str(res.inserted_id)

        # Enqueue to Redis Stream
        if self.cache.should_use_redis() and self.cache._redis:
            queue_name = f"queue:{kind}_tasks"
            try:
                await self.cache._redis.xadd(queue_name, {"job_id": job_id})
            except Exception as e:
                logger.error("Failed to enqueue job %s to Redis: %s", job_id, e)
                # Degraded mode: the job is just in MongoDB.
        return job_id

    async def acquire_lease(self, job_id: str, ttl_seconds: int = 60) -> bool:
        """
        Exactly-Once Execution Guard.
        Attempts to acquire a lease via MongoDB atomic update.
        Only succeeds if status == "pending" OR lock expired.
        """
        now = datetime.now(UTC)
        expiry = now + timedelta(seconds=ttl_seconds)

        from bson import ObjectId

        query = {
            "_id": ObjectId(job_id),
            "status": {"$in": ["pending", "running"]},
            "$or": [{"lock_owner": None}, {"lock_expires_at": {"$lt": now}}],
        }
        update = {
            "$set": {
                "lock_owner": self.worker_id,
                "lock_expires_at": expiry,
                "status": "running",
                "updated_at": now,
            }
        }
        res = await self.db.collection(self.jobs_collection).update_one(query, update)
        return res.modified_count > 0

    async def update_job(self, job_id: str, status: str, **kwargs: Any) -> None:
        """Update job progress/status."""
        from bson import ObjectId

        now = datetime.now(UTC)
        update_doc = {"status": status, "updated_at": now, **kwargs}
        if status in ("done", "error", "cancelled"):
            update_doc["lock_owner"] = None
            update_doc["lock_expires_at"] = None

        await self.db.collection(self.jobs_collection).update_one(
            {"_id": ObjectId(job_id)}, {"$set": update_doc}
        )

    async def start_ghost_worker_detector(self) -> None:
        """Background loop to detect and requeue stale leases."""
        while True:
            try:
                now = datetime.now(UTC)
                query = {"status": "running", "lock_expires_at": {"$lt": now}}
                update = {
                    "$set": {
                        "status": "pending",
                        "lock_owner": None,
                        "lock_expires_at": None,
                        "updated_at": now,
                    }
                }
                # Find which kinds of jobs were stale to requeue them
                stale_jobs = (
                    await self.db.collection(self.jobs_collection)
                    .find(query)
                    .to_list(100)
                )
                if stale_jobs:
                    # Update them in MongoDB
                    await self.db.collection(self.jobs_collection).update_many(
                        query, update
                    )
                    # Re-inject to Redis streams
                    if self.cache.should_use_redis() and self.cache._redis:
                        for job in stale_jobs:
                            queue_name = f"queue:{job['kind']}_tasks"
                            await self.cache._redis.xadd(
                                queue_name, {"job_id": str(job["_id"])}
                            )
                            logger.info(
                                "Ghost worker detected. Requeued job %s to %s",
                                str(job["_id"]),
                                queue_name,
                            )
            except Exception as e:
                logger.error("Ghost worker detector error: %s", e)
            await asyncio.sleep(60)

    async def consume_stream(
        self,
        queue_name: str,
        group_name: str,
        handler: Callable[[str], Coroutine[Any, Any, None]],
    ) -> None:
        """Continuously reads from Redis Stream, executes handler via MongoDB lease."""
        if not self.cache.should_use_redis() or not self.cache._redis:
            logger.warning("Redis not available. Degraded mode for %s", queue_name)
            return

        redis = self.cache._redis
        try:
            await redis.xgroup_create(queue_name, group_name, id="0", mkstream=True)
        except Exception:
            pass  # Group probably exists

        while True:
            try:
                # Read 1 item
                streams = await redis.xreadgroup(
                    group_name, self.worker_id, {queue_name: ">"}, count=1, block=5000
                )
                if not streams:
                    continue

                for stream, messages in streams:
                    for message_id, msg_data in messages:
                        job_id = msg_data.get("job_id")
                        if job_id:
                            # Try to acquire lease
                            if await self.acquire_lease(job_id):
                                try:
                                    await handler(job_id)
                                except Exception as e:
                                    logger.error(
                                        "Job %s execution failed: %s", job_id, e
                                    )
                                    await self.update_job(job_id, status="error")
                            else:
                                # Dropped (already done or leased by another worker)
                                pass

                        # Always ACK
                        await redis.xack(queue_name, group_name, message_id)
            except Exception as e:
                logger.error("Stream consumer error: %s", e)
                await asyncio.sleep(5)
