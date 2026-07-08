"""
Control Plane Guard (v3.3)
Protects the system from API overloads, queue pressure, and batch storms.
Reads metrics from Redis to provide backpressure.
"""

from __future__ import annotations

import logging

from fastapi import HTTPException
from pydantic import BaseModel

from server.core.cache import CacheManager
from server.core.ratelimit import RateLimiter

logger = logging.getLogger("multiplex.control_plane")


class ControlPlaneMetrics(BaseModel):
    pypi_queue_depth: int
    batch_queue_depth: int
    mcp_queue_depth: int
    failed_job_ratio: float
    system_overloaded: bool


class ControlPlaneGuard:
    def __init__(self, cache: CacheManager, rate_limiter: RateLimiter):
        self.cache = cache
        self.rate_limiter = rate_limiter
        self.MAX_QUEUE_DEPTH = 50
        self.MAX_GLOBAL_BATCHES = 2
        self.MAX_GLOBAL_CONCURRENT_JOBS = 5

    async def get_queue_depth(self, queue_name: str) -> int:
        if not self.cache.should_use_redis() or self.cache._redis is None:
            return 0
        try:
            return await self.cache._redis.xlen(queue_name)
        except Exception as e:
            logger.warning("Failed to get queue depth for %s: %s", queue_name, e)
            return 0

    async def get_metrics(self) -> ControlPlaneMetrics:
        pypi_depth = await self.get_queue_depth("queue:pypi_tasks")
        batch_depth = await self.get_queue_depth("queue:batch_tasks")
        mcp_depth = await self.get_queue_depth("queue:mcp_tasks")

        # Simple heuristic for failure ratio based on a cached counter
        failed_count = 0
        total_count = 1
        if self.cache.should_use_redis() and self.cache._redis:
            val = await self.cache.get("metrics:failed_jobs_window")
            if val and isinstance(val, dict):
                failed_count = val.get("failed", 0)
                total_count = max(val.get("total", 1), 1)

        failed_job_ratio = failed_count / total_count
        overloaded = (
            pypi_depth > self.MAX_QUEUE_DEPTH
            or batch_depth > self.MAX_QUEUE_DEPTH
            or mcp_depth > self.MAX_QUEUE_DEPTH
            or failed_job_ratio > 0.5
        )

        return ControlPlaneMetrics(
            pypi_queue_depth=pypi_depth,
            batch_queue_depth=batch_depth,
            mcp_queue_depth=mcp_depth,
            failed_job_ratio=failed_job_ratio,
            system_overloaded=overloaded,
        )

    async def check_system_load(self) -> None:
        """
        Throws 429 if the system is overloaded.
        """
        metrics = await self.get_metrics()
        if metrics.system_overloaded:
            raise HTTPException(
                status_code=429,
                detail="SYSTEM_BACKPRESSURE",
                headers={"Retry-After": "10", "X-System-Load": "high"},
            )

    async def record_job_result(self, failed: bool) -> None:
        """
        Maintains a rolling window of success/fail ratios.
        """
        if not self.cache.should_use_redis() or self.cache._redis is None:
            return

        try:
            val = await self.cache.get("metrics:failed_jobs_window") or {
                "failed": 0,
                "total": 0,
            }
            val["total"] += 1
            if failed:
                val["failed"] += 1

            if val["total"] > 100:
                val["total"] = val["total"] // 2
                val["failed"] = val["failed"] // 2

            await self.cache.set("metrics:failed_jobs_window", val, ttl_seconds=3600)
        except Exception:
            pass
