from __future__ import annotations

import json
import time
from typing import Any

from server.core.logging import get_logger

LOGGER = get_logger("multiplex.cache")


class CacheManager:
    """
    Centralized cache manager.
    Supports Redis (with JSON serialization) and falls back to an in-memory dictionary
    with TTL if Redis is disabled or temporarily unavailable.
    """

    def __init__(
        self,
        redis_mode: str = "disabled",
        redis_url: str | None = None,
        redis_runtime_enabled: bool = False,
    ) -> None:
        self.redis_mode = redis_mode
        self.redis_url = redis_url
        self.redis_runtime_enabled = redis_runtime_enabled
        self._redis: Any | None = None
        self._memory: dict[str, tuple[float, Any]] = {}

    def should_use_redis(self) -> bool:
        if self.redis_mode == "required":
            return True
        if self.redis_mode == "runtime":
            return self.redis_runtime_enabled
        return False

    async def connect(self) -> None:
        if self.should_use_redis():
            if not self.redis_url:
                raise RuntimeError("REDIS__URL must be configured when Redis is enabled")
            try:
                from redis.asyncio import from_url
                self._redis = from_url(self.redis_url, decode_responses=True)
                await self._redis.ping()
                LOGGER.info("Connected to Redis cache")
            except Exception as exc:
                LOGGER.error("Failed to connect to Redis cache: %s", exc)
                self._redis = None
                if self.redis_mode == "required":
                    raise RuntimeError("Redis connection failed but mode is required") from exc

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    async def set_runtime_enabled(self, enabled: bool) -> None:
        if self.redis_mode == "required" and not enabled:
            raise ValueError("Redis runtime disable is forbidden when REDIS__MODE=required")
        self.redis_runtime_enabled = enabled
        if self.should_use_redis():
            if not self._redis:
                await self.connect()
        elif self._redis is not None:
            await self.close()

    def _cleanup_memory(self) -> None:
        now = time.monotonic()
        keys = [k for k, (exp, _) in self._memory.items() if exp < now]
        for k in keys:
            self._memory.pop(k, None)

    async def get(self, key: str) -> Any | None:
        if self.should_use_redis() and self._redis is not None:
            try:
                val = await self._redis.get(key)
                if val is not None:
                    return json.loads(val)
                return None
            except Exception as exc:
                LOGGER.warning("Redis get error for %s: %s", key, exc)

        # Fallback
        self._cleanup_memory()
        item = self._memory.get(key)
        if item:
            exp, val = item
            if exp >= time.monotonic():
                return val
            else:
                self._memory.pop(key, None)
        return None

    async def set(self, key: str, value: Any, ttl_seconds: int = 300) -> None:
        if self.should_use_redis() and self._redis is not None:
            try:
                await self._redis.setex(key, ttl_seconds, json.dumps(value))
                return
            except Exception as exc:
                LOGGER.warning("Redis set error for %s: %s", key, exc)

        # Fallback
        self._cleanup_memory()
        self._memory[key] = (time.monotonic() + ttl_seconds, value)

    async def delete(self, key: str) -> None:
        if self.should_use_redis() and self._redis is not None:
            try:
                await self._redis.delete(key)
            except Exception as exc:
                LOGGER.warning("Redis delete error for %s: %s", key, exc)

        self._memory.pop(key, None)

    async def delete_prefix(self, prefix: str) -> None:
        if self.should_use_redis() and self._redis is not None:
            try:
                keys = await self._redis.keys(f"{prefix}*")
                if keys:
                    await self._redis.delete(*keys)
            except Exception as exc:
                LOGGER.warning("Redis delete_prefix error for %s: %s", prefix, exc)

        keys_to_delete = [k for k in self._memory.keys() if k.startswith(prefix)]
        for k in keys_to_delete:
            self._memory.pop(k, None)
