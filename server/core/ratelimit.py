from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any


class RateLimitError(Exception):
    def __init__(self, policy_name: str, retry_after: int) -> None:
        super().__init__(f"Rate limit exceeded for policy '{policy_name}'")
        self.policy_name = policy_name
        self.retry_after = retry_after


@dataclass(slots=True, frozen=True)
class RateLimitPolicy:
    name: str
    limit: int
    window_seconds: int


@dataclass(slots=True, frozen=True)
class RateLimitResult:
    allowed: bool
    limit: int
    remaining: int
    retry_after: int
    reset_after: int


class MemoryRateLimiterBackend:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._buckets: dict[str, tuple[int, float]] = {}

    async def consume(self, key: str, policy: RateLimitPolicy) -> RateLimitResult:
        async with self._lock:
            now = time.time()
            count, reset_at = self._buckets.get(key, (0, now + policy.window_seconds))
            if now >= reset_at:
                count = 0
                reset_at = now + policy.window_seconds

            if count >= policy.limit:
                retry_after = max(1, int(reset_at - now))
                return RateLimitResult(False, policy.limit, 0, retry_after, retry_after)

            count += 1
            self._buckets[key] = (count, reset_at)
            remaining = max(0, policy.limit - count)
            reset_after = max(1, int(reset_at - now))
            return RateLimitResult(True, policy.limit, remaining, 0, reset_after)


class RedisRateLimiterBackend:
    def __init__(self, redis_url: str, prefix: str = "multiplex:ratelimit") -> None:
        self.redis_url = redis_url
        self.prefix = prefix
        self._redis: Any | None = None

    async def connect(self) -> None:
        if self._redis is not None:
            return
        try:
            from redis.asyncio import from_url
        except ImportError as exc:
            raise RuntimeError("redis dependency is required for Redis-backed rate limiting") from exc
        self._redis = from_url(self.redis_url, decode_responses=True)
        await self._redis.ping()

    async def close(self) -> None:
        if self._redis is None:
            return
        await self._redis.close()
        self._redis = None

    async def consume(self, key: str, policy: RateLimitPolicy) -> RateLimitResult:
        if self._redis is None:
            await self.connect()
        assert self._redis is not None
        redis_key = f"{self.prefix}:{policy.name}:{key}"
        count = await self._redis.incr(redis_key)
        if count == 1:
            await self._redis.expire(redis_key, policy.window_seconds)
        ttl = await self._redis.ttl(redis_key)
        retry_after = max(1, ttl if ttl > 0 else policy.window_seconds)
        allowed = int(count) <= policy.limit
        remaining = max(0, policy.limit - int(count))
        return RateLimitResult(allowed, policy.limit, remaining if allowed else 0, 0 if allowed else retry_after, retry_after)


class RateLimiter:
    def __init__(
        self,
        policies: dict[str, RateLimitPolicy],
        redis_mode: str = "disabled",
        redis_url: str | None = None,
        redis_runtime_enabled: bool = False,
    ) -> None:
        self.policies = policies
        self.redis_mode = redis_mode
        self.redis_url = redis_url
        self.redis_runtime_enabled = redis_runtime_enabled
        self._memory = MemoryRateLimiterBackend()
        self._redis = RedisRateLimiterBackend(redis_url) if redis_url else None

    def should_use_redis(self) -> bool:
        if self.redis_mode == "required":
            return True
        if self.redis_mode == "runtime":
            return self.redis_runtime_enabled
        return False

    async def initialize(self) -> None:
        if self.should_use_redis():
            if not self._redis:
                raise RuntimeError("REDIS__URL must be configured when Redis is enabled")
            await self._redis.connect()

    async def shutdown(self) -> None:
        if self._redis is not None:
            await self._redis.close()

    async def set_runtime_enabled(self, enabled: bool) -> None:
        if self.redis_mode == "required" and not enabled:
            raise ValueError("Redis runtime disable is forbidden when REDIS__MODE=required")
        self.redis_runtime_enabled = enabled
        if self.should_use_redis():
            if not self._redis:
                raise RuntimeError("REDIS__URL must be configured when Redis is enabled")
            await self._redis.connect()
        elif self._redis is not None:
            await self._redis.close()

    async def consume(self, policy_name: str, key: str) -> RateLimitResult:
        policy = self.policies[policy_name]
        backend = self._redis if self.should_use_redis() and self._redis is not None else self._memory
        return await backend.consume(key, policy)

    async def enforce(self, policy_name: str, key: str) -> RateLimitResult:
        result = await self.consume(policy_name, key)
        if not result.allowed:
            raise RateLimitError(policy_name, result.retry_after)
        return result
