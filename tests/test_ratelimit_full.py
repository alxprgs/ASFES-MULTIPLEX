from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from server.core.ratelimit import (
    RateLimitError,
    RateLimitPolicy,
    RateLimitResult,
    RateLimiter,
    MemoryRateLimiterBackend,
    RedisRateLimiterBackend,
)


@pytest.mark.asyncio
async def test_memory_rate_limiter_expiry(monkeypatch) -> None:
    backend = MemoryRateLimiterBackend()
    policy = RateLimitPolicy("test", limit=2, window_seconds=10)

    fake_now = 1000.0
    monkeypatch.setattr(time, "time", lambda: fake_now)

    # First consume
    res1 = await backend.consume("user1", policy)
    assert res1.allowed is True
    assert res1.remaining == 1
    assert res1.retry_after == 0

    # Second consume
    res2 = await backend.consume("user1", policy)
    assert res2.allowed is True
    assert res2.remaining == 0

    # Third consume (limit exceeded)
    res3 = await backend.consume("user1", policy)
    assert res3.allowed is False
    assert res3.remaining == 0
    assert res3.retry_after == 10

    # Advance time beyond window
    fake_now += 11.0
    res4 = await backend.consume("user1", policy)
    assert res4.allowed is True
    assert res4.remaining == 1


@pytest.mark.asyncio
async def test_redis_rate_limiter_flow() -> None:
    backend = RedisRateLimiterBackend(redis_url="redis://localhost:6379")
    mock_redis = MagicMock()
    mock_redis.incr = AsyncMock(return_value=1)
    mock_redis.expire = AsyncMock()
    mock_redis.ttl = AsyncMock(return_value=5)
    backend._redis = mock_redis

    policy = RateLimitPolicy("login", limit=2, window_seconds=10)
    res = await backend.consume("user1", policy)

    assert res.allowed is True
    assert res.remaining == 1
    assert res.retry_after == 0
    mock_redis.incr.assert_called_once_with("multiplex:ratelimit:login:user1")
    mock_redis.expire.assert_called_once_with("multiplex:ratelimit:login:user1", 10)

    # Limit exceeded scenario
    mock_redis.incr.return_value = 3
    res_exceeded = await backend.consume("user1", policy)
    assert res_exceeded.allowed is False
    assert res_exceeded.remaining == 0
    assert res_exceeded.retry_after == 5


@pytest.mark.asyncio
async def test_redis_rate_limiter_connection() -> None:
    backend = RedisRateLimiterBackend(redis_url="redis://localhost:6379")
    # Stub connection methods
    backend.connect = AsyncMock()
    backend.close = AsyncMock()
    backend._redis = MagicMock()
    backend._redis.incr = AsyncMock(return_value=1)
    backend._redis.expire = AsyncMock()
    backend._redis.ttl = AsyncMock(return_value=5)

    policy = RateLimitPolicy("login", limit=2, window_seconds=10)
    await backend.consume("user1", policy)
    await backend.close()


@pytest.mark.asyncio
async def test_rate_limiter_switching() -> None:
    policies = {"login": RateLimitPolicy("login", 2, 60)}
    limiter = RateLimiter(
        policies=policies,
        redis_mode="runtime",
        redis_url="redis://localhost:6379",
        redis_runtime_enabled=False,
    )

    assert limiter.should_use_redis() is False

    # Mock Redis connect/close
    limiter._redis = MagicMock()
    limiter._redis.connect = AsyncMock()
    limiter._redis.close = AsyncMock()
    limiter._redis.consume = AsyncMock(return_value=RateLimitResult(True, 2, 1, 0, 60))

    await limiter.set_runtime_enabled(True)
    assert limiter.should_use_redis() is True

    # Consuming should route to redis now
    res = await limiter.consume("login", "user1")
    assert res.allowed is True
    limiter._redis.consume.assert_called_once()

    await limiter.set_runtime_enabled(False)
    assert limiter.should_use_redis() is False

    # For required mode, disabling raises ValueError
    limiter_req = RateLimiter(policies=policies, redis_mode="required", redis_url="redis://localhost:6379")
    with pytest.raises(ValueError):
        await limiter_req.set_runtime_enabled(False)


@pytest.mark.asyncio
async def test_rate_limiter_enforce_exception() -> None:
    policies = {"login": RateLimitPolicy("login", 1, 60)}
    limiter = RateLimiter(policies=policies)

    # First succeeds
    await limiter.enforce("login", "user1")

    # Second fails
    with pytest.raises(RateLimitError) as exc_info:
        await limiter.enforce("login", "user1")
    assert exc_info.value.policy_name == "login"
    assert exc_info.value.retry_after > 0
