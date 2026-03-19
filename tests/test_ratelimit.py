from __future__ import annotations

import pytest

from server.core.ratelimit import RateLimitError, RateLimitPolicy, RateLimiter


@pytest.mark.asyncio
async def test_memory_rate_limiter_blocks_after_limit() -> None:
    limiter = RateLimiter({"login": RateLimitPolicy("login", 2, 60)})
    await limiter.enforce("login", "127.0.0.1:root")
    await limiter.enforce("login", "127.0.0.1:root")
    with pytest.raises(RateLimitError):
        await limiter.enforce("login", "127.0.0.1:root")
