import asyncio
import pytest
from server.core.cache import CacheManager

@pytest.mark.asyncio
async def test_cache_memory_fallback():
    # When redis is disabled, CacheManager should fallback to in-memory dict
    cache = CacheManager(redis_mode="disabled")
    await cache.connect()
    
    assert await cache.get("test_key") is None
    
    await cache.set("test_key", {"data": 123}, ttl_seconds=10)
    assert await cache.get("test_key") == {"data": 123}
    
    await cache.delete("test_key")
    assert await cache.get("test_key") is None

@pytest.mark.asyncio
async def test_cache_memory_ttl():
    cache = CacheManager(redis_mode="disabled")
    await cache.connect()
    
    # TTL of 0 seconds means it expires immediately
    await cache.set("fast_expire", "val", ttl_seconds=0)
    
    # Needs a tiny sleep to ensure monotonic time progresses
    await asyncio.sleep(0.01)
    assert await cache.get("fast_expire") is None

@pytest.mark.asyncio
async def test_cache_delete_prefix():
    cache = CacheManager(redis_mode="disabled")
    await cache.connect()
    
    await cache.set("pypi:html:pkg:flask", "html1", ttl_seconds=10)
    await cache.set("pypi:html:pkg:django", "html2", ttl_seconds=10)
    await cache.set("pypi:meta:flask", "meta", ttl_seconds=10)
    
    await cache.delete_prefix("pypi:html:pkg:")
    
    assert await cache.get("pypi:html:pkg:flask") is None
    assert await cache.get("pypi:html:pkg:django") is None
    assert await cache.get("pypi:meta:flask") == "meta"
