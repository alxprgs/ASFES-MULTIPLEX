"""
Proxy routing layer — pure transport concern.
Decoupled from download logic so each layer stays focused.
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Literal

import aiohttp

logger = logging.getLogger("multiplex.python_mirror.proxy")

NetworkMode = Literal["direct", "proxy", "mix", "fallback"]


class ProxyRouter:
    """
    Manages proxy selection strategy for outbound HTTP requests.

    Modes:
      direct   — no proxy used
      proxy    — random choice from proxies list
      mix      — random: proxy or direct
      fallback — try each proxy in sequence, then direct
    """

    def __init__(
        self,
        proxies: list[str],
        mode: NetworkMode = "direct",
        verify_ssl: bool = True,
    ) -> None:
        self._proxies = [p.strip() for p in proxies if p.strip()]
        self._mode = mode
        self._verify_ssl = verify_ssl

    def choose_proxy(self) -> str | None:
        """Return a single proxy for non-fallback modes."""
        if self._mode == "direct" or not self._proxies:
            return None
        if self._mode == "proxy":
            return random.choice(self._proxies)
        if self._mode == "mix":
            pool = [*self._proxies, None]
            return random.choice(pool)
        # fallback: use first proxy as initial candidate
        return self._proxies[0] if self._proxies else None

    def get_fallback_candidates(self) -> list[str | None]:
        """
        Return ordered proxy list for fallback mode.
        Always ends with None (direct connection) as last resort.
        """
        if self._mode == "fallback" and self._proxies:
            return [*self._proxies, None]
        # for other modes — single candidate
        return [self.choose_proxy()]

    async def get(
        self,
        session: aiohttp.ClientSession,
        url: str,
        **kwargs,
    ) -> aiohttp.ClientResponse:
        """
        Perform GET with proxy fallback.
        Returns the first successful ClientResponse.
        Caller is responsible for closing the response (use as context manager).
        Raises RuntimeError if all candidates fail.
        """
        return await self._request(session, "GET", url, **kwargs)

    async def head(
        self,
        session: aiohttp.ClientSession,
        url: str,
        **kwargs,
    ) -> aiohttp.ClientResponse:
        """Perform HEAD with proxy fallback."""
        return await self._request(session, "HEAD", url, **kwargs)

    async def _request(
        self,
        session: aiohttp.ClientSession,
        method: str,
        url: str,
        **kwargs,
    ) -> aiohttp.ClientResponse:
        candidates = self.get_fallback_candidates()
        last_exc: Exception | None = None
        for proxy in candidates:
            try:
                resp = await session._request(  # noqa: SLF001  — aiohttp internal
                    method,
                    url,
                    proxy=proxy,
                    ssl=self._verify_ssl,
                    **kwargs,
                )
                return resp
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                logger.debug(
                    "proxy_attempt_failed method=%s proxy=%s url=%s error=%s",
                    method,
                    proxy,
                    url,
                    exc,
                )
                last_exc = exc
        raise RuntimeError(
            f"All {len(candidates)} connection attempt(s) failed for {url}"
        ) from last_exc
