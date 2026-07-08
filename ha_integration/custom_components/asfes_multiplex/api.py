"""HTTP client for ASFES Multiplex HA API.

Single httpx.AsyncClient instance per config entry, with proactive token refresh.
"""

from __future__ import annotations

import time
from typing import Any

import httpx

from .const import (
    API_AUTH,
    API_AUTH_2FA,
    API_BUTTON,
    API_DIAGNOSTICS,
    API_HEALTH,
    API_REFRESH,
    API_STATE,
    API_SWITCH,
)
from .exceptions import AuthRequired, CannotConnect, InvalidAuth, InvalidTotp

# Refresh access token if less than 60 seconds until expiry
_REFRESH_BUFFER_SECONDS = 60


class AsfesMultiplexApi:
    """Async HTTP client for ASFES Multiplex HA API."""

    def __init__(
        self,
        host: str,
        access_token: str,
        refresh_token: str,
        access_token_expires_in: int,
    ) -> None:
        self._host = host.rstrip("/")
        self._access_token = access_token
        self._refresh_token = refresh_token
        self._access_token_exp = time.time() + access_token_expires_in
        self._client = httpx.AsyncClient(
            base_url=self._host,
            timeout=15.0,
        )

    @property
    def access_token(self) -> str:
        return self._access_token

    @property
    def refresh_token(self) -> str:
        return self._refresh_token

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        await self._client.aclose()

    def _auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._access_token}"}

    async def ensure_token_fresh(self) -> None:
        """Refresh access token proactively if close to expiry."""
        if self._access_token_exp - time.time() > _REFRESH_BUFFER_SECONDS:
            return
        await self._do_refresh()

    async def _do_refresh(self) -> None:
        """Perform token refresh. Raises AuthRequired on failure."""
        try:
            resp = await self._client.post(
                API_REFRESH,
                json={"refresh_token": self._refresh_token},
            )
            if resp.status_code == 401:
                raise AuthRequired("Refresh token rejected by server")
            resp.raise_for_status()
            data = resp.json()
            self._access_token = data["access_token"]
            self._refresh_token = data["refresh_token"]
            self._access_token_exp = time.time() + data.get("expires_in", 1800)
        except AuthRequired:
            raise
        except httpx.HTTPStatusError as err:
            raise AuthRequired(f"Token refresh failed: {err}") from err
        except httpx.RequestError as err:
            raise CannotConnect(f"Network error during token refresh: {err}") from err

    async def check_health(self) -> bool:
        """Check server health. Returns True if healthy."""
        try:
            resp = await self._client.get(API_HEALTH)
            return resp.status_code == 200
        except httpx.RequestError:
            return False

    async def authenticate(
        self, username: str, password: str, label: str
    ) -> dict[str, Any]:
        """POST /api/ha/auth/token — returns raw response dict."""
        try:
            resp = await self._client.post(
                API_AUTH,
                json={"username": username, "password": password, "account_label": label},
            )
            if resp.status_code == 401:
                raise InvalidAuth(resp.json().get("detail", "Invalid credentials"))
            resp.raise_for_status()
            return resp.json()
        except (InvalidAuth, InvalidTotp):
            raise
        except httpx.HTTPStatusError as err:
            raise CannotConnect(f"HTTP error: {err}") from err
        except httpx.RequestError as err:
            raise CannotConnect(f"Cannot connect: {err}") from err

    async def authenticate_2fa(
        self, challenge_token: str, totp_code: str
    ) -> dict[str, Any]:
        """POST /api/ha/auth/token/2fa — complete TOTP challenge."""
        try:
            resp = await self._client.post(
                API_AUTH_2FA,
                json={"challenge_token": challenge_token, "totp_code": totp_code},
            )
            if resp.status_code == 401:
                raise InvalidTotp(resp.json().get("detail", "Invalid TOTP code"))
            resp.raise_for_status()
            return resp.json()
        except (InvalidTotp,):
            raise
        except httpx.HTTPStatusError as err:
            raise CannotConnect(f"HTTP error: {err}") from err
        except httpx.RequestError as err:
            raise CannotConnect(f"Cannot connect: {err}") from err

    async def get_state(self) -> dict[str, Any]:
        """GET /api/ha/state — single polling endpoint."""
        await self.ensure_token_fresh()
        try:
            resp = await self._client.get(
                API_STATE, headers=self._auth_headers()
            )
            if resp.status_code == 401:
                raise AuthRequired("Access token rejected")
            resp.raise_for_status()
            return resp.json()
        except AuthRequired:
            raise
        except httpx.HTTPStatusError as err:
            raise CannotConnect(f"HTTP error: {err}") from err
        except httpx.RequestError as err:
            raise CannotConnect(f"Cannot connect: {err}") from err

    async def get_diagnostics(self) -> dict[str, Any]:
        """GET /api/ha/diagnostics — on-demand only."""
        await self.ensure_token_fresh()
        try:
            resp = await self._client.get(
                API_DIAGNOSTICS, headers=self._auth_headers()
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as err:
            raise CannotConnect(f"HTTP error: {err}") from err
        except httpx.RequestError as err:
            raise CannotConnect(f"Cannot connect: {err}") from err

    async def set_switch(self, name: str, value: bool) -> dict[str, Any]:
        """POST /api/ha/switches/{name}."""
        await self.ensure_token_fresh()
        try:
            resp = await self._client.post(
                API_SWITCH.format(name=name),
                headers=self._auth_headers(),
                json={"value": value},
            )
            if resp.status_code == 401:
                raise AuthRequired("Access token rejected")
            resp.raise_for_status()
            return resp.json()
        except AuthRequired:
            raise
        except httpx.HTTPStatusError as err:
            raise CannotConnect(f"HTTP error: {err}") from err
        except httpx.RequestError as err:
            raise CannotConnect(f"Cannot connect: {err}") from err

    async def press_button(self, name: str) -> dict[str, Any]:
        """POST /api/ha/buttons/{name}."""
        await self.ensure_token_fresh()
        try:
            resp = await self._client.post(
                API_BUTTON.format(name=name),
                headers=self._auth_headers(),
            )
            if resp.status_code == 401:
                raise AuthRequired("Access token rejected")
            resp.raise_for_status()
            return resp.json()
        except AuthRequired:
            raise
        except httpx.HTTPStatusError as err:
            raise CannotConnect(f"HTTP error: {err}") from err
        except httpx.RequestError as err:
            raise CannotConnect(f"Cannot connect: {err}") from err


async def create_api_from_credentials(
    host: str,
    username: str,
    password: str,
    label: str,
) -> tuple["AsfesMultiplexApi", dict[str, Any]]:
    """Create API client by authenticating. Returns (api, auth_response_data)."""
    # Temporary unauthenticated client for auth
    async with httpx.AsyncClient(base_url=host.rstrip("/"), timeout=15.0) as client:
        try:
            resp = await client.post(
                API_AUTH,
                json={"username": username, "password": password, "account_label": label},
            )
            if resp.status_code == 401:
                raise InvalidAuth(resp.json().get("detail", "Invalid credentials"))
            if resp.status_code == 503:
                raise CannotConnect("HA integration is disabled on the server")
            resp.raise_for_status()
            data = resp.json()
        except (InvalidAuth,):
            raise
        except httpx.HTTPStatusError as err:
            raise CannotConnect(f"HTTP error: {err}") from err
        except httpx.RequestError as err:
            raise CannotConnect(f"Cannot connect to {host}: {err}") from err

    if data.get("challenge_required"):
        # Caller must handle 2FA step
        return None, data  # type: ignore[return-value]

    api = AsfesMultiplexApi(
        host=host,
        access_token=data["access_token"],
        refresh_token=data["refresh_token"],
        access_token_expires_in=data.get("expires_in", 1800),
    )
    return api, data
