"""DataUpdateCoordinator for ASFES Multiplex integration."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import AsfesMultiplexApi
from .const import DOMAIN, OPT_POLL_INTERVAL, OPT_POLL_INTERVAL_DEFAULT
from .exceptions import AuthRequired, CannotConnect

_LOGGER = logging.getLogger(__name__)


@dataclass
class AsfesMultiplexData:
    """Data container for coordinator."""

    sensors: dict[str, Any]
    binary_sensors: dict[str, Any]
    switches: dict[str, Any]
    meta: dict[str, Any]


class AsfesMultiplexCoordinator(DataUpdateCoordinator[AsfesMultiplexData]):
    """Centralized polling coordinator for all ASFES Multiplex entities."""

    config_entry: ConfigEntry

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        api: AsfesMultiplexApi,
    ) -> None:
        poll_interval = entry.options.get(
            OPT_POLL_INTERVAL, OPT_POLL_INTERVAL_DEFAULT
        )
        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN}_{entry.unique_id}",
            config_entry=entry,
            update_interval=timedelta(seconds=poll_interval),
            always_update=False,
        )
        self.api = api
        self.serial: str = entry.data.get("serial", "")

    async def _async_update_data(self) -> AsfesMultiplexData:
        """Fetch state from ASFES Multiplex (single HTTP request)."""
        try:
            async with asyncio.timeout(15):
                state = await self.api.get_state()
        except AuthRequired as err:
            raise ConfigEntryAuthFailed(
                "HA token expired and refresh failed"
            ) from err
        except CannotConnect as err:
            raise UpdateFailed(f"Cannot reach ASFES Multiplex: {err}") from err

        return AsfesMultiplexData(
            sensors=state.get("sensors", {}),
            binary_sensors=state.get("binary_sensors", {}),
            switches=state.get("switches", {}),
            meta=state.get("meta", {}),
        )
