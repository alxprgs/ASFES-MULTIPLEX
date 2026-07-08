"""ASFES Multiplex Home Assistant integration."""

from __future__ import annotations

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers import config_validation as cv

from .api import AsfesMultiplexApi
from .const import (
    CONF_ACCESS_TOKEN,
    CONF_HOST,
    CONF_REFRESH_TOKEN,
    DOMAIN,
)
from .coordinator import AsfesMultiplexCoordinator

CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)

PLATFORMS = ["sensor", "binary_sensor", "switch", "button"]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up ASFES Multiplex from a config entry."""
    api = AsfesMultiplexApi(
        host=entry.data[CONF_HOST],
        access_token=entry.data[CONF_ACCESS_TOKEN],
        refresh_token=entry.data[CONF_REFRESH_TOKEN],
        access_token_expires_in=0,  # Will refresh on first poll
    )

    coordinator = AsfesMultiplexCoordinator(hass, entry, api)
    await coordinator.async_config_entry_first_refresh()

    entry.runtime_data = coordinator
    entry.async_on_unload(entry.add_update_listener(_async_options_listener))

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    coordinator: AsfesMultiplexCoordinator = entry.runtime_data
    await coordinator.api.close()
    return await hass.config_entries.async_unload_platforms(entry, PLATFORMS)


async def _async_options_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Handle options update (e.g., poll interval changed)."""
    await hass.config_entries.async_reload(entry.entry_id)
