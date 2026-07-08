"""Diagnostics for ASFES Multiplex integration."""

from __future__ import annotations

from typing import Any

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .coordinator import AsfesMultiplexCoordinator

TO_REDACT = {"access_token", "refresh_token", "serial", "hostname"}


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> dict[str, Any]:
    """Return diagnostics for a config entry."""
    coordinator: AsfesMultiplexCoordinator = entry.runtime_data
    try:
        diag = await coordinator.api.get_diagnostics()
    except Exception:  # noqa: BLE001
        diag = {"error": "Failed to retrieve diagnostics"}

    return async_redact_data(
        {
            "entry": async_redact_data(dict(entry.data), TO_REDACT),
            "diagnostics": diag,
            "coordinator_last_update_success": coordinator.last_update_success,
        },
        TO_REDACT,
    )
