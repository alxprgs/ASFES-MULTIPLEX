"""Switch entities for ASFES Multiplex integration.

Switches are only created when HA__SWITCHES_ENABLED=true on the server
(i.e., the server returns non-None values in the switches response).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from homeassistant.components.switch import SwitchEntity, SwitchEntityDescription
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .coordinator import AsfesMultiplexCoordinator, AsfesMultiplexData
from .entity import AsfesMultiplexEntity


@dataclass(frozen=True)
class AsfesMultiplexSwitchEntityDescription(SwitchEntityDescription):
    """Describes an ASFES Multiplex switch."""

    value_fn: Callable[[AsfesMultiplexData], bool | None] = lambda _: None
    api_name: str = ""


SWITCH_DESCRIPTIONS: tuple[AsfesMultiplexSwitchEntityDescription, ...] = (
    AsfesMultiplexSwitchEntityDescription(
        key="enable_registration",
        name="Enable Registration",
        icon="mdi:account-plus",
        api_name="enable_registration",
        value_fn=lambda d: d.switches.get("enable_registration"),
    ),
    AsfesMultiplexSwitchEntityDescription(
        key="enable_mcp",
        name="Enable MCP",
        icon="mdi:protocol",
        api_name="enable_mcp",
        value_fn=lambda d: d.switches.get("enable_mcp"),
    ),
    AsfesMultiplexSwitchEntityDescription(
        key="enable_redis",
        name="Enable Redis",
        icon="mdi:database-sync",
        api_name="enable_redis",
        value_fn=lambda d: d.switches.get("enable_redis"),
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up switch entities — only those present in coordinator data."""
    coordinator: AsfesMultiplexCoordinator = entry.runtime_data
    entities = [
        AsfesMultiplexSwitch(coordinator, description)
        for description in SWITCH_DESCRIPTIONS
        if description.value_fn(coordinator.data) is not None
    ]
    async_add_entities(entities)


class AsfesMultiplexSwitch(AsfesMultiplexEntity, SwitchEntity):
    """A switch entity for ASFES Multiplex."""

    entity_description: AsfesMultiplexSwitchEntityDescription

    def __init__(
        self,
        coordinator: AsfesMultiplexCoordinator,
        description: AsfesMultiplexSwitchEntityDescription,
    ) -> None:
        super().__init__(coordinator, description.key)
        self.entity_description = description

    @property
    def is_on(self) -> bool:
        """Return True if switch is on."""
        return bool(self.entity_description.value_fn(self.coordinator.data))

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Turn the switch on."""
        await self.coordinator.api.set_switch(self.entity_description.api_name, True)
        await self.coordinator.async_request_refresh()

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Turn the switch off."""
        await self.coordinator.api.set_switch(self.entity_description.api_name, False)
        await self.coordinator.async_request_refresh()
