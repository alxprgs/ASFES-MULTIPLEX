"""Binary sensor entities for ASFES Multiplex integration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
    BinarySensorEntityDescription,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .coordinator import AsfesMultiplexCoordinator, AsfesMultiplexData
from .entity import AsfesMultiplexEntity


@dataclass(frozen=True)
class AsfesMultiplexBinarySensorEntityDescription(BinarySensorEntityDescription):
    """Describes an ASFES Multiplex binary sensor."""

    value_fn: Callable[[AsfesMultiplexData], bool] = lambda _: False


BINARY_SENSOR_DESCRIPTIONS: tuple[
    AsfesMultiplexBinarySensorEntityDescription, ...
] = (
    AsfesMultiplexBinarySensorEntityDescription(
        key="mongodb_online",
        name="MongoDB Online",
        device_class=BinarySensorDeviceClass.CONNECTIVITY,
        icon="mdi:database-check",
        value_fn=lambda d: bool(d.binary_sensors.get("mongodb_online")),
    ),
    AsfesMultiplexBinarySensorEntityDescription(
        key="redis_online",
        name="Redis Online",
        device_class=BinarySensorDeviceClass.CONNECTIVITY,
        icon="mdi:database-sync",
        value_fn=lambda d: bool(d.binary_sensors.get("redis_online")),
    ),
    AsfesMultiplexBinarySensorEntityDescription(
        key="api_healthy",
        name="API Healthy",
        device_class=BinarySensorDeviceClass.RUNNING,
        icon="mdi:api",
        value_fn=lambda d: bool(d.binary_sensors.get("api_healthy")),
    ),
    AsfesMultiplexBinarySensorEntityDescription(
        key="mcp_healthy",
        name="MCP Healthy",
        device_class=BinarySensorDeviceClass.RUNNING,
        icon="mdi:protocol",
        value_fn=lambda d: bool(d.binary_sensors.get("mcp_healthy")),
    ),
    AsfesMultiplexBinarySensorEntityDescription(
        key="python_mirror_running",
        name="Python Mirror Running",
        device_class=BinarySensorDeviceClass.RUNNING,
        icon="mdi:language-python",
        value_fn=lambda d: bool(d.binary_sensors.get("python_mirror_running")),
    ),
    AsfesMultiplexBinarySensorEntityDescription(
        key="pypi_mirror_running",
        name="PyPI Mirror Running",
        device_class=BinarySensorDeviceClass.RUNNING,
        icon="mdi:package-variant",
        value_fn=lambda d: bool(d.binary_sensors.get("pypi_mirror_running")),
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up binary sensor entities."""
    coordinator: AsfesMultiplexCoordinator = entry.runtime_data
    async_add_entities(
        AsfesMultiplexBinarySensor(coordinator, description)
        for description in BINARY_SENSOR_DESCRIPTIONS
    )


class AsfesMultiplexBinarySensor(AsfesMultiplexEntity, BinarySensorEntity):
    """A binary sensor entity for ASFES Multiplex."""

    entity_description: AsfesMultiplexBinarySensorEntityDescription

    def __init__(
        self,
        coordinator: AsfesMultiplexCoordinator,
        description: AsfesMultiplexBinarySensorEntityDescription,
    ) -> None:
        super().__init__(coordinator, description.key)
        self.entity_description = description

    @property
    def is_on(self) -> bool:
        """Return True if binary sensor is on."""
        return self.entity_description.value_fn(self.coordinator.data)
