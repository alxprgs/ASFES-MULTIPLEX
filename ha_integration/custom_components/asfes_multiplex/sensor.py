"""Sensor entities for ASFES Multiplex integration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    PERCENTAGE,
    UnitOfInformation,
    UnitOfTemperature,
    UnitOfTime,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.typing import StateType

from .coordinator import AsfesMultiplexCoordinator, AsfesMultiplexData
from .entity import AsfesMultiplexEntity


@dataclass(frozen=True)
class AsfesMultiplexSensorEntityDescription(SensorEntityDescription):
    """Describes an ASFES Multiplex sensor."""

    value_fn: Callable[[AsfesMultiplexData], StateType] = lambda _: None


SENSOR_DESCRIPTIONS: tuple[AsfesMultiplexSensorEntityDescription, ...] = (
    AsfesMultiplexSensorEntityDescription(
        key="cpu_usage",
        name="CPU Usage",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:cpu-64-bit",
        value_fn=lambda d: d.sensors.get("cpu_usage"),
    ),
    AsfesMultiplexSensorEntityDescription(
        key="ram_usage",
        name="RAM Usage",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:memory",
        value_fn=lambda d: d.sensors.get("ram_usage"),
    ),
    AsfesMultiplexSensorEntityDescription(
        key="disk_usage",
        name="Disk Usage",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:harddisk",
        value_fn=lambda d: d.sensors.get("disk_usage"),
    ),
    AsfesMultiplexSensorEntityDescription(
        key="uptime",
        name="Uptime",
        native_unit_of_measurement=UnitOfTime.SECONDS,
        device_class=SensorDeviceClass.DURATION,
        state_class=SensorStateClass.TOTAL_INCREASING,
        icon="mdi:timer-outline",
        value_fn=lambda d: d.sensors.get("uptime_seconds"),
    ),
    AsfesMultiplexSensorEntityDescription(
        key="network_rx",
        name="Network RX",
        native_unit_of_measurement=UnitOfInformation.BYTES,
        device_class=SensorDeviceClass.DATA_SIZE,
        state_class=SensorStateClass.TOTAL_INCREASING,
        icon="mdi:download-network-outline",
        value_fn=lambda d: d.sensors.get("network_rx_bytes"),
    ),
    AsfesMultiplexSensorEntityDescription(
        key="network_tx",
        name="Network TX",
        native_unit_of_measurement=UnitOfInformation.BYTES,
        device_class=SensorDeviceClass.DATA_SIZE,
        state_class=SensorStateClass.TOTAL_INCREASING,
        icon="mdi:upload-network-outline",
        value_fn=lambda d: d.sensors.get("network_tx_bytes"),
    ),
    AsfesMultiplexSensorEntityDescription(
        key="temperature",
        name="Temperature",
        native_unit_of_measurement=UnitOfTemperature.CELSIUS,
        device_class=SensorDeviceClass.TEMPERATURE,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.sensors.get("temperature"),
    ),
    AsfesMultiplexSensorEntityDescription(
        key="docker_containers_running",
        name="Docker Containers Running",
        native_unit_of_measurement="containers",
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:docker",
        value_fn=lambda d: d.sensors.get("docker_containers_running"),
    ),
    AsfesMultiplexSensorEntityDescription(
        key="running_processes",
        name="Running Processes",
        native_unit_of_measurement="processes",
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:application-cog-outline",
        value_fn=lambda d: d.sensors.get("running_processes"),
    ),
    AsfesMultiplexSensorEntityDescription(
        key="redis_clients",
        name="Redis Connected Clients",
        native_unit_of_measurement="clients",
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:database-clock-outline",
        value_fn=lambda d: d.sensors.get("redis_connected_clients"),
    ),
    AsfesMultiplexSensorEntityDescription(
        key="mongo_clients",
        name="MongoDB Connected Clients",
        native_unit_of_measurement="clients",
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:leaf",
        value_fn=lambda d: d.sensors.get("mongo_connected_clients"),
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up sensor entities."""
    coordinator: AsfesMultiplexCoordinator = entry.runtime_data
    async_add_entities(
        AsfesMultiplexSensor(coordinator, description)
        for description in SENSOR_DESCRIPTIONS
    )


class AsfesMultiplexSensor(AsfesMultiplexEntity, SensorEntity):
    """A sensor entity for ASFES Multiplex."""

    entity_description: AsfesMultiplexSensorEntityDescription

    def __init__(
        self,
        coordinator: AsfesMultiplexCoordinator,
        description: AsfesMultiplexSensorEntityDescription,
    ) -> None:
        super().__init__(coordinator, description.key)
        self.entity_description = description

    @property
    def native_value(self) -> StateType:
        """Return the sensor value."""
        return self.entity_description.value_fn(self.coordinator.data)
