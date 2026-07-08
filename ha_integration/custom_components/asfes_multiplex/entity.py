"""Base entity for ASFES Multiplex integration."""

from __future__ import annotations

from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import CONF_ACCOUNT_LABEL, CONF_HOST, DOMAIN, MANUFACTURER
from .coordinator import AsfesMultiplexCoordinator


class AsfesMultiplexEntity(CoordinatorEntity[AsfesMultiplexCoordinator]):
    """Base entity for ASFES Multiplex integration."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: AsfesMultiplexCoordinator,
        key: str,
    ) -> None:
        super().__init__(coordinator)
        self._key = key
        self._attr_unique_id = f"{coordinator.serial}_{key}"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, coordinator.serial)},
            name=f"ASFES Multiplex ({coordinator.config_entry.data.get(CONF_ACCOUNT_LABEL, 'Multiplex')})",
            manufacturer=MANUFACTURER,
            model="Multiplex Control Plane",
            configuration_url=coordinator.config_entry.data.get(CONF_HOST),
        )

    @property
    def available(self) -> bool:
        """Entity is unavailable if coordinator last update failed."""
        return self.coordinator.last_update_success
