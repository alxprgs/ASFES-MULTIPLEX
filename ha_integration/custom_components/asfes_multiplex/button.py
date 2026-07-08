"""Button entities for ASFES Multiplex integration.

Admin buttons (restart_multiplex, restart_docker) are only created
when meta.destructive_buttons_enabled is True in the server response.
"""

from __future__ import annotations

from dataclasses import dataclass

from homeassistant.components.button import ButtonEntity, ButtonEntityDescription
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .coordinator import AsfesMultiplexCoordinator
from .entity import AsfesMultiplexEntity


@dataclass(frozen=True)
class AsfesMultiplexButtonEntityDescription(ButtonEntityDescription):
    """Describes an ASFES Multiplex button."""

    api_name: str = ""
    requires_destructive: bool = False


BUTTON_DESCRIPTIONS: tuple[AsfesMultiplexButtonEntityDescription, ...] = (
    AsfesMultiplexButtonEntityDescription(
        key="restart_multiplex",
        name="Restart Multiplex",
        icon="mdi:restart",
        api_name="restart_multiplex",
        requires_destructive=True,
    ),
    AsfesMultiplexButtonEntityDescription(
        key="restart_docker",
        name="Restart Docker",
        icon="mdi:docker",
        api_name="restart_docker",
        requires_destructive=True,
    ),
    AsfesMultiplexButtonEntityDescription(
        key="reload_plugins",
        name="Reload Plugins",
        icon="mdi:puzzle-outline",
        api_name="reload_plugins",
    ),
    AsfesMultiplexButtonEntityDescription(
        key="refresh_python_mirror",
        name="Refresh Python Mirror",
        icon="mdi:language-python",
        api_name="refresh_python_mirror",
    ),
    AsfesMultiplexButtonEntityDescription(
        key="refresh_pypi",
        name="Refresh PyPI",
        icon="mdi:package-variant",
        api_name="refresh_pypi",
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up button entities."""
    coordinator: AsfesMultiplexCoordinator = entry.runtime_data
    destructive_enabled = coordinator.data.meta.get("destructive_buttons_enabled", False)
    entities = [
        AsfesMultiplexButton(coordinator, description)
        for description in BUTTON_DESCRIPTIONS
        if not description.requires_destructive or destructive_enabled
    ]
    async_add_entities(entities)


class AsfesMultiplexButton(AsfesMultiplexEntity, ButtonEntity):
    """A button entity for ASFES Multiplex."""

    entity_description: AsfesMultiplexButtonEntityDescription

    def __init__(
        self,
        coordinator: AsfesMultiplexCoordinator,
        description: AsfesMultiplexButtonEntityDescription,
    ) -> None:
        super().__init__(coordinator, description.key)
        self.entity_description = description

    async def async_press(self) -> None:
        """Press the button."""
        await self.coordinator.api.press_button(self.entity_description.api_name)
