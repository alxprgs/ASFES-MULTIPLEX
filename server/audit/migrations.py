from typing import Any


class SchemaMigrationRegistry:
    def __init__(self) -> None:
        self._transformers: dict[tuple[int, int], callable] = {}

    def register(self, from_version: int, to_version: int):
        def decorator(func: callable):
            self._transformers[(from_version, to_version)] = func
            return func

        return decorator

    def upgrade(
        self, event_dict: dict[str, Any], target_version: int = 1
    ) -> dict[str, Any]:
        """Upgrades an event dictionary to the target schema version."""
        current_version = event_dict.get("schema_version", 1)
        while current_version < target_version:
            transformer = self._transformers.get((current_version, current_version + 1))
            if not transformer:
                raise ValueError(
                    f"No migration path from schema version {current_version} to {current_version + 1}"
                )
            event_dict = transformer(event_dict)
            current_version += 1
            event_dict["schema_version"] = current_version
        return event_dict


migration_registry = SchemaMigrationRegistry()

# Example future migration:
# @migration_registry.register(1, 2)
# def v1_to_v2(event: dict[str, Any]) -> dict[str, Any]:
#     # Transform logic here
#     return event
