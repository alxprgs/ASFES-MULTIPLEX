from __future__ import annotations

import tomllib

from server.core.config import BASE_DIR, Settings


def test_app_version_defaults_to_pyproject() -> None:
    with (BASE_DIR / "pyproject.toml").open("rb") as pyproject_file:
        project_data = tomllib.load(pyproject_file)

    cfg = Settings(_env_file=None)

    assert cfg.app.version == project_data["project"]["version"]


def test_app_version_can_be_overridden_by_environment(monkeypatch) -> None:
    monkeypatch.setenv("APP__VERSION", "9.8.7")

    cfg = Settings(_env_file=None)

    assert cfg.app.version == "9.8.7"
