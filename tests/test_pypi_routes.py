"""
Unit tests for the PyPI mirror routes and core logic.
Uses mock services — no real MongoDB or network required.
"""

from __future__ import annotations

import pytest

from server.core.pypi_mirror import normalize_package_name


# ---------------------------------------------------------------------------
# PEP 503 normalization
# ---------------------------------------------------------------------------


def test_normalize_simple():
    assert normalize_package_name("flask") == "flask"


def test_normalize_uppercase():
    assert normalize_package_name("Flask") == "flask"


def test_normalize_underscores():
    assert normalize_package_name("flask_cors") == "flask-cors"


def test_normalize_mixed():
    assert normalize_package_name("Flask-Cors") == "flask-cors"


def test_normalize_dots():
    assert normalize_package_name("zope.interface") == "zope-interface"


def test_normalize_multiple_separators():
    assert normalize_package_name("My__Package.Name-1") == "my-package-name-1"


def test_normalize_already_normalized():
    assert normalize_package_name("flask-cors") == "flask-cors"


# ---------------------------------------------------------------------------
# PyPIMirrorService unit tests (no MongoDB)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_is_blocked_checks_package(tmp_path):
    """is_blocked returns True when package is in blocked_packages list."""
    from unittest.mock import AsyncMock, MagicMock
    from server.core.config import PyPIConfig
    from server.pypi_service import PyPIMirrorService

    config = PyPIConfig(data_dir=tmp_path / "pypi_storage")
    db = MagicMock()
    db.collection = MagicMock(return_value=MagicMock())
    audit = MagicMock()
    audit.record = AsyncMock()

    service = PyPIMirrorService(config=config, db=db, audit=audit)

    # Patch _get_blocklist_doc to return a blocked package
    service._get_blocklist_doc = AsyncMock(
        return_value={
            "_id": "blocklist",
            "blocked_packages": ["flask"],
            "blocked_versions": {},
        }
    )

    assert await service.is_blocked("Flask") is True  # normalizes to flask
    assert await service.is_blocked("flask-cors") is False  # different package


@pytest.mark.asyncio
async def test_is_blocked_checks_version(tmp_path):
    """is_blocked returns True when a specific version is blocked."""
    from unittest.mock import AsyncMock, MagicMock
    from server.core.config import PyPIConfig
    from server.pypi_service import PyPIMirrorService

    config = PyPIConfig(data_dir=tmp_path / "pypi_storage")
    service = PyPIMirrorService(config=config, db=MagicMock(), audit=MagicMock())

    service._get_blocklist_doc = AsyncMock(
        return_value={
            "_id": "blocklist",
            "blocked_packages": [],
            "blocked_versions": {"flask": ["0.12.0", "1.0.0"]},
        }
    )

    assert await service.is_blocked("flask", "0.12.0") is True
    assert await service.is_blocked("flask", "2.0.0") is False
    assert await service.is_blocked("flask") is False  # whole-package check: no block


@pytest.mark.asyncio
async def test_simple_api_blocked_package_raises(tmp_path):
    """simple_api_package_html raises ValueError for blocked packages."""
    from unittest.mock import AsyncMock, MagicMock
    from server.core.config import PyPIConfig
    from server.pypi_service import PyPIMirrorService

    config = PyPIConfig(data_dir=tmp_path / "pypi_storage")
    service = PyPIMirrorService(config=config, db=MagicMock(), audit=MagicMock())

    service._get_blocklist_doc = AsyncMock(
        return_value={
            "_id": "blocklist",
            "blocked_packages": ["banned-lib"],
            "blocked_versions": {},
        }
    )

    with pytest.raises(ValueError, match="blocked"):
        await service.simple_api_package_html("Banned-Lib")


@pytest.mark.asyncio
async def test_simple_api_package_html_links_local_files(tmp_path):
    """
    simple_api_package_html rewrites file links to /pypi/files/…
    and never points to pythonhosted.org.
    """
    from unittest.mock import AsyncMock, MagicMock, patch
    from server.core.config import PyPIConfig
    from server.pypi_service import PyPIMirrorService

    # Create a fake local file in the storage
    pkg_dir = tmp_path / "pypi_storage" / "my-pkg" / "1.0.0"
    pkg_dir.mkdir(parents=True)
    (pkg_dir / "my_pkg-1.0.0-py3-none-any.whl").write_bytes(b"fake wheel")

    config = PyPIConfig(data_dir=tmp_path / "pypi_storage", simple_path="/pypi")
    service = PyPIMirrorService(config=config, db=MagicMock(), audit=MagicMock())

    service._get_blocklist_doc = AsyncMock(
        return_value={
            "_id": "blocklist",
            "blocked_packages": [],
            "blocked_versions": {},
        }
    )

    # Fake PyPI metadata response
    fake_metadata = {
        "releases": {
            "1.0.0": [
                {
                    "filename": "my_pkg-1.0.0-py3-none-any.whl",
                    "url": "https://files.pythonhosted.org/packages/my_pkg-1.0.0-py3-none-any.whl",
                    "digests": {"sha256": "deadbeef1234"},
                }
            ]
        }
    }

    with patch.object(
        service._mirror, "_fetch_metadata", new=AsyncMock(return_value=fake_metadata)
    ):
        html = await service.simple_api_package_html("my-pkg")

    assert html is not None
    assert "pythonhosted.org" not in html, "Links must not point to PyPI CDN"
    assert "/pypi/files/my-pkg/1.0.0/" in html, "Links must use local /pypi/files/ path"
    assert "#sha256=deadbeef1234" in html, "sha256 fragment must be included"


@pytest.mark.asyncio
async def test_simple_api_package_html_excludes_blocked_versions(tmp_path):
    """Blocked versions are excluded from the Simple API HTML."""
    from unittest.mock import AsyncMock, MagicMock, patch
    from server.core.config import PyPIConfig
    from server.pypi_service import PyPIMirrorService

    pkg_dir_v1 = tmp_path / "pypi_storage" / "mylib" / "1.0.0"
    pkg_dir_v1.mkdir(parents=True)
    (pkg_dir_v1 / "mylib-1.0.0.whl").write_bytes(b"x")

    pkg_dir_v2 = tmp_path / "pypi_storage" / "mylib" / "2.0.0"
    pkg_dir_v2.mkdir(parents=True)
    (pkg_dir_v2 / "mylib-2.0.0.whl").write_bytes(b"x")

    config = PyPIConfig(data_dir=tmp_path / "pypi_storage", simple_path="/pypi")
    service = PyPIMirrorService(config=config, db=MagicMock(), audit=MagicMock())

    service._get_blocklist_doc = AsyncMock(
        return_value={
            "_id": "blocklist",
            "blocked_packages": [],
            "blocked_versions": {"mylib": ["1.0.0"]},  # 1.0.0 is blocked
        }
    )

    fake_metadata = {
        "releases": {
            "1.0.0": [{"filename": "mylib-1.0.0.whl", "digests": {"sha256": "aaa"}}],
            "2.0.0": [{"filename": "mylib-2.0.0.whl", "digests": {"sha256": "bbb"}}],
        }
    }

    with patch.object(
        service._mirror, "_fetch_metadata", new=AsyncMock(return_value=fake_metadata)
    ):
        html = await service.simple_api_package_html("mylib")

    assert html is not None
    assert "mylib-1.0.0.whl" not in html, "Blocked version must not appear in index"
    assert "mylib-2.0.0.whl" in html, "Non-blocked version must appear"


@pytest.mark.asyncio
async def test_get_file_path_blocked_package_raises(tmp_path):
    """get_file_path raises PermissionError for a fully blocked package."""
    from unittest.mock import AsyncMock, MagicMock
    from server.core.config import PyPIConfig
    from server.pypi_service import PyPIMirrorService

    config = PyPIConfig(data_dir=tmp_path / "pypi_storage")
    service = PyPIMirrorService(config=config, db=MagicMock(), audit=MagicMock())
    service._get_blocklist_doc = AsyncMock(
        return_value={
            "_id": "blocklist",
            "blocked_packages": ["badlib"],
            "blocked_versions": {},
        }
    )

    with pytest.raises(PermissionError):
        await service.get_file_path("badlib", "1.0.0", "badlib-1.0.0.whl")


@pytest.mark.asyncio
async def test_get_file_path_blocked_version_raises(tmp_path):
    """get_file_path raises PermissionError for a blocked version."""
    from unittest.mock import AsyncMock, MagicMock
    from server.core.config import PyPIConfig
    from server.pypi_service import PyPIMirrorService

    config = PyPIConfig(data_dir=tmp_path / "pypi_storage")
    service = PyPIMirrorService(config=config, db=MagicMock(), audit=MagicMock())
    service._get_blocklist_doc = AsyncMock(
        return_value={
            "_id": "blocklist",
            "blocked_packages": [],
            "blocked_versions": {"goodlib": ["0.1.0"]},
        }
    )

    with pytest.raises(PermissionError):
        await service.get_file_path("goodlib", "0.1.0", "goodlib-0.1.0.whl")


@pytest.mark.asyncio
async def test_get_file_path_returns_existing_file(tmp_path):
    """get_file_path returns the local path when the file exists."""
    from unittest.mock import AsyncMock, MagicMock
    from server.core.config import PyPIConfig
    from server.pypi_service import PyPIMirrorService

    storage = tmp_path / "pypi_storage"
    pkg_dir = storage / "flask" / "2.0.0"
    pkg_dir.mkdir(parents=True)
    whl = pkg_dir / "Flask-2.0.0-py3-none-any.whl"
    whl.write_bytes(b"fake wheel content")

    config = PyPIConfig(data_dir=storage, on_demand_proxy=False)
    service = PyPIMirrorService(config=config, db=MagicMock(), audit=MagicMock())
    service._get_blocklist_doc = AsyncMock(
        return_value={
            "_id": "blocklist",
            "blocked_packages": [],
            "blocked_versions": {},
        }
    )

    result = await service.get_file_path(
        "flask", "2.0.0", "Flask-2.0.0-py3-none-any.whl"
    )
    assert result is not None
    assert result.exists()


@pytest.mark.asyncio
async def test_get_stats(tmp_path):
    """get_stats returns correct counts for a populated storage."""
    from unittest.mock import AsyncMock, MagicMock
    from server.core.config import PyPIConfig
    from server.pypi_service import PyPIMirrorService

    storage = tmp_path / "pypi_storage"
    pkg = storage / "flask" / "2.0.0"
    pkg.mkdir(parents=True)
    (pkg / "Flask-2.0.0-py3-none-any.whl").write_bytes(b"data")

    config = PyPIConfig(data_dir=storage)
    service = PyPIMirrorService(config=config, db=MagicMock(), audit=MagicMock())
    service._get_blocklist_doc = AsyncMock(
        return_value={
            "_id": "blocklist",
            "blocked_packages": [],
            "blocked_versions": {},
        }
    )

    stats = await service.get_stats()
    assert stats.packages_count == 1
    assert stats.versions_count == 1
    assert stats.files_count == 1
    assert stats.total_size_bytes == 4  # b"data"


def test_parse_pkg_spec_exact():
    from server.pypi_service import _parse_pkg_spec

    assert _parse_pkg_spec("flask==2.0.0") == ("flask", "2.0.0")


def test_parse_pkg_spec_no_version():
    from server.pypi_service import _parse_pkg_spec

    assert _parse_pkg_spec("flask") == ("flask", None)


def test_parse_pkg_spec_gte():
    from server.pypi_service import _parse_pkg_spec

    name, ver = _parse_pkg_spec("flask>=2.0")
    assert name == "flask"
    assert ver is None  # non-exact spec has no pinned version
