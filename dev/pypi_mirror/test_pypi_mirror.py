import asyncio
import hashlib
import shutil
import sys
import types
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

# Lightweight stubs for environments without aiohttp/aiofiles installed.
if "aiohttp" not in sys.modules:
    aiohttp_stub = types.ModuleType("aiohttp")

    class _ClientSession:  # pragma: no cover - import compatibility only
        pass

    aiohttp_stub.ClientSession = _ClientSession
    sys.modules["aiohttp"] = aiohttp_stub

if "aiofiles" not in sys.modules:
    aiofiles_stub = types.ModuleType("aiofiles")

    class _AioFileWrapper:
        def __init__(self, path, mode):
            self._path = path
            self._mode = mode
            self._fh = None

        async def __aenter__(self):
            self._fh = open(self._path, self._mode)
            return self

        async def __aexit__(self, exc_type, exc, tb):
            if self._fh:
                self._fh.close()
            return False

        async def read(self, size=-1):
            return self._fh.read(size)

        async def write(self, data):
            self._fh.write(data)

    def _aio_open(path, mode="r", *args, **kwargs):
        return _AioFileWrapper(path, mode)

    aiofiles_stub.open = _aio_open
    sys.modules["aiofiles"] = aiofiles_stub

# Allow importing dev/pypi_mirror/pypi_mirror.py as module pypi_mirror
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pypi_mirror import AsyncPypiMirror, MirrorConfig


class FakeResponse:
    def __init__(self, *, status=200, json_data=None, chunks=None, raise_exc=None):
        self.status = status
        self._json_data = json_data if json_data is not None else {}
        self._chunks = chunks if chunks is not None else []
        self._raise_exc = raise_exc
        self.content = SimpleNamespace(iter_chunked=self._iter_chunked)

    async def _iter_chunked(self, _size):
        for chunk in self._chunks:
            yield chunk

    async def json(self):
        return self._json_data

    def raise_for_status(self):
        if self._raise_exc:
            raise self._raise_exc

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeSession:
    def __init__(self, response=None, exc=None):
        self.response = response
        self.exc = exc
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if self.exc:
            raise self.exc
        return self.response


@pytest.fixture
def temp_dir():
    root = Path.cwd() / ".test_tmp"
    root.mkdir(parents=True, exist_ok=True)
    d = root / f"pypi-mirror-{uuid4().hex}"
    d.mkdir(parents=True, exist_ok=True)
    try:
        yield d
    finally:
        shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def cfg(temp_dir):
    return MirrorConfig(data_dir=temp_dir, proxies=["http://p1", "http://p2"], parallel=2)


@pytest.fixture
def mirror(cfg):
    return AsyncPypiMirror(cfg)


def test_choose_proxy_modes(mirror, monkeypatch):
    mirror.cfg.network_mode = "direct"
    assert mirror._choose_proxy() is None

    mirror.cfg.network_mode = "proxy"
    monkeypatch.setattr("random.choice", lambda seq: seq[0])
    assert mirror._choose_proxy() == "http://p1"

    mirror.cfg.network_mode = "mix"
    monkeypatch.setattr("random.choice", lambda seq: seq[-1])
    assert mirror._choose_proxy() == "http://p2"

    mirror.cfg.proxies = []
    assert mirror._choose_proxy() is None


def test_get_dir_helpers(mirror, temp_dir):
    pkg = mirror._get_pkg_dir("Requests")
    ver = mirror._get_ver_dir("Requests", "2.31.0")
    assert pkg == temp_dir / "requests"
    assert ver == temp_dir / "requests" / "2.31.0"


def test_verify_hash_variants(mirror, temp_dir):
    async def _run():
        file_path = temp_dir / "a.whl"
        data = b"hello-world"
        file_path.write_bytes(data)
        expected = hashlib.sha256(data).hexdigest()

        assert await mirror._verify_hash(file_path, expected) is True
        assert await mirror._verify_hash(file_path, "0" * 64) is False
        assert await mirror._verify_hash(temp_dir / "missing.whl", expected) is False

    asyncio.run(_run())


def test_fetch_metadata_success_and_failures(mirror):
    async def _run():
        ok_payload = {"releases": {"1.0": []}}
        ok_session = FakeSession(response=FakeResponse(status=200, json_data=ok_payload))
        assert await mirror._fetch_metadata(ok_session, "demo") == ok_payload

        bad_status_session = FakeSession(response=FakeResponse(status=404, json_data={}))
        assert await mirror._fetch_metadata(bad_status_session, "demo") is None

        broken_session = FakeSession(exc=RuntimeError("boom"))
        assert await mirror._fetch_metadata(broken_session, "demo") is None

    asyncio.run(_run())


def test_check_disk_space(mirror, monkeypatch):
    async def _run():
        monkeypatch.setattr("shutil.disk_usage", lambda _p: (0, 0, 10 * 1024**3))
        await mirror._check_disk_space(1 * 1024**3)

        monkeypatch.setattr("shutil.disk_usage", lambda _p: (0, 0, 3 * 1024**3))
        with pytest.raises(IOError):
            await mirror._check_disk_space(1 * 1024**3)

    asyncio.run(_run())


def test_download_file_success_and_rate_limit(temp_dir, monkeypatch):
    async def _run():
        cfg = MirrorConfig(data_dir=temp_dir, rate_limit_mb=0.001)
        mirror = AsyncPypiMirror(cfg)

        dest = temp_dir / "pkg.whl"
        response = FakeResponse(chunks=[b"abc", b"def"])
        session = FakeSession(response=response)

        sleep_mock = AsyncMock()
        monkeypatch.setattr(asyncio, "sleep", sleep_mock)

        # Make expected > elapsed to trigger sleep branch deterministically.
        t = iter([0.0, 0.0, 0.0])
        monkeypatch.setattr("time.time", lambda: next(t, 0.0))

        await mirror._download_file(session, "https://example/file", dest)

        assert dest.read_bytes() == b"abcdef"
        assert sleep_mock.await_count >= 1

    asyncio.run(_run())


def test_download_version_missing_version_returns_false(mirror):
    async def _run():
        mirror._fetch_metadata = AsyncMock(return_value={"releases": {"1.0": []}})
        session = MagicMock()
        assert await mirror.download_version(session, "demo", "2.0") is False

    asyncio.run(_run())


def test_download_version_skips_when_hash_is_valid(mirror):
    async def _run():
        ver_dir = mirror._get_ver_dir("demo", "1.0")
        ver_dir.mkdir(parents=True, exist_ok=True)
        (ver_dir / "demo.whl").write_bytes(b"ok")

        metadata = {
            "releases": {
                "1.0": [
                    {
                        "url": "https://example/demo.whl",
                        "filename": "demo.whl",
                        "digests": {"sha256": "x"},
                        "size": 10,
                    }
                ]
            }
        }
        mirror._fetch_metadata = AsyncMock(return_value=metadata)
        mirror._verify_hash = AsyncMock(return_value=True)
        mirror._check_disk_space = AsyncMock()
        mirror._download_file = AsyncMock()

        ok = await mirror.download_version(MagicMock(), "demo", "1.0")

        assert ok is True
        mirror._check_disk_space.assert_not_awaited()
        mirror._download_file.assert_not_awaited()

    asyncio.run(_run())


def test_download_version_hash_mismatch_removes_file(mirror):
    async def _run():
        metadata = {
            "releases": {
                "1.0": [
                    {
                        "url": "https://example/demo.whl",
                        "filename": "demo.whl",
                        "digests": {"sha256": "expected"},
                        "size": 10,
                    }
                ]
            }
        }
        mirror._fetch_metadata = AsyncMock(return_value=metadata)

        # First check (existing file) -> False, second check (after download) -> False
        mirror._verify_hash = AsyncMock(side_effect=[False, False])
        mirror._check_disk_space = AsyncMock()

        async def fake_download(_session, _url, dest):
            dest.write_bytes(b"content")

        mirror._download_file = fake_download

        ok = await mirror.download_version(MagicMock(), "demo", "1.0")

        assert ok is False
        assert not (mirror._get_ver_dir("demo", "1.0") / "demo.whl").exists()

    asyncio.run(_run())


def test_download_version_download_exception_returns_false(mirror):
    async def _run():
        metadata = {
            "releases": {
                "1.0": [
                    {
                        "url": "https://example/demo.whl",
                        "filename": "demo.whl",
                        "digests": {"sha256": "expected"},
                        "size": 10,
                    }
                ]
            }
        }
        mirror._fetch_metadata = AsyncMock(return_value=metadata)
        mirror._verify_hash = AsyncMock(return_value=False)
        mirror._check_disk_space = AsyncMock()

        async def fail_download(*_args, **_kwargs):
            raise RuntimeError("network error")

        mirror._download_file = fail_download

        assert await mirror.download_version(MagicMock(), "demo", "1.0") is False

    asyncio.run(_run())


def test_download_all_versions(mirror):
    async def _run():
        mirror._fetch_metadata = AsyncMock(return_value={"releases": {"1.0": [], "2.0": []}})
        mirror.download_version = AsyncMock(return_value=True)

        await mirror.download_all_versions(MagicMock(), "demo")

        called_versions = {call.args[2] for call in mirror.download_version.await_args_list}
        assert called_versions == {"1.0", "2.0"}

    asyncio.run(_run())


def test_download_all_versions_no_metadata(mirror):
    async def _run():
        mirror._fetch_metadata = AsyncMock(return_value=None)
        mirror.download_version = AsyncMock(return_value=True)

        await mirror.download_all_versions(MagicMock(), "demo")

        mirror.download_version.assert_not_awaited()

    asyncio.run(_run())


def test_check_integrity_warns_on_corrupted_file(mirror, temp_dir, monkeypatch):
    async def _run():
        lib_dir = temp_dir / "demo" / "1.0"
        lib_dir.mkdir(parents=True)

        mirror._fetch_metadata = AsyncMock(
            return_value={
                "releases": {
                    "1.0": [
                        {
                            "filename": "demo.whl",
                            "digests": {"sha256": "x"},
                        }
                    ]
                }
            }
        )
        mirror._verify_hash = AsyncMock(return_value=False)

        warn_mock = MagicMock()
        monkeypatch.setattr("pypi_mirror.logger.warning", warn_mock)

        await mirror.check_integrity(MagicMock())

        assert warn_mock.call_count >= 1

    asyncio.run(_run())


def test_delete_library_full_and_version(mirror):
    full = mirror._get_pkg_dir("demo")
    full.mkdir(parents=True)
    (full / "1.0").mkdir()
    (full / "2.0").mkdir()

    mirror.delete_library("demo", "1.0")
    assert not (full / "1.0").exists()
    assert (full / "2.0").exists()

    mirror.delete_library("demo")
    assert not full.exists()


def test_get_info(mirror):
    async def _run():
        pkg = mirror._get_pkg_dir("demo")
        (pkg / "1.0").mkdir(parents=True)

        mirror._fetch_metadata = AsyncMock(return_value={"releases": {"1.0": [], "2.0": []}})

        info = await mirror.get_info(MagicMock(), "demo")

        assert info["name"] == "demo"
        assert info["is_downloaded"] is True
        assert "1.0" in info["downloaded_versions"]
        assert info["missing_versions"] == ["2.0"]

    asyncio.run(_run())


def test_list_libraries(mirror):
    (mirror._get_ver_dir("a", "1.0")).mkdir(parents=True)
    (mirror._get_ver_dir("b", "2.0")).mkdir(parents=True)

    plain = mirror.list_libraries(include_versions=False)
    with_versions = mirror.list_libraries(include_versions=True)

    assert plain == {"a": [], "b": []}
    assert set(with_versions["a"]) == {"1.0"}
    assert set(with_versions["b"]) == {"2.0"}


def test_get_path_variants(mirror):
    assert mirror.get_path("missing") is None

    (mirror._get_ver_dir("demo", "1.0")).mkdir(parents=True)
    (mirror._get_ver_dir("demo", "2.0")).mkdir(parents=True)

    assert mirror.get_path("demo", "1.0") == mirror._get_ver_dir("demo", "1.0")
    assert mirror.get_path("demo", "9.9") is None
    assert mirror.get_path("demo") == mirror._get_ver_dir("demo", "2.0")


def test_get_path_fallback_for_non_pep440(mirror):
    pkg = mirror._get_pkg_dir("weird")
    (pkg / "abc").mkdir(parents=True)
    (pkg / "zzz").mkdir(parents=True)

    # version.parse will fail for non-PEP440 names, method should fallback to lexical max.
    assert mirror.get_path("weird") == pkg / "zzz"
