import asyncio
import shutil
import sys
import types
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

if "aiohttp" not in sys.modules:
    aiohttp_stub = types.ModuleType("aiohttp")

    class _ClientSession:
        pass

    class _ClientError(Exception):
        pass

    aiohttp_stub.ClientSession = _ClientSession
    aiohttp_stub.ClientError = _ClientError
    sys.modules["aiohttp"] = aiohttp_stub
else:
    import aiohttp as _aiohttp

    if not hasattr(_aiohttp, "ClientError"):
        class _ClientError(Exception):
            pass

        _aiohttp.ClientError = _ClientError

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

        async def write(self, data):
            self._fh.write(data)

    def _aio_open(path, mode="r", *args, **kwargs):
        return _AioFileWrapper(path, mode)

    aiofiles_stub.open = _aio_open
    sys.modules["aiofiles"] = aiofiles_stub

from python_mirror import MirrorConfig, AsyncPythonMirror


class FakeResponse:
    def __init__(self, *, status=200, headers=None, text_data="", chunks=None, raise_exc=None):
        self.status = status
        self.headers = headers or {}
        self._text_data = text_data
        self._chunks = chunks or []
        self._raise_exc = raise_exc
        self.content = SimpleNamespace(iter_chunked=self._iter_chunked)

    async def _iter_chunked(self, _size):
        for chunk in self._chunks:
            yield chunk

    async def text(self):
        return self._text_data

    def raise_for_status(self):
        if self._raise_exc:
            raise self._raise_exc

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeSession:
    def __init__(self, get_response=None, head_response=None, get_exc=None, head_exc=None):
        self._get_response = get_response
        self._head_response = head_response
        self._get_exc = get_exc
        self._head_exc = head_exc

    def get(self, *_args, **_kwargs):
        if self._get_exc:
            raise self._get_exc
        return self._get_response

    def head(self, *_args, **_kwargs):
        if self._head_exc:
            raise self._head_exc
        return self._head_response


@pytest.fixture
def temp_dir():
    root = Path.cwd() / ".test_tmp_py"
    root.mkdir(parents=True, exist_ok=True)
    d = root / f"python-mirror-{uuid4().hex}"
    d.mkdir(parents=True, exist_ok=True)
    try:
        yield d
    finally:
        shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def base_config(temp_dir):
    return MirrorConfig(data_dir=temp_dir, parallel=2, show_progress=False)


@pytest.fixture
def mirror(base_config):
    return AsyncPythonMirror(base_config)


def test_config_path_validation():
    config = MirrorConfig()
    assert config.data_dir is not None
    assert isinstance(config.data_dir, Path)


def test_config_invalid_parallel():
    with pytest.raises(ValueError):
        MirrorConfig(parallel=25)


def test_safe_path_protection(mirror, temp_dir):
    with pytest.raises(ValueError):
        mirror._safe_path("../etc/passwd")

    valid_path = mirror._safe_path("3.12.0", "python.exe")
    assert str(valid_path).startswith(str(temp_dir))


def test_is_useful_file(mirror):
    assert mirror._is_useful_file("Python-3.11.0.tar.xz", "3.11.0") is True
    assert mirror._is_useful_file("python-3.11.0-amd64.exe", "3.11.0") is True
    assert mirror._is_useful_file("random_file.txt", "3.11.0") is False


def test_load_proxies_variants(temp_dir):
    mirror = AsyncPythonMirror(MirrorConfig(data_dir=temp_dir, proxies=None))
    assert mirror.proxies == []

    mirror = AsyncPythonMirror(MirrorConfig(data_dir=temp_dir, proxies=["http://p1", "http://p2"]))
    assert mirror.proxies == ["http://p1", "http://p2"]

    proxy_file = temp_dir / "proxies.txt"
    proxy_file.write_text("http://p1\n\nhttp://p2\n", encoding="utf-8")
    mirror = AsyncPythonMirror(MirrorConfig(data_dir=temp_dir, proxies=str(proxy_file)))
    assert mirror.proxies == ["http://p1", "http://p2"]

    mirror = AsyncPythonMirror(MirrorConfig(data_dir=temp_dir, proxies="http://single"))
    assert mirror.proxies == ["http://single"]


def test_choose_proxy_modes(mirror, monkeypatch):
    mirror.network_mode = "direct"
    mirror.proxies = ["http://p1"]
    assert mirror._choose_proxy() is None

    mirror.network_mode = "proxy"
    mirror.proxies = ["http://p1", "http://p2"]
    monkeypatch.setattr("random.choice", lambda seq: seq[1])
    assert mirror._choose_proxy() == "http://p2"


def test_format_size(mirror):
    assert mirror._format_size(512) == "512.00 B"
    assert mirror._format_size(1024) == "1.00 KB"


def test_get_remote_file_size_success_and_fail(mirror):
    async def _run():
        ok = FakeSession(head_response=FakeResponse(status=200, headers={"Content-Length": "321"}))
        assert await mirror._get_remote_file_size(ok, "3.12.0", "a.exe") == 321

        bad_status = FakeSession(head_response=FakeResponse(status=404, headers={}))
        assert await mirror._get_remote_file_size(bad_status, "3.12.0", "a.exe") == 0

        broken = FakeSession(head_exc=RuntimeError("boom"))
        assert await mirror._get_remote_file_size(broken, "3.12.0", "a.exe") == 0

    asyncio.run(_run())


def test_check_disk_space_insufficient_and_zero_total(mirror, monkeypatch):
    async def _run():
        mirror._get_remote_file_size = AsyncMock(return_value=10 * 1024**3)
        monkeypatch.setattr("shutil.disk_usage", lambda _p: (0, 0, 5 * 1024**3))
        with pytest.raises(IOError):
            await mirror._check_disk_space(MagicMock(), "3.12.0", ["file1"])

        mirror._get_remote_file_size = AsyncMock(return_value=0)
        await mirror._check_disk_space(MagicMock(), "3.12.0", ["file1"])

    asyncio.run(_run())


def test_check_file_integrity_variants(mirror, temp_dir):
    async def _run():
        dest = temp_dir / "f.bin"
        assert await mirror._check_file_integrity(MagicMock(), "3.12.0", "f.bin", dest) is False

        dest.write_bytes(b"1234")
        ok = FakeSession(head_response=FakeResponse(status=200, headers={"Content-Length": "4"}))
        assert await mirror._check_file_integrity(ok, "3.12.0", "f.bin", dest) is True

        mismatch = FakeSession(head_response=FakeResponse(status=200, headers={"Content-Length": "5"}))
        assert await mirror._check_file_integrity(mismatch, "3.12.0", "f.bin", dest) is False

        broken = FakeSession(head_exc=RuntimeError("boom"))
        assert await mirror._check_file_integrity(broken, "3.12.0", "f.bin", dest) is False

    asyncio.run(_run())


def test_download_file_success_and_error_cleanup(mirror, temp_dir, monkeypatch):
    async def _run():
        dest = temp_dir / "test.exe"
        url = "https://fake.url/test.exe"

        resp = FakeResponse(chunks=[b"data1", b"data2"])
        session = FakeSession(get_response=resp)

        sleep_mock = AsyncMock()
        mirror.rate_limit = 1
        monkeypatch.setattr(asyncio, "sleep", sleep_mock)
        monkeypatch.setattr("time.time", lambda: 0.0)

        ok = await mirror._download_file(session, url, dest)
        assert ok is True
        assert dest.read_bytes() == b"data1data2"
        assert sleep_mock.await_count >= 1

        temp = dest.with_suffix(".download.tmp")

        class FailingAsyncCM:
            async def __aenter__(self):
                raise RuntimeError("write failed")

            async def __aexit__(self, exc_type, exc, tb):
                return False

        def fail_open(*_args, **_kwargs):
            return FailingAsyncCM()

        temp.write_bytes(b"x")
        monkeypatch.setattr("aiofiles.open", fail_open)
        with pytest.raises(RuntimeError):
            await mirror._download_file(session, url, dest)
        assert not temp.exists()

    asyncio.run(_run())


def test_download_single_variants(mirror):
    async def _run():
        sem = asyncio.Semaphore(1)

        mirror._check_file_integrity = AsyncMock(return_value=True)
        mirror._download_file = AsyncMock(return_value=True)
        assert await mirror._download_single(MagicMock(), "3.12.0", "a.exe", sem) is True
        mirror._download_file.assert_not_awaited()

        mirror._check_file_integrity = AsyncMock(return_value=False)
        mirror._download_file = AsyncMock(return_value=True)
        assert await mirror._download_single(MagicMock(), "3.12.0", "a.exe", sem) is True

        async def fail_download(*_args, **_kwargs):
            raise RuntimeError("boom")

        mirror._download_file = fail_download
        assert await mirror._download_single(MagicMock(), "3.12.0", "a.exe", sem) is False

    asyncio.run(_run())


def test_get_versions_network_and_fallback(mirror):
    async def _run():
        html = """
        <html>
            <a href="3.10.1/">3.10.1/</a>
            <a href="3.11.0/">3.11.0/</a>
            <a href="latest/">latest/</a>
        </html>
        """
        ok_session = FakeSession(get_response=FakeResponse(text_data=html))
        versions = await mirror.get_versions(ok_session)
        assert versions == ["3.11.0", "3.10.1"]

        mirror.list_installed = AsyncMock(return_value=["3.9.0"])
        broken = FakeSession(get_exc=asyncio.TimeoutError())
        assert await mirror.get_versions(broken) == ["3.9.0"]

    asyncio.run(_run())


def test_install_version_no_files_success_retry_and_fail(mirror, monkeypatch):
    async def _run():
        empty_html = "<html><a href='readme.txt'>readme.txt</a></html>"
        session = FakeSession(get_response=FakeResponse(text_data=empty_html))

        assert await mirror.install_version(session, "3.12.0") is False

        good_html = """
        <html>
            <a href="Python-3.12.0.tar.xz">Python-3.12.0.tar.xz</a>
            <a href="python-3.12.0-amd64.exe">python-3.12.0-amd64.exe</a>
        </html>
        """
        session2 = FakeSession(get_response=FakeResponse(text_data=good_html))
        mirror._check_disk_space = AsyncMock()
        mirror._download_single = AsyncMock(return_value=True)
        assert await mirror.install_version(session2, "3.12.0") is True

        state = {"first": True}

        async def flaky(*_args, **_kwargs):
            if state["first"]:
                state["first"] = False
                return False
            return True

        mirror.max_retries = 2
        mirror._download_single = flaky
        monkeypatch.setattr(asyncio, "sleep", AsyncMock())
        assert await mirror.install_version(session2, "3.12.0") is True

        mirror.max_retries = 1

        async def always_fail(*_args, **_kwargs):
            return False

        mirror._download_single = always_fail
        with pytest.raises(RuntimeError):
            await mirror.install_version(session2, "3.12.0")

    asyncio.run(_run())


def test_list_installed_plain_and_integrity(mirror):
    async def _run():
        (mirror._safe_path("3.12.0")).mkdir(parents=True, exist_ok=True)
        (mirror._safe_path("3.12.0", "f.exe")).write_bytes(b"x")
        (mirror._safe_path("3.11.0")).mkdir(parents=True, exist_ok=True)

        installed = await mirror.list_installed()
        assert set(installed) == {"3.12.0", "3.11.0"}

        with pytest.raises(ValueError):
            await mirror.list_installed(check_integrity=True)

        mirror._check_file_integrity = AsyncMock(return_value=True)
        valid = await mirror.list_installed(session=MagicMock(), check_integrity=True)
        assert valid == ["3.12.0"]

    asyncio.run(_run())


def test_repair_all_calls_install_for_each(mirror):
    async def _run():
        mirror.list_installed = AsyncMock(return_value=["3.10.0", "3.11.0"])
        mirror.install_version = AsyncMock(return_value=True)

        await mirror.repair_all(MagicMock())

        called = [c.args[1] for c in mirror.install_version.await_args_list]
        assert called == ["3.10.0", "3.11.0"]

    asyncio.run(_run())


def test_get_file_path_variants_and_invalid(mirror):
    w = mirror.get_file_path("3.12.0", "windows", "amd64", True)
    assert w.name == "python-3.12.0-amd64.exe"

    l = mirror.get_file_path("3.12.0", "linux", "amd64", False)
    assert l.name == "Python-3.12.0.tar.xz"

    m = mirror.get_file_path("3.12.0", "macos", "amd64", True)
    assert m.name == "python-3.12.0-macos11.pkg"

    with pytest.raises(ValueError):
        mirror.get_file_path("3.12.0", "unknown-os")


def test_version_size_total_size_and_remove(mirror):
    ver = mirror._safe_path("3.12.0")
    ver.mkdir(parents=True, exist_ok=True)
    (ver / "a.bin").write_bytes(b"1234")
    sub = ver / "sub"
    sub.mkdir()
    (sub / "b.bin").write_bytes(b"12")

    assert mirror.get_version_size("3.12.0") == 6
    assert mirror.get_total_size() >= 6
    assert mirror.remove_version("3.12.0") is True
    assert mirror.remove_version("3.12.0") is False
