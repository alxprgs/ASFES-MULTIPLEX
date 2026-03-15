import asyncio
import json
import os
import shutil
import sys
import types
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

# Lightweight stubs for environments without aiohttp/aiofiles.
if "aiohttp" not in sys.modules:
    aiohttp_stub = types.ModuleType("aiohttp")

    class _ClientSession:
        pass

    class _ClientTimeout:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    aiohttp_stub.ClientSession = _ClientSession
    aiohttp_stub.ClientTimeout = _ClientTimeout
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

        async def write(self, data):
            self._fh.write(data)

    def _aio_open(path, mode="r", *args, **kwargs):
        return _AioFileWrapper(path, mode)

    aiofiles_stub.open = _aio_open
    sys.modules["aiofiles"] = aiofiles_stub

# Allow importing local module as "arduino_mirror"
sys.path.insert(0, str(Path(__file__).resolve().parent))

from arduino_mirror import AsyncArduinoMirror, MirrorConfig


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

    async def read(self):
        return b"".join(self._chunks)

    def raise_for_status(self):
        if self._raise_exc:
            raise self._raise_exc

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeSession:
    def __init__(self, *, get_response=None, head_response=None, get_exc=None, head_exc=None):
        self._get_response = get_response
        self._head_response = head_response
        self._get_exc = get_exc
        self._head_exc = head_exc
        self.get_calls = []
        self.head_calls = []

    def get(self, *args, **kwargs):
        self.get_calls.append((args, kwargs))
        if self._get_exc:
            raise self._get_exc
        if callable(self._get_response):
            return self._get_response(*args, **kwargs)
        return self._get_response

    def head(self, *args, **kwargs):
        self.head_calls.append((args, kwargs))
        if self._head_exc:
            raise self._head_exc
        if callable(self._head_response):
            return self._head_response(*args, **kwargs)
        return self._head_response


@pytest.fixture
def temp_dir():
    root = Path.cwd() / ".test_tmp_arduino"
    root.mkdir(parents=True, exist_ok=True)
    d = root / f"arduino-mirror-{uuid4().hex}"
    d.mkdir(parents=True, exist_ok=True)
    try:
        yield d
    finally:
        shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def mirror(temp_dir):
    cfg = MirrorConfig(
        data_dir=temp_dir,
        proxies=["http://p1", "http://p2"],
        show_progress=False,
        parallel=2,
    )
    return AsyncArduinoMirror(cfg)


def test_choose_proxy_modes(mirror, monkeypatch):
    mirror.cfg.network_mode = "direct"
    assert mirror._choose_proxy() is None

    mirror.cfg.network_mode = "proxy"
    monkeypatch.setattr("random.choice", lambda seq: seq[1])
    assert mirror._choose_proxy() == "http://p2"

    mirror.cfg.network_mode = "mix"
    monkeypatch.setattr("random.choice", lambda seq: seq[-1])
    assert mirror._choose_proxy() in {None, "http://p2"}


def test_safe_path_blocks_traversal(mirror, temp_dir):
    with pytest.raises(ValueError):
        mirror._safe_path("..", "etc", "passwd")

    safe = mirror._safe_path("hardware", "Uno_R4_WiFi")
    assert str(safe).startswith(str(temp_dir))


def test_normalize_url_variants(mirror):
    url_a = mirror._normalize_url("https://docs.arduino.cc/software")
    url_b = mirror._normalize_url("https://docs.arduino.cc/software/")
    url_c = mirror._normalize_url("https://docs.arduino.cc/software/#tabs")
    url_d = mirror._normalize_url("/software/?utm=test")

    assert url_a == "https://docs.arduino.cc/software/"
    assert url_a == url_b == url_c
    assert url_d == "https://docs.arduino.cc/software/"


def test_detect_page_type_hardware_product(mirror):
    html = """
    <html><body>
      <h1>UNO R4 WiFi</h1>
      <h2>Tech Specs</h2>
      <h2>Downloadable resources</h2>
      <h2>Suggested Libraries</h2>
    </body></html>
    """
    soup = mirror._content_node(__import__("bs4").BeautifulSoup(html, "html.parser"))
    page_type = mirror._detect_page_type(
        "https://docs.arduino.cc/hardware/uno-r4-wifi/",
        __import__("bs4").BeautifulSoup(html, "html.parser"),
        html,
    )
    assert page_type == "hardware_product"
    assert soup is not None


def test_detect_page_type_reference_page(mirror):
    html = """
    <html><body>
      <h1>if(Serial)</h1>
      <h2>Syntax</h2>
      <h2>Parameters</h2>
      <p>Last revision 2025-06-13</p>
    </body></html>
    """
    page_type = mirror._detect_page_type(
        "https://docs.arduino.cc/language-reference/en/functions/communication/serial/ifSerial/",
        __import__("bs4").BeautifulSoup(html, "html.parser"),
        html,
    )
    assert page_type == "reference_page"


def test_extract_downloadable_resources(mirror):
    html = """
    <html><body>
      <h2>Downloadable resources</h2>
      <a href="/resources/pinouts/ABX00087-full-pinout.pdf">Pinout (PDF)</a>
      <a href="/resources/schematics/ABX00087-cad-files.zip">CAD files</a>
    </body></html>
    """
    soup = __import__("bs4").BeautifulSoup(html, "html.parser")
    resources = mirror._extract_downloadable_resources(soup, "https://docs.arduino.cc/hardware/uno-r4-wifi/")

    assert len(resources) == 2
    assert resources[0]["url"].endswith("ABX00087-full-pinout.pdf")
    assert resources[1]["url"].endswith("ABX00087-cad-files.zip")


def test_extract_feature_cards(mirror):
    html = """
    <html><body>
      <h2>Features</h2>
      <a href="/tutorials/uno-r4-wifi/led-matrix"><h3>LED Matrix</h3><p>Display patterns.</p></a>
      <a href="/tutorials/uno-r4-wifi/rtc"><h3>RTC</h3><p>Real-time clock.</p></a>
    </body></html>
    """
    soup = __import__("bs4").BeautifulSoup(html, "html.parser")
    cards = mirror._extract_feature_cards(soup, "https://docs.arduino.cc/hardware/uno-r4-wifi/")

    assert len(cards) >= 2
    assert any(card["title"] == "LED Matrix" for card in cards)
    assert any("tutorials/uno-r4-wifi/rtc" in card["url"] for card in cards)


def test_extract_tech_specs(mirror):
    html = """
    <html><body>
      <h2>Tech Specs</h2>
      <p>Electrical characteristics.</p>
      <table><tr><th>Param</th><th>Value</th></tr><tr><td>Voltage</td><td>5V</td></tr></table>
      <h2>Compatibility</h2>
    </body></html>
    """
    soup = __import__("bs4").BeautifulSoup(html, "html.parser")
    specs = mirror._extract_tech_specs(soup)

    assert specs["found"] is True
    assert "Voltage" in specs["markdown"]
    assert "| Param | Value |" in specs["markdown"]


def test_markdown_table_conversion(mirror):
    html = "<table><tr><th>A</th><th>B</th></tr><tr><td>1</td><td>2</td></tr></table>"
    table = __import__("bs4").BeautifulSoup(html, "html.parser").find("table")
    md = mirror._convert_table_to_markdown(table)

    assert "| A | B |" in md
    assert "| 1 | 2 |" in md


def test_markdown_code_block_conversion(mirror):
    html = "<pre><code class='language-cpp'>int x = 1;\nSerial.println(x);</code></pre>"
    pre = __import__("bs4").BeautifulSoup(html, "html.parser").find("pre")
    md = mirror._convert_code_block(pre)

    assert md.startswith("```cpp")
    assert "Serial.println" in md


def test_download_file_success_and_rate_limit(temp_dir, monkeypatch):
    async def _run():
        cfg = MirrorConfig(data_dir=temp_dir, rate_limit_mb=0.001, show_progress=False)
        mirror = AsyncArduinoMirror(cfg)
        dest = temp_dir / "demo.bin"

        response = FakeResponse(headers={"Content-Length": "6"}, chunks=[b"abc", b"def"])
        session = FakeSession(get_response=response)

        rate_mock = AsyncMock()
        monkeypatch.setattr(mirror, "_apply_rate_limit", rate_mock)

        await mirror._download_file(session, "https://docs.arduino.cc/assets/demo.bin", dest)
        assert dest.read_bytes() == b"abcdef"
        assert rate_mock.await_count >= 1

    asyncio.run(_run())


def test_download_asset_temp_rename(mirror, temp_dir, monkeypatch):
    async def _run():
        dest_dir = mirror._safe_path("hardware", "Uno_R4_WiFi", "files")
        dest_dir.mkdir(parents=True, exist_ok=True)

        response = FakeResponse(headers={"Content-Length": "4"}, chunks=[b"data"])
        head = FakeResponse(status=200, headers={"Content-Disposition": 'attachment; filename="sample.pdf"'})
        session = FakeSession(get_response=response, head_response=head)

        calls = []
        real_replace = os.replace

        def spy_replace(src, dst):
            calls.append((str(src), str(dst)))
            return real_replace(src, dst)

        monkeypatch.setattr(os, "replace", spy_replace)

        asset = await mirror._download_asset(
            session,
            "https://docs.arduino.cc/resources/sample.pdf",
            dest_dir,
        )
        assert Path(asset["local_path"]).exists()
        assert len(calls) >= 1
        assert not any(p.suffix == ".tmp" for p in dest_dir.glob("*"))

    asyncio.run(_run())


def test_verify_hash_or_size_logic(mirror, temp_dir):
    file_path = temp_dir / "asset.pdf"
    file_path.write_bytes(b"1234")

    bad_size = {"local_path": str(file_path), "size_bytes": 5, "sha256": ""}
    ok, reason = mirror._verify_asset(bad_size)
    assert ok is False
    assert reason == "size_mismatch"

    bad_hash = {"local_path": str(file_path), "size_bytes": 4, "sha256": "0" * 64}
    ok, reason = mirror._verify_asset(bad_hash)
    assert ok is False
    assert reason == "hash_mismatch"


def test_state_resume_skips_known_pages(mirror):
    async def _run():
        url = "https://docs.arduino.cc/learn/"
        mirror._mark_visited(url)
        mirror._save_state()

        result = await mirror.crawl_url(None, url)
        assert result["status"] == "skipped"
        assert result["reason"] == "visited"

    asyncio.run(_run())


def test_rebuild_indexes(mirror):
    page_md = mirror._safe_path("learn", "digital-pins.md")
    page_json = page_md.with_suffix(".json")
    page_md.parent.mkdir(parents=True, exist_ok=True)
    page_md.write_text("# Digital Pins\n", encoding="utf-8")
    page_json.write_text(
        json.dumps(
            {
                "title": "Digital Pins",
                "normalized_url": "https://docs.arduino.cc/learn/digital-pins/",
                "page_type": "article_page",
                "section": "learn",
                "breadcrumbs": ["Home", "Learn"],
                "description": "desc",
                "last_revision": "2025-01-01",
                "local_markdown_path": str(page_md),
                "local_html_path": "",
                "asset_paths": [],
                "outgoing_links": [],
                "fetched_at": "2026-01-01",
                "content_hash": "abc",
                "metadata_path": str(page_json),
                "source_url": "https://docs.arduino.cc/learn/digital-pins/",
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    hw_dir = mirror._safe_path("hardware", "Uno_R4_WiFi")
    hw_dir.mkdir(parents=True, exist_ok=True)
    (hw_dir / "HardwareInfo.json").write_text(
        json.dumps(
            {
                "name": "UNO R4 WiFi",
                "slug": "uno-r4-wifi",
                "source_url": "https://docs.arduino.cc/hardware/uno-r4-wifi/",
                "breadcrumbs": ["Home", "Hardware"],
                "markdown_files": {"index": str(hw_dir / "_index.md")},
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    stats = mirror.rebuild_indexes()
    assert stats["pages_index"] >= 1
    assert stats["hardware_index"] >= 1


def test_get_path_variants(mirror):
    hw_dir = mirror._safe_path("hardware", "Uno_R4_WiFi")
    hw_dir.mkdir(parents=True, exist_ok=True)

    assert mirror.get_path("hardware") == mirror._safe_path("hardware")
    assert mirror.get_path("hardware", "UNO R4 WiFi") == hw_dir
    assert mirror.get_path("hardware", "missing-item") is None


def test_delete_target(mirror):
    file_path = mirror._safe_path("learn", "digital-pins.md")
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text("# demo\n", encoding="utf-8")

    assert mirror.delete_target("learn", "digital-pins") is True
    assert not file_path.exists()


def test_get_info(mirror):
    summary = mirror.get_info()
    assert summary["mirror"] == "arduino"

    url = "https://docs.arduino.cc/learn/digital-pins/"
    mirror.pages_index.append(
        {
            "title": "Digital Pins",
            "normalized_url": url,
            "page_type": "article_page",
            "section": "learn",
            "local_markdown_path": str(mirror._safe_path("learn", "digital-pins.md")),
        }
    )
    info = mirror.get_info(url)
    assert info["record"]["normalized_url"] == url


def test_article_page_metadata_extraction(mirror):
    html = """
    <html><body>
      <nav aria-label="breadcrumb"><a>Home</a><a>Learn</a><span>Digital Pins</span></nav>
      <h1>Digital Pins</h1>
      <p>Last revision 2025-06-13</p>
    </body></html>
    """
    soup = __import__("bs4").BeautifulSoup(html, "html.parser")

    title = mirror._extract_title(soup)
    crumbs = mirror._extract_breadcrumbs(soup)
    revision = mirror._extract_last_revision(soup)

    assert title == "Digital Pins"
    assert crumbs == ["Home", "Learn", "Digital Pins"]
    assert revision == "2025-06-13"


def test_link_localization(mirror):
    current = mirror._safe_path("learn", "digital-pins.md")
    target_page = mirror._safe_path("tutorials", "uno-r4-wifi", "led-matrix.md")
    target_asset = mirror._safe_path("tutorials", "uno-r4-wifi", "assets", "img.png")
    target_page.parent.mkdir(parents=True, exist_ok=True)
    target_asset.parent.mkdir(parents=True, exist_ok=True)
    target_page.write_text("# demo\n", encoding="utf-8")
    target_asset.write_bytes(b"img")

    page_url = mirror._normalize_url("https://docs.arduino.cc/tutorials/uno-r4-wifi/led-matrix/")
    asset_url = mirror._normalize_url("https://docs.arduino.cc/tutorials/uno-r4-wifi/assets/img.png")
    mirror.url_to_local_path[page_url] = str(target_page)
    mirror._assets_by_url[asset_url] = {"local_path": str(target_asset), "normalized_url": asset_url}

    md = (
        "See [LED Matrix](https://docs.arduino.cc/tutorials/uno-r4-wifi/led-matrix/)"
        " and ![img](https://docs.arduino.cc/tutorials/uno-r4-wifi/assets/img.png)"
    )
    rewritten = mirror._rewrite_links_to_local(md, current)

    assert "../tutorials/uno-r4-wifi/led-matrix.md" in rewritten
    assert "../tutorials/uno-r4-wifi/assets/img.png" in rewritten
