
import asyncio
import hashlib
import json
import logging
import os
import random
import re
import shutil
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, TypedDict, Union
from urllib.parse import parse_qsl, unquote, urlencode, urljoin, urlparse, urlunparse

import aiofiles
import aiohttp
from bs4 import BeautifulSoup, NavigableString, Tag
from pydantic import BaseModel, Field
try:
    import yaml # pyright: ignore[reportMissingModuleSource]
except Exception:  # pragma: no cover - optional dependency fallback
    yaml = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AsyncArduinoMirror")


class AssetRecord(TypedDict, total=False):
    label: str
    source_url: str
    normalized_url: str
    filename: str
    local_path: str
    content_type: str
    size_bytes: int
    sha256: str
    downloaded_at: str


class PageRecord(TypedDict, total=False):
    title: str
    normalized_url: str
    page_type: str
    section: str
    breadcrumbs: List[str]
    description: str
    last_revision: str
    local_markdown_path: str
    local_html_path: str
    metadata_path: str
    asset_paths: List[str]
    outgoing_links: List[str]
    fetched_at: str
    content_hash: str
    source_url: str


class MirrorConfig(BaseModel):
    base_url: str = "https://docs.arduino.cc"
    start_sections: List[str] = Field(
        default_factory=lambda: [
            "https://docs.arduino.cc/hardware/",
            "https://docs.arduino.cc/software/",
            "https://docs.arduino.cc/programming/",
            "https://docs.arduino.cc/learn/",
        ]
    )
    data_dir: Path = Field(
        default_factory=lambda: Path(__file__).resolve().parents[2] / "data" / "arduino"
    )
    proxies: Union[List[str], str, None] = None
    network_mode: str = Field(default="direct", pattern="^(direct|proxy|mix)$")
    rate_limit_mb: Optional[float] = Field(default=None, gt=0)
    parallel: int = Field(default=6, ge=1, le=32)
    max_retries: int = Field(default=3, ge=1, le=10)
    request_timeout: int = Field(default=20, ge=1, le=300)
    connect_timeout: int = Field(default=10, ge=1, le=120)
    read_timeout: int = Field(default=20, ge=1, le=300)
    verify_ssl: bool = True
    user_agent: str = "AsyncArduinoMirror/1.0 (+https://docs.arduino.cc)"
    save_html_snapshot: bool = True
    save_images_from_pages: bool = True
    save_page_data_json: bool = False
    follow_tutorial_links: bool = True
    follow_reference_links: bool = True
    follow_learn_links: bool = True
    follow_query_strings: bool = False
    min_safe_space_gb: float = Field(default=3.0, gt=0)
    deduplicate_assets: bool = True
    localize_links: bool = True
    dry_run: bool = False
    show_progress: bool = True


class AsyncArduinoMirror:
    SUPPORTED_ASSET_EXTENSIONS = {
        ".pdf",
        ".zip",
        ".png",
        ".jpg",
        ".jpeg",
        ".svg",
        ".webp",
        ".txt",
        ".csv",
        ".json",
        ".ini",
        ".conf",
        ".xml",
    }
    HEADING_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6"}
    JUNK_SELECTORS = (
        "script,style,noscript,footer,header,aside,form,button,"
        "[aria-hidden='true'],.cookie,.cookies,.cookie-banner"
    )

    def __init__(self, config: MirrorConfig):
        self.cfg = config
        self.data_dir = self.cfg.data_dir.resolve()
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.proxies = self._load_proxies(self.cfg.proxies)
        self._rate_limit_bytes = (
            self.cfg.rate_limit_mb * 1024 * 1024 if self.cfg.rate_limit_mb else None
        )
        self._semaphore = asyncio.Semaphore(self.cfg.parallel)
        self._rate_lock = asyncio.Lock()
        self._rate_tokens = float(self._rate_limit_bytes or 0)
        self._rate_last_refill = time.monotonic()

        self._shared_dir = self._safe_path("_shared")
        self._assets_dir = self._safe_path("_shared", "assets")
        self._state_dir = self._safe_path("_shared", "state")
        self._cache_dir = self._safe_path("_shared", "cache")
        self._logs_dir = self._safe_path("_shared", "logs")
        self._indexes_dir = self._safe_path("_shared", "indexes")

        for section in (
            "hardware",
            "software",
            "programming",
            "learn",
            "tutorials",
            "language_reference",
        ):
            self._safe_path(section).mkdir(parents=True, exist_ok=True)
        for path in (
            self._shared_dir,
            self._assets_dir,
            self._state_dir,
            self._cache_dir,
            self._logs_dir,
            self._indexes_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

        self._visited_file = self._safe_path("_shared", "state", "visited_urls.json")
        self._url_map_file = self._safe_path("_shared", "state", "url_to_local_path.json")
        self._failures_file = self._safe_path("_shared", "state", "failures.json")
        self._pages_index_file = self._safe_path("_shared", "indexes", "pages_index.json")
        self._assets_index_file = self._safe_path("_shared", "indexes", "assets_index.json")
        self._hardware_index_file = self._safe_path(
            "_shared", "indexes", "hardware_index.json"
        )
        self._software_index_file = self._safe_path(
            "_shared", "indexes", "software_index.json"
        )
        self._programming_index_file = self._safe_path(
            "_shared", "indexes", "programming_index.json"
        )
        self._learn_index_file = self._safe_path("_shared", "indexes", "learn_index.json")

        self.visited_urls: set[str] = set()
        self.url_to_local_path: Dict[str, str] = {}
        self.pages_index: List[PageRecord] = []
        self.assets_index: List[AssetRecord] = []
        self.hardware_index: List[Dict[str, Any]] = []
        self.software_index: List[Dict[str, Any]] = []
        self.programming_index: List[Dict[str, Any]] = []
        self.learn_index: List[Dict[str, Any]] = []
        self.failures: Dict[str, str] = {}
        self._assets_by_url: Dict[str, AssetRecord] = {}

        self._load_state()

    # ---------------------------------------------------------------------
    # config/models helpers
    # ---------------------------------------------------------------------
    def _load_proxies(self, proxies: Union[List[str], str, None]) -> List[str]:
        if not proxies:
            return []
        if isinstance(proxies, list):
            return [p.strip() for p in proxies if str(p).strip()]
        proxy_path = Path(proxies)
        if proxy_path.exists():
            return [
                line.strip()
                for line in proxy_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
        return [str(proxies).strip()]

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _url_lookup_key(self, url: str) -> str:
        """Build a case-insensitive key for URL-to-local lookups."""
        return self._normalize_url(url).lower()

    async def _apply_rate_limit(self, chunk_size: int) -> None:
        """Apply global token-bucket throttling across concurrent downloads."""
        if not self._rate_limit_bytes or chunk_size <= 0:
            return

        while True:
            wait_time = 0.0
            async with self._rate_lock:
                now = time.monotonic()
                elapsed = max(0.0, now - self._rate_last_refill)
                refill = elapsed * self._rate_limit_bytes
                bucket_cap = float(self._rate_limit_bytes)
                self._rate_tokens = min(bucket_cap, self._rate_tokens + refill)
                self._rate_last_refill = now

                if self._rate_tokens >= chunk_size:
                    self._rate_tokens -= chunk_size
                    return

                missing = chunk_size - self._rate_tokens
                wait_time = missing / float(self._rate_limit_bytes)
                self._rate_tokens = 0.0

            await asyncio.sleep(wait_time)

    # ---------------------------------------------------------------------
    # path/url safety helpers
    # ---------------------------------------------------------------------
    def _safe_path(self, *parts: Union[str, Path]) -> Path:
        clean_parts: List[str] = []
        for raw_part in parts:
            tokenized = str(raw_part).replace("\\", "/")
            for token in tokenized.split("/"):
                if token in {"", "."}:
                    continue
                if token == "..":
                    raise ValueError(f"path traversal blocked: {raw_part}")
                clean_parts.append(token)
        root = self.data_dir.resolve()
        target = root.joinpath(*clean_parts).resolve()
        if not target.is_relative_to(root):
            raise ValueError(f"path traversal blocked: {target}")
        return target

    def _safe_slug(self, value: str, lower: bool = True, keep_dot: bool = False) -> str:
        value = (value or "").strip()
        value = re.sub(r"[\s/\\]+", "-", value)
        pattern = r"[^A-Za-z0-9_.-]+" if keep_dot else r"[^A-Za-z0-9_-]+"
        value = re.sub(pattern, "", value)
        value = re.sub(r"-{2,}", "-", value).strip("-_.")
        if lower:
            value = value.lower()
        return value or "item"

    def _safe_filename(self, filename: str) -> str:
        raw = unquote((filename or "").strip())
        if not raw:
            return f"asset-{int(time.time())}.bin"
        stem = Path(raw).stem
        suffix = Path(raw).suffix
        safe_stem = self._safe_slug(stem, lower=False)
        safe_suffix = re.sub(r"[^A-Za-z0-9.]", "", suffix)
        if safe_suffix and not safe_suffix.startswith("."):
            safe_suffix = f".{safe_suffix}"
        return f"{safe_stem}{safe_suffix}" if safe_suffix else safe_stem

    def _slug_to_dirname(self, name: str) -> str:
        special = {
            "wifi": "WiFi",
            "ble": "BLE",
            "usb": "USB",
            "hid": "HID",
            "rtc": "RTC",
            "dac": "DAC",
            "adc": "ADC",
            "i2c": "I2C",
            "spi": "SPI",
            "can": "CAN",
        }
        parts: List[str] = []
        for token in re.split(r"[\s/_-]+", (name or "").strip()):
            if not token:
                continue
            key = token.lower()
            if key in special:
                parts.append(special[key])
                continue
            if token.isdigit():
                parts.append(token)
                continue
            if re.fullmatch(r"[A-Z]\d+", token):
                parts.append(token)
                continue
            if token.isupper() and len(token) <= 4:
                parts.append(token.title())
                continue
            parts.append(token[:1].upper() + token[1:])
        return "_".join(parts) or "Item"

    def _normalize_url(self, url: str) -> str:
        if not url:
            return ""
        joined = urljoin(f"{self.cfg.base_url.rstrip('/')}/", url.strip())
        parsed = urlparse(joined)
        scheme = parsed.scheme or "https"
        netloc = parsed.netloc.lower()
        path = re.sub(r"/{2,}", "/", parsed.path or "/")
        if not path.startswith("/"):
            path = f"/{path}"
        query = ""
        if self.cfg.follow_query_strings and parsed.query:
            pairs = sorted(parse_qsl(parsed.query, keep_blank_values=True))
            query = urlencode(pairs)
        if path and not path.endswith("/") and not re.search(r"\.[A-Za-z0-9]{1,8}$", path):
            path = f"{path}/"
        return urlunparse((scheme, netloc, path, "", query, ""))

    def _is_asset_url(self, url: str) -> bool:
        path = urlparse(url).path.lower()
        return Path(path).suffix.lower() in self.SUPPORTED_ASSET_EXTENSIONS

    def _is_allowed_url(self, url: str) -> bool:
        if not url:
            return False
        normalized = self._normalize_url(url)
        parsed = urlparse(normalized)
        base_host = urlparse(self.cfg.base_url).netloc.lower()
        host = parsed.netloc.lower()
        path = parsed.path.lower()

        if self._is_asset_url(normalized):
            if host == base_host:
                return True
            return host.endswith("arduino.cc")

        if host != base_host:
            return False
        if path.startswith("/tutorials/") and not self.cfg.follow_tutorial_links:
            return False
        if path.startswith("/language-reference/") and not self.cfg.follow_reference_links:
            return False
        if path.startswith("/learn/") and not self.cfg.follow_learn_links:
            return False
        return True

    def _url_to_section(self, url: str) -> str:
        path = urlparse(self._normalize_url(url)).path.lower()
        if path.startswith("/hardware/"):
            return "hardware"
        if path.startswith("/software/"):
            return "software"
        if path.startswith("/programming/"):
            return "programming"
        if path.startswith("/learn/"):
            return "learn"
        if path.startswith("/tutorials/"):
            return "tutorials"
        if path.startswith("/language-reference/"):
            return "language_reference"
        first = path.strip("/").split("/", 1)[0].strip()
        if first:
            return self._safe_slug(first).replace("-", "_")
        return "learn"

    def _choose_proxy(self) -> Optional[str]:
        if self.cfg.network_mode == "direct" or not self.proxies:
            return None
        if self.cfg.network_mode == "proxy":
            return random.choice(self.proxies)
        if self.cfg.network_mode == "mix":
            return random.choice([None, random.choice(self.proxies)])
        return None

    def _format_size(self, size_bytes: int) -> str:
        size = float(size_bytes)
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if size < 1024:
                return f"{size:.2f} {unit}"
            size /= 1024
        return f"{size:.2f} PB"

    def _hash_bytes(self, payload: bytes) -> str:
        return hashlib.sha256(payload).hexdigest()

    def _hash_file(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(8192)
                if not chunk:
                    break
                digest.update(chunk)
        return digest.hexdigest()

    # ---------------------------------------------------------------------
    # network/download helpers
    # ---------------------------------------------------------------------
    def _request_kwargs(self) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {
            "headers": {"User-Agent": self.cfg.user_agent},
            "proxy": self._choose_proxy(),
            "ssl": self.cfg.verify_ssl,
        }
        try:
            kwargs["timeout"] = aiohttp.ClientTimeout(
                total=self.cfg.request_timeout,
                connect=self.cfg.connect_timeout,
                sock_read=self.cfg.read_timeout,
            )
        except Exception:
            kwargs["timeout"] = self.cfg.request_timeout
        return kwargs

    async def _fetch_text(self, session: aiohttp.ClientSession, url: str) -> Tuple[str, str]:
        normalized = self._normalize_url(url)
        last_error: Optional[Exception] = None
        for attempt in range(1, self.cfg.max_retries + 1):
            try:
                async with self._semaphore:
                    async with session.get(normalized, **self._request_kwargs()) as resp:
                        if hasattr(resp, "raise_for_status"):
                            resp.raise_for_status()
                        text = await resp.text()
                        content_type = (getattr(resp, "headers", {}) or {}).get(
                            "Content-Type", ""
                        )
                        return text, content_type
            except Exception as exc:
                last_error = exc
                if attempt < self.cfg.max_retries:
                    await asyncio.sleep(min(2 ** (attempt - 1), 3.0))
        if last_error is not None:
            raise last_error
        raise RuntimeError("unexpected fetch_text failure")

    async def _fetch_bytes(self, session: aiohttp.ClientSession, url: str) -> Tuple[bytes, str]:
        normalized = self._normalize_url(url)
        last_error: Optional[Exception] = None
        for attempt in range(1, self.cfg.max_retries + 1):
            try:
                async with self._semaphore:
                    async with session.get(normalized, **self._request_kwargs()) as resp:
                        if hasattr(resp, "raise_for_status"):
                            resp.raise_for_status()
                        if hasattr(resp, "read"):
                            payload = await resp.read()
                        else:
                            chunks: List[bytes] = []
                            async for chunk in resp.content.iter_chunked(65536):
                                chunks.append(chunk)
                            payload = b"".join(chunks)
                        content_type = (getattr(resp, "headers", {}) or {}).get(
                            "Content-Type", ""
                        )
                        return payload, content_type
            except Exception as exc:
                last_error = exc
                if attempt < self.cfg.max_retries:
                    await asyncio.sleep(min(2 ** (attempt - 1), 3.0))
        if last_error is not None:
            raise last_error
        raise RuntimeError("unexpected fetch_bytes failure")

    async def _head_or_get_meta(self, session: aiohttp.ClientSession, url: str) -> Dict[str, Any]:
        normalized = self._normalize_url(url)
        headers: Dict[str, str] = {}
        status = None

        if hasattr(session, "head"):
            try:
                async with self._semaphore:
                    async with session.head(normalized, **self._request_kwargs()) as resp:
                        status = getattr(resp, "status", None)
                        headers = dict((getattr(resp, "headers", {}) or {}))
                        if status and status < 400:
                            return {"status": status, "headers": headers}
            except Exception:
                pass

        try:
            async with self._semaphore:
                async with session.get(normalized, **self._request_kwargs()) as resp:
                    status = getattr(resp, "status", None)
                    headers = dict((getattr(resp, "headers", {}) or {}))
                    return {"status": status, "headers": headers}
        except Exception:
            return {"status": status, "headers": headers}

    async def _check_disk_space(self, required_bytes: int) -> None:
        if required_bytes <= 0:
            return
        _, _, free = shutil.disk_usage(self.data_dir)
        min_safe = int(self.cfg.min_safe_space_gb * 1024**3)
        if free - required_bytes < min_safe:
            raise IOError(
                "Not enough disk space to download safely. "
                f"free={self._format_size(free)}, need={self._format_size(required_bytes)}, "
                f"min_safe={self.cfg.min_safe_space_gb:.2f}GB"
            )

    async def _download_file(
        self, session: aiohttp.ClientSession, url: str, dest: Path
    ) -> Dict[str, Any]:
        """Download a file with retries, global rate-limit and temp-file safety."""
        normalized = self._normalize_url(url)
        if self.cfg.dry_run:
            return {"size_bytes": 0, "content_type": "", "normalized_url": normalized}

        dest.parent.mkdir(parents=True, exist_ok=True)
        temp_path = dest.with_name(f"{dest.name}.download.tmp")

        last_error: Optional[Exception] = None
        for attempt in range(1, self.cfg.max_retries + 1):
            downloaded = 0
            content_type = ""
            completed = False
            try:
                async with self._semaphore:
                    async with session.get(normalized, **self._request_kwargs()) as resp:
                        if hasattr(resp, "raise_for_status"):
                            resp.raise_for_status()
                        headers = dict((getattr(resp, "headers", {}) or {}))
                        content_type = headers.get("Content-Type", "")
                        size_header = headers.get("Content-Length")
                        if size_header and str(size_header).isdigit():
                            await self._check_disk_space(int(size_header))

                        async with aiofiles.open(temp_path, "wb") as handle:
                            async for chunk in resp.content.iter_chunked(65536):
                                if not chunk:
                                    continue
                                await self._apply_rate_limit(len(chunk))
                                await handle.write(chunk)
                                downloaded += len(chunk)

                if dest.exists():
                    dest.unlink()
                await asyncio.to_thread(os.replace, temp_path, dest)
                completed = True
                return {
                    "size_bytes": downloaded,
                    "content_type": content_type,
                    "normalized_url": normalized,
                }
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "download failed url=%s attempt=%s/%s exc=%s: %s",
                    normalized,
                    attempt,
                    self.cfg.max_retries,
                    type(exc).__name__,
                    exc,
                )
                if attempt < self.cfg.max_retries:
                    await asyncio.sleep(min(2 ** (attempt - 1), 3.0))
            finally:
                if not completed and temp_path.exists():
                    try:
                        temp_path.unlink()
                    except OSError:
                        pass

        if last_error is not None:
            raise last_error
        raise RuntimeError("unexpected download failure")

    def _filename_from_url_or_headers(self, url: str, headers: Dict[str, str]) -> str:
        disposition = headers.get("Content-Disposition") or headers.get("content-disposition") or ""
        if disposition:
            match = re.search(r"filename\*=UTF-8''([^;]+)", disposition, flags=re.IGNORECASE)
            if match:
                return self._safe_filename(unquote(match.group(1)))
            match = re.search(r'filename="?([^";]+)"?', disposition, flags=re.IGNORECASE)
            if match:
                return self._safe_filename(match.group(1))

        base = unquote(Path(urlparse(url).path).name)
        if base:
            return self._safe_filename(base)

        content_type = headers.get("Content-Type") or headers.get("content-type") or ""
        ext_map = {
            "application/pdf": ".pdf",
            "application/zip": ".zip",
            "image/png": ".png",
            "image/jpeg": ".jpg",
            "image/svg+xml": ".svg",
            "image/webp": ".webp",
            "application/json": ".json",
            "text/plain": ".txt",
            "text/csv": ".csv",
        }
        ext = ext_map.get(content_type.split(";")[0].strip().lower(), ".bin")
        return f"asset-{int(time.time())}{ext}"

    async def _download_asset(
        self,
        session: aiohttp.ClientSession,
        url: str,
        dest_dir: Path,
        expected_name: Optional[str] = None,
    ) -> AssetRecord:
        """Download and register one asset with retries and deduplication."""
        normalized = self._normalize_url(url)
        lookup_key = self._url_lookup_key(normalized)
        dest_dir = dest_dir.resolve()
        if not dest_dir.is_relative_to(self.data_dir):
            raise ValueError("asset destination must stay inside data_dir")
        dest_dir.mkdir(parents=True, exist_ok=True)

        if self.cfg.deduplicate_assets and lookup_key in self._assets_by_url:
            known = self._assets_by_url[lookup_key]
            known_path = Path(known["local_path"])
            if known_path.exists():
                return known

        meta = await self._head_or_get_meta(session, normalized)
        headers = meta.get("headers", {}) or {}
        filename = expected_name or self._filename_from_url_or_headers(normalized, headers)
        filename = self._safe_filename(filename)
        dest = self._safe_path(*dest_dir.relative_to(self.data_dir).parts, filename)
        temp_path = dest.with_name(f"{dest.name}.download.tmp")

        last_error: Optional[Exception] = None
        for attempt in range(1, self.cfg.max_retries + 1):
            try:
                download_meta = await self._download_file(session, normalized, dest)
                sha256 = self._hash_file(dest)
                size_bytes = dest.stat().st_size
                record: AssetRecord = {
                    "label": filename,
                    "source_url": url,
                    "normalized_url": normalized,
                    "filename": filename,
                    "local_path": str(dest),
                    "content_type": download_meta.get("content_type") or headers.get("Content-Type", ""),
                    "size_bytes": int(size_bytes),
                    "sha256": sha256,
                    "downloaded_at": self._now_iso(),
                }
                self._register_asset_record(record)
                return record
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "asset download failed url=%s attempt=%s/%s exc=%s: %s",
                    normalized,
                    attempt,
                    self.cfg.max_retries,
                    type(exc).__name__,
                    exc,
                )
                if attempt < self.cfg.max_retries:
                    await asyncio.sleep(min(2 ** (attempt - 1), 3.0))
            finally:
                if temp_path.exists():
                    try:
                        temp_path.unlink()
                    except OSError:
                        pass

        if last_error is not None:
            raise last_error
        raise RuntimeError(f"asset download failed for {normalized}")
    # ---------------------------------------------------------------------
    # storage / state helpers
    # ---------------------------------------------------------------------
    def _read_json(self, path: Path, default: Any) -> Any:
        if not path.exists():
            return default
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return default

    def _load_state(self) -> None:
        self.visited_urls = set(self._read_json(self._visited_file, []))
        self.url_to_local_path = dict(self._read_json(self._url_map_file, {}))
        self.failures = dict(self._read_json(self._failures_file, {}))
        self.pages_index = list(self._read_json(self._pages_index_file, []))
        self.assets_index = list(self._read_json(self._assets_index_file, []))
        self.hardware_index = list(self._read_json(self._hardware_index_file, []))
        self.software_index = list(self._read_json(self._software_index_file, []))
        self.programming_index = list(self._read_json(self._programming_index_file, []))
        self.learn_index = list(self._read_json(self._learn_index_file, []))
        self._assets_by_url = {}
        for rec in self.assets_index:
            normalized = rec.get("normalized_url")
            if normalized:
                self._assets_by_url[self._url_lookup_key(normalized)] = rec

    async def _write_json(self, path: Path, data: Any) -> None:
        """Atomically write JSON file via aiofiles."""
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_name(f"{path.name}.tmp")
        payload = json.dumps(data, indent=2, ensure_ascii=False)
        async with aiofiles.open(temp, "w", encoding="utf-8") as handle:
            await handle.write(payload)
        await asyncio.to_thread(os.replace, temp, path)

    async def _write_markdown(self, path: Path, text: str) -> None:
        """Atomically write markdown file via aiofiles."""
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_name(f"{path.name}.tmp")
        async with aiofiles.open(temp, "w", encoding="utf-8") as handle:
            await handle.write(text)
        await asyncio.to_thread(os.replace, temp, path)

    async def _write_html_snapshot(self, path: Path, html: str) -> None:
        """Atomically write HTML snapshot via aiofiles."""
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_name(f"{path.name}.tmp")
        async with aiofiles.open(temp, "w", encoding="utf-8") as handle:
            await handle.write(html)
        await asyncio.to_thread(os.replace, temp, path)

    async def _save_state_async(self) -> None:
        """Persist all state/index files asynchronously."""
        await self._write_json(self._visited_file, sorted(self.visited_urls))
        await self._write_json(self._url_map_file, self.url_to_local_path)
        await self._write_json(self._failures_file, self.failures)
        await self._write_json(self._pages_index_file, self.pages_index)
        await self._write_json(self._assets_index_file, self.assets_index)
        await self._write_json(self._hardware_index_file, self.hardware_index)
        await self._write_json(self._software_index_file, self.software_index)
        await self._write_json(self._programming_index_file, self.programming_index)
        await self._write_json(self._learn_index_file, self.learn_index)

    def _save_state(self) -> None:
        """Sync wrapper for state persistence."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(self._save_state_async())
            return
        loop.create_task(self._save_state_async())

    def _mark_visited(self, url: str) -> None:
        self.visited_urls.add(self._normalize_url(url))

    def _is_visited(self, url: str) -> bool:
        return self._normalize_url(url) in self.visited_urls

    def _upsert_by_key(
        self, collection: List[Dict[str, Any]], record: Dict[str, Any], key: str
    ) -> None:
        record_key = record.get(key)
        if record_key is None:
            collection.append(record)
            return
        for idx, existing in enumerate(collection):
            if existing.get(key) == record_key:
                collection[idx] = record
                return
        collection.append(record)

    def _register_asset_record(self, record: AssetRecord) -> None:
        rec = dict(record)
        self._upsert_by_key(self.assets_index, rec, "normalized_url")
        if rec.get("normalized_url"):
            key = self._url_lookup_key(rec["normalized_url"])
            self._assets_by_url[key] = rec  # type: ignore[index]

    def _register_index_record(self, record: PageRecord) -> None:
        page_rec = dict(record)
        self._upsert_by_key(self.pages_index, page_rec, "normalized_url")

        section = record.get("section", "")
        if section == "hardware":
            self._upsert_by_key(self.hardware_index, page_rec, "normalized_url")
        if section == "software":
            self._upsert_by_key(self.software_index, page_rec, "normalized_url")
        if section in {"programming", "language_reference"} or record.get("page_type") == "reference_page":
            self._upsert_by_key(self.programming_index, page_rec, "normalized_url")
        if section == "learn":
            self._upsert_by_key(self.learn_index, page_rec, "normalized_url")

    # ---------------------------------------------------------------------
    # detection / extraction helpers
    # ---------------------------------------------------------------------
    def _detect_page_type(self, url: str, soup: BeautifulSoup, html: str) -> str:
        """Detect page type using both URL patterns and structural/CSS hints."""
        normalized = self._normalize_url(url)
        path = urlparse(normalized).path.lower()

        if self._is_asset_url(normalized):
            return "asset"
        if path in {"/hardware/", "/software/", "/programming/", "/learn/"}:
            return "section_root"
        if path.startswith("/language-reference/"):
            return "reference_page"
        if path.startswith("/tutorials/"):
            return "tutorial_page"

        has_product_css = bool(
            soup.select_one(
                ".product, .product-page, .tech-specs, .downloadable-resources, [data-page-type='product']"
            )
        )
        has_reference_css = bool(
            soup.select_one(
                ".reference, .language-reference, .api-reference, [data-page-type='reference']"
            )
        )
        has_tutorial_css = bool(
            soup.select_one(".tutorial, .tutorial__content, [data-page-type='tutorial']")
        )
        if has_reference_css:
            return "reference_page"
        if has_tutorial_css and path.startswith("/tutorials/"):
            return "tutorial_page"

        text = soup.get_text(" ", strip=True).lower()
        has_product_signals = any(
            token in text
            for token in (
                "downloadable resources",
                "tech specs",
                "suggested libraries",
                "compatibility",
            )
        )
        has_features_signals = "features" in text and ("tutorials" in text or "specs" in text)
        if path.startswith("/hardware/") and (has_product_css or has_product_signals or has_features_signals):
            return "hardware_product"
        if path.startswith("/hardware/"):
            return "hardware_index_like"
        if path.startswith("/software/") and path != "/software/":
            return "software_tool"
        if path.startswith("/learn/"):
            return "article_page"
        if "last revision" in text and ("syntax" in text or "parameters" in text):
            return "reference_page"
        return "article_page"

    def _extract_breadcrumbs(self, soup: BeautifulSoup) -> List[str]:
        """Extract breadcrumb trail from common navigation selectors."""
        breadcrumbs: List[str] = []
        selectors = [
            "nav[aria-label*='breadcrumb' i] a",
            "nav[aria-label*='breadcrumb' i] span",
            ".breadcrumb a",
            ".breadcrumbs a",
            ".breadcrumbs span",
        ]
        for selector in selectors:
            for node in soup.select(selector):
                text = node.get_text(" ", strip=True)
                if text and text not in breadcrumbs:
                    breadcrumbs.append(text)
        return breadcrumbs

    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Extract page title from h1/title tags."""
        h1 = soup.find("h1")
        if isinstance(h1, Tag):
            title = h1.get_text(" ", strip=True)
            if title:
                return title
        title_tag = soup.find("title")
        if isinstance(title_tag, Tag):
            raw = title_tag.get_text(" ", strip=True)
            if raw:
                return raw.split("|")[0].strip()
        return "Untitled"

    def _extract_description(self, soup: BeautifulSoup) -> str:
        """Extract description from meta or first paragraph fallback."""
        meta = soup.find("meta", attrs={"name": "description"})
        if isinstance(meta, Tag):
            desc = meta.get("content", "").strip()
            if desc:
                return desc
        first_p = soup.find("p")
        if isinstance(first_p, Tag):
            return first_p.get_text(" ", strip=True)
        return ""

    def _extract_last_revision(self, soup: BeautifulSoup) -> str:
        """Extract last revision date string from page text."""
        text = soup.get_text(" ", strip=True)
        match = re.search(
            r"Last\s+revision\s*[:\-]?\s*([0-9]{4}-[0-9]{2}-[0-9]{2}|"
            r"[A-Za-z]{3,9}\s+\d{1,2},\s+[0-9]{4})",
            text,
            flags=re.IGNORECASE,
        )
        return match.group(1).strip() if match else ""

    def _extract_all_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extract all allowed internal links from a page."""
        links: List[str] = []
        seen: set[str] = set()
        for node in soup.find_all("a", href=True):
            href = node.get("href", "").strip()
            if not href or href.startswith("javascript:"):
                continue
            resolved = self._normalize_url(urljoin(base_url, href))
            if not resolved:
                continue
            if self._is_allowed_url(resolved) and resolved not in seen:
                seen.add(resolved)
                links.append(resolved)
        return links

    def _extract_asset_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extract downloadable and image asset links from a page."""
        links: List[str] = []
        seen: set[str] = set()

        for node in soup.find_all("a", href=True):
            href = node.get("href", "").strip()
            if not href:
                continue
            resolved = self._normalize_url(urljoin(base_url, href))
            if self._is_asset_url(resolved) and resolved not in seen:
                seen.add(resolved)
                links.append(resolved)

        if self.cfg.save_images_from_pages:
            for img in soup.find_all("img"):
                src = img.get("src", "").strip()
                if not src or src.startswith("data:"):
                    continue
                resolved = self._normalize_url(urljoin(base_url, src))
                if self._is_asset_url(resolved) and resolved not in seen:
                    seen.add(resolved)
                    links.append(resolved)
        return links

    def _find_heading(self, soup: BeautifulSoup, patterns: Iterable[str]) -> Optional[Tag]:
        compiled = [re.compile(p, flags=re.IGNORECASE) for p in patterns]
        for heading in soup.find_all(self.HEADING_TAGS):
            if not isinstance(heading, Tag):
                continue
            text = heading.get_text(" ", strip=True)
            if any(p.search(text) for p in compiled):
                return heading
        return None

    def _collect_section_html(self, heading: Tag) -> str:
        level = int(heading.name[1]) if heading.name in self.HEADING_TAGS else 6
        parts = [str(heading)]
        for sibling in heading.next_siblings:
            if isinstance(sibling, Tag) and sibling.name in self.HEADING_TAGS:
                sibling_level = int(sibling.name[1])
                if sibling_level <= level:
                    break
            if isinstance(sibling, Tag):
                parts.append(str(sibling))
            elif isinstance(sibling, NavigableString):
                text = sibling.strip()
                if text:
                    parts.append(f"<p>{text}</p>")
        return "<section>" + "".join(parts) + "</section>"

    def _extract_downloadable_resources(
        self, soup: BeautifulSoup, base_url: str
    ) -> List[Dict[str, str]]:
        """Extract links from downloadable resources section."""
        resources: List[Dict[str, str]] = []
        seen: set[str] = set()

        scope_nodes: List[Tag] = []
        heading = self._find_heading(soup, [r"downloadable resources", r"\bdownloads?\b"])
        if heading is not None:
            scope_nodes.append(heading.parent if isinstance(heading.parent, Tag) else heading)

        if not scope_nodes:
            scope_nodes.append(soup)

        for scope in scope_nodes:
            for anchor in scope.find_all("a", href=True):
                href = anchor.get("href", "").strip()
                if not href:
                    continue
                resolved = self._normalize_url(urljoin(base_url, href))
                if not self._is_asset_url(resolved):
                    continue
                if resolved in seen:
                    continue
                seen.add(resolved)
                filename = unquote(Path(urlparse(resolved).path).name) or self._safe_slug(
                    anchor.get_text(" ", strip=True)
                )
                resources.append(
                    {
                        "label": anchor.get_text(" ", strip=True) or filename,
                        "url": resolved,
                        "filename": filename,
                    }
                )
        return resources

    def _extract_feature_cards(
        self, soup: BeautifulSoup, base_url: str
    ) -> List[Dict[str, str]]:
        """Extract feature cards with title/description/tag/url fields."""
        cards: List[Dict[str, str]] = []
        seen: set[str] = set()
        scopes: List[Tag] = []

        feature_heading = self._find_heading(soup, [r"\bfeatures?\b"])
        if feature_heading is not None:
            parent = feature_heading.parent if isinstance(feature_heading.parent, Tag) else feature_heading
            scopes.append(parent)

        if not scopes:
            for node in soup.find_all(class_=re.compile(r"feature|card", flags=re.IGNORECASE)):
                if isinstance(node, Tag):
                    scopes.append(node)
            if not scopes:
                scopes = [soup]

        for scope in scopes:
            for anchor in scope.find_all("a", href=True):
                href = anchor.get("href", "").strip()
                if not href:
                    continue
                resolved = self._normalize_url(urljoin(base_url, href))
                if not self._is_allowed_url(resolved):
                    continue
                title_node = (
                    anchor.find(["h2", "h3", "h4", "strong", "span"]) or anchor
                )
                title = title_node.get_text(" ", strip=True)
                description_node = anchor.find("p")
                description = (
                    description_node.get_text(" ", strip=True)
                    if isinstance(description_node, Tag)
                    else ""
                )
                tag_node = anchor.find(class_=re.compile(r"tag|category", re.IGNORECASE))
                tag_text = tag_node.get_text(" ", strip=True) if isinstance(tag_node, Tag) else ""
                dedupe_key = f"{title.lower()}::{resolved}"
                if title and dedupe_key not in seen:
                    seen.add(dedupe_key)
                    cards.append(
                        {
                            "title": title,
                            "description": description,
                            "tag": tag_text,
                            "url": resolved,
                        }
                    )
        return cards

    def _extract_tech_specs(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract Tech Specs section and convert to markdown."""
        heading = self._find_heading(soup, [r"tech specs?", r"technical specifications?"])
        if heading is None:
            return {"found": False, "markdown": "", "html": ""}
        section_html = self._collect_section_html(heading)
        section_soup = BeautifulSoup(section_html, "html.parser")
        md = self._html_to_markdown(section_soup, page_url="", local_dir=self.data_dir)
        return {"found": True, "markdown": md, "html": section_html}

    def _extract_named_section_markdown(
        self, soup: BeautifulSoup, heading_patterns: Iterable[str]
    ) -> str:
        """Extract any named section by heading regex and convert to markdown."""
        heading = self._find_heading(soup, heading_patterns)
        if heading is None:
            return ""
        html = self._collect_section_html(heading)
        return self._html_to_markdown(BeautifulSoup(html, "html.parser"), "", self.data_dir)

    # ---------------------------------------------------------------------
    # markdown conversion helpers
    # ---------------------------------------------------------------------
    def _inline_to_markdown(self, node: Any) -> str:
        if isinstance(node, NavigableString):
            return str(node)
        if not isinstance(node, Tag):
            return ""

        name = node.name.lower()
        if name in {"script", "style", "noscript"}:
            return ""
        if name == "br":
            return "  \n"
        if name == "code":
            return f"`{node.get_text(' ', strip=True)}`"
        if name in {"strong", "b"}:
            value = "".join(self._inline_to_markdown(c) for c in node.children).strip()
            return f"**{value}**" if value else ""
        if name in {"em", "i"}:
            value = "".join(self._inline_to_markdown(c) for c in node.children).strip()
            return f"*{value}*" if value else ""
        if name == "a":
            href = node.get("href", "").strip()
            label = "".join(self._inline_to_markdown(c) for c in node.children).strip()
            if not label:
                label = node.get_text(" ", strip=True)
            if href:
                normalized = self._normalize_url(href)
                return f"[{label}]({normalized})"
            return label
        if name == "img":
            src = node.get("src", "").strip()
            if not src or src.startswith("data:"):
                return ""
            alt = node.get("alt", "").strip()
            normalized = self._normalize_url(src)
            return f"![{alt}]({normalized})"

        return "".join(self._inline_to_markdown(child) for child in node.children)

    def _convert_table_to_markdown(self, table_tag: Tag) -> str:
        """Convert HTML table into GitHub-compatible markdown table."""
        rows: List[List[str]] = []
        for tr in table_tag.find_all("tr"):
            row: List[str] = []
            cells = tr.find_all(["th", "td"])
            for cell in cells:
                text = self._inline_to_markdown(cell)
                text = re.sub(r"\s+", " ", text).strip().replace("|", r"\|")
                row.append(text)
            if row:
                rows.append(row)
        if not rows:
            return ""

        col_count = max(len(row) for row in rows)
        padded = [row + [""] * (col_count - len(row)) for row in rows]
        header = padded[0]
        sep = ["---"] * col_count
        lines = [
            "| " + " | ".join(header) + " |",
            "| " + " | ".join(sep) + " |",
        ]
        for row in padded[1:]:
            lines.append("| " + " | ".join(row) + " |")
        return "\n".join(lines)

    def _convert_code_block(self, pre_tag: Tag) -> str:
        """Convert <pre><code> block to fenced markdown code block."""
        language = ""
        code_tag = pre_tag.find("code")
        if isinstance(code_tag, Tag):
            for cls in code_tag.get("class", []):
                if cls.startswith("language-"):
                    language = cls.split("language-", 1)[1]
                    break
        code_text = pre_tag.get_text("\n", strip=False).strip("\n")
        return f"```{language}\n{code_text}\n```"

    def _convert_list_to_markdown(self, list_tag: Tag, level: int = 0) -> str:
        """Convert nested HTML lists to markdown lists."""
        ordered = list_tag.name.lower() == "ol"
        lines: List[str] = []
        for idx, item in enumerate(list_tag.find_all("li", recursive=False), start=1):
            marker = f"{idx}." if ordered else "-"
            text_parts: List[str] = []
            nested_parts: List[Tag] = []
            for child in item.children:
                if isinstance(child, Tag) and child.name in {"ul", "ol"}:
                    nested_parts.append(child)
                    continue
                text_parts.append(self._inline_to_markdown(child).strip())
            text = re.sub(r"\s+", " ", " ".join([p for p in text_parts if p]).strip())
            lines.append(f"{'  ' * level}{marker} {text}".rstrip())
            for nested in nested_parts:
                nested_md = self._convert_list_to_markdown(nested, level + 1).rstrip()
                if nested_md:
                    lines.append(nested_md)
        return "\n".join(lines)

    def _node_to_markdown(self, node: Any, level: int = 0) -> str:
        if isinstance(node, NavigableString):
            text = str(node).strip()
            return f"{text}\n\n" if text else ""
        if not isinstance(node, Tag):
            return ""
        name = node.name.lower()
        if name in {"script", "style", "noscript"}:
            return ""

        if name in self.HEADING_TAGS:
            heading_level = int(name[1])
            title = re.sub(r"\s+", " ", node.get_text(" ", strip=True))
            return f"{'#' * heading_level} {title}\n\n" if title else ""
        if name == "p":
            text = re.sub(r"\s+", " ", self._inline_to_markdown(node).strip())
            return f"{text}\n\n" if text else ""
        if name in {"ul", "ol"}:
            body = self._convert_list_to_markdown(node, level)
            return f"{body}\n\n" if body else ""
        if name == "table":
            body = self._convert_table_to_markdown(node)
            return f"{body}\n\n" if body else ""
        if name == "pre":
            return f"{self._convert_code_block(node)}\n\n"
        if name == "blockquote":
            text = self._html_to_markdown(node, "", self.data_dir).strip()
            lines = [f"> {line}".rstrip() if line else ">" for line in text.splitlines()]
            return "\n".join(lines).rstrip() + "\n\n"
        if name == "hr":
            return "---\n\n"
        if name == "figure":
            img = node.find("img")
            caption = node.find("figcaption")
            image_md = self._inline_to_markdown(img) if isinstance(img, Tag) else ""
            cap_text = caption.get_text(" ", strip=True) if isinstance(caption, Tag) else ""
            if image_md and cap_text:
                return f"{image_md}\n\n*{cap_text}*\n\n"
            return f"{image_md}\n\n" if image_md else ""
        if name == "img":
            image_md = self._inline_to_markdown(node)
            return f"{image_md}\n\n" if image_md else ""
        if name in {"div", "section", "article", "main", "li"}:
            blocks: List[str] = []
            for child in node.children:
                blocks.append(self._node_to_markdown(child, level))
            return "".join(blocks)

        inline = re.sub(r"\s+", " ", self._inline_to_markdown(node).strip())
        return f"{inline}\n\n" if inline else ""

    def _normalize_markdown(self, text: str) -> str:
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        text = re.sub(r"[ \t]+\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip() + "\n"

    def _rewrite_links_to_local(self, text: str, current_local_path: Path) -> str:
        """Rewrite markdown links to local files with case-insensitive URL matching."""
        pattern = re.compile(r"(!?\[[^\]]*\]\()([^)]+)(\))")
        pages_lookup = {
            self._url_lookup_key(url): path for url, path in self.url_to_local_path.items()
        }

        def _replace(match: re.Match[str]) -> str:
            prefix, raw_link, suffix = match.groups()
            link = raw_link.strip()
            if link.startswith("#") or link.startswith("mailto:"):
                return match.group(0)

            normalized = self._normalize_url(link)
            key = self._url_lookup_key(normalized)
            if key in pages_lookup:
                target = Path(pages_lookup[key])
                rel = os.path.relpath(target, current_local_path.parent).replace("\\", "/")
                return f"{prefix}{rel}{suffix}"

            known_asset = self._assets_by_url.get(key)
            if known_asset and known_asset.get("local_path"):
                rel = os.path.relpath(
                    known_asset["local_path"], current_local_path.parent
                ).replace("\\", "/")
                return f"{prefix}{rel}{suffix}"

            return match.group(0)

        return pattern.sub(_replace, text)

    def _content_node(self, soup: BeautifulSoup) -> Tag:
        for selector in ("main", "article", "[class*='content']", "[class*='article']"):
            node = soup.select_one(selector)
            if isinstance(node, Tag):
                for junk in node.select(self.JUNK_SELECTORS):
                    junk.decompose()
                return node
        if isinstance(soup.body, Tag):
            body = soup.body
            for junk in body.select(self.JUNK_SELECTORS):
                junk.decompose()
            return body
        return soup

    def _html_to_markdown(self, content_node: Any, page_url: str, local_dir: Path) -> str:
        if isinstance(content_node, (str, bytes)):
            soup = BeautifulSoup(content_node, "html.parser")
            node: Any = self._content_node(soup)
        elif isinstance(content_node, BeautifulSoup):
            node = self._content_node(content_node)
        else:
            node = content_node

        blocks: List[str] = []
        if isinstance(node, Tag):
            for child in node.children:
                blocks.append(self._node_to_markdown(child))
        else:
            blocks.append(self._node_to_markdown(node))
        markdown = self._normalize_markdown("".join(blocks))
        return markdown
    # ---------------------------------------------------------------------
    # section/page parser helpers
    # ---------------------------------------------------------------------
    def _resolve_page_paths(
        self, url: str, page_type: str, title: str
    ) -> Tuple[Path, Path, Path, str]:
        normalized = self._normalize_url(url)
        section = self._url_to_section(normalized)
        parsed = urlparse(normalized)
        segments = [seg for seg in parsed.path.strip("/").split("/") if seg]

        if page_type == "section_root":
            base = self._safe_path(section)
            md_path = self._safe_path(*base.relative_to(self.data_dir).parts, "_index.md")
        elif page_type == "hardware_product":
            dirname = self._slug_to_dirname(title or (segments[-1] if segments else "hardware-item"))
            base = self._safe_path("hardware", dirname)
            md_path = self._safe_path("hardware", dirname, "_index.md")
        elif page_type == "software_tool":
            dirname = self._slug_to_dirname(title or (segments[-1] if segments else "software-tool"))
            base = self._safe_path("software", dirname)
            md_path = self._safe_path("software", dirname, "_index.md")
        else:
            if section == "language_reference":
                tail = segments[1:] if segments and segments[0] == "language-reference" else segments
                if not tail:
                    md_path = self._safe_path("language_reference", "_index.md")
                else:
                    parts = [self._safe_slug(seg) for seg in tail[:-1]]
                    md_path = self._safe_path(
                        "language_reference",
                        *parts,
                        f"{self._safe_slug(tail[-1])}.md",
                    )
            elif section == "tutorials":
                tail = segments[1:] if segments and segments[0] == "tutorials" else segments
                if not tail:
                    md_path = self._safe_path("tutorials", "_index.md")
                else:
                    parts = [self._safe_slug(seg) for seg in tail[:-1]]
                    md_path = self._safe_path(
                        "tutorials",
                        *parts,
                        f"{self._safe_slug(tail[-1])}.md",
                    )
            else:
                section_token = section.replace("_", "-")
                tail = segments[1:] if segments and segments[0] == section_token else segments
                if not tail:
                    md_path = self._safe_path(section, "_index.md")
                else:
                    parts = [self._safe_slug(seg) for seg in tail[:-1]]
                    md_path = self._safe_path(section, *parts, f"{self._safe_slug(tail[-1])}.md")
            base = md_path.parent

        if md_path.name == "_index.md":
            json_path = md_path.with_name("_index.json")
            html_path = md_path.with_name("_index.html")
        else:
            json_path = md_path.with_suffix(".json")
            html_path = md_path.with_suffix(".html")
        return md_path, json_path, html_path, section

    def _predict_local_markdown_path(self, url: str) -> Path:
        normalized = self._normalize_url(url)
        section = self._url_to_section(normalized)
        if section == "hardware":
            page_type = "hardware_product"
        elif section == "software":
            page_type = "software_tool"
        elif section == "language_reference":
            page_type = "reference_page"
        elif section == "tutorials":
            page_type = "tutorial_page"
        elif section == "learn":
            page_type = "article_page"
        else:
            page_type = "article_page"
        md_path, _, _, _ = self._resolve_page_paths(normalized, page_type, "")
        return md_path

    def _build_front_matter(
        self,
        title: str,
        source_url: str,
        page_type: str,
        section: str,
        last_revision: str,
        fetched_at: str,
        description: str = "",
    ) -> str:
        """Build safe YAML front matter for markdown pages."""
        payload = {
            "title": title or "",
            "description": description or "",
            "source_url": source_url or "",
            "page_type": page_type or "",
            "section": section or "",
            "last_revision": last_revision or "",
            "fetched_at": fetched_at or "",
        }
        if yaml is not None:
            body = yaml.safe_dump(payload, allow_unicode=True, sort_keys=False).strip()
        else:
            lines = []
            for key, value in payload.items():
                safe_value = json.dumps(str(value), ensure_ascii=False)
                lines.append(f"{key}: {safe_value}")
            body = "\n".join(lines)
        return f"---\n{body}\n---\n\n"

    async def _parse_generic_page(
        self,
        session: aiohttp.ClientSession,
        url: str,
        soup: BeautifulSoup,
        html: str,
        page_type: str,
    ) -> Dict[str, Any]:
        """Parse generic page types into markdown/json and collect assets."""
        normalized = self._normalize_url(url)
        title = self._extract_title(soup)
        breadcrumbs = self._extract_breadcrumbs(soup)
        description = self._extract_description(soup)
        last_revision = self._extract_last_revision(soup)
        fetched_at = self._now_iso()

        md_path, json_path, html_path, section = self._resolve_page_paths(normalized, page_type, title)
        self.url_to_local_path[normalized] = str(md_path)

        content_node = self._content_node(soup)
        markdown_body = self._html_to_markdown(content_node, normalized, md_path.parent)
        if self.cfg.localize_links:
            markdown_body = self._rewrite_links_to_local(markdown_body, md_path)
        markdown = self._build_front_matter(
            title=title,
            source_url=normalized,
            page_type=page_type,
            section=section,
            last_revision=last_revision,
            fetched_at=fetched_at,
            description=description,
        ) + markdown_body

        asset_records: List[AssetRecord] = []
        if self.cfg.save_images_from_pages:
            for asset_url in self._extract_asset_links(soup, normalized):
                if not self._is_allowed_url(asset_url):
                    continue
                try:
                    asset = await self._download_asset(session, asset_url, md_path.parent / "assets")
                    asset_records.append(asset)
                except Exception as exc:
                    self.failures[asset_url] = f"asset download failed: {exc}"

        if not self.cfg.dry_run:
            await self._write_markdown(md_path, markdown)
            await self._write_json(
                json_path,
                {
                    "title": title,
                    "normalized_url": normalized,
                    "page_type": page_type,
                    "section": section,
                    "breadcrumbs": breadcrumbs,
                    "description": description,
                    "last_revision": last_revision,
                    "local_markdown_path": str(md_path),
                    "local_html_path": str(html_path) if self.cfg.save_html_snapshot else "",
                    "asset_paths": [a["local_path"] for a in asset_records],
                    "outgoing_links": self._extract_all_links(soup, normalized),
                    "fetched_at": fetched_at,
                    "content_hash": self._hash_bytes(markdown.encode("utf-8")),
                    "metadata_path": str(json_path),
                    "source_url": normalized,
                },
            )
            if self.cfg.save_html_snapshot:
                await self._write_html_snapshot(html_path, html)

        metadata: PageRecord = {
            "title": title,
            "normalized_url": normalized,
            "page_type": page_type,
            "section": section,
            "breadcrumbs": breadcrumbs,
            "description": description,
            "last_revision": last_revision,
            "local_markdown_path": str(md_path),
            "local_html_path": str(html_path) if self.cfg.save_html_snapshot else "",
            "metadata_path": str(json_path),
            "asset_paths": [a["local_path"] for a in asset_records],
            "outgoing_links": self._extract_all_links(soup, normalized),
            "fetched_at": fetched_at,
            "content_hash": self._hash_bytes(markdown.encode("utf-8")),
            "source_url": normalized,
        }
        self._register_index_record(metadata)
        return {
            "status": "ok",
            "normalized_url": normalized,
            "page_type": page_type,
            "section": section,
            "local_markdown_path": str(md_path),
            "links": metadata["outgoing_links"],
            "assets": asset_records,
            "metadata": metadata,
        }

    async def _parse_hardware_index(
        self, session: aiohttp.ClientSession, url: str, soup: BeautifulSoup, html: str
    ) -> Dict[str, Any]:
        """Parse hardware hub/index-like pages."""
        return await self._parse_generic_page(session, url, soup, html, "hardware_index_like")

    async def _process_hardware_downloads(
        self,
        session: aiohttp.ClientSession,
        soup: BeautifulSoup,
        page_url: str,
        files_dir: Path,
    ) -> List[AssetRecord]:
        """Download all downloadable resources from hardware page."""
        downloads_meta: List[AssetRecord] = []
        for resource in self._extract_downloadable_resources(soup, page_url):
            try:
                asset = await self._download_asset(
                    session,
                    resource["url"],
                    files_dir,
                    expected_name=resource.get("filename"),
                )
                asset["label"] = resource.get("label", asset.get("filename", ""))
                downloads_meta.append(asset)
            except Exception as exc:
                self.failures[resource["url"]] = f"download failed: {exc}"
        return downloads_meta

    async def _process_hardware_features(
        self,
        soup: BeautifulSoup,
        page_url: str,
        dirname: str,
        features_all_path: Path,
    ) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
        """Extract feature cards and save summary/feature markdown files."""
        feature_cards = self._extract_feature_cards(soup, page_url)
        feature_docs: List[Dict[str, str]] = []
        features_all_lines = ["# Features", ""]

        for card in feature_cards:
            card_title = card.get("title", "").strip() or "Feature"
            card_desc = card.get("description", "").strip()
            card_tag = card.get("tag", "").strip()
            card_url = card.get("url", "").strip()
            local_doc = ""
            if card_url:
                normalized_card_url = self._normalize_url(card_url)
                local_doc = self.url_to_local_path.get(normalized_card_url, "")
                if not local_doc:
                    local_doc = str(self._predict_local_markdown_path(normalized_card_url))

            features_all_lines.append(f"## {card_title}")
            if card_desc:
                features_all_lines.append(card_desc)
            if card_tag:
                features_all_lines.append(f"- Tag: {card_tag}")
            if card_url:
                features_all_lines.append(f"- Original URL: {card_url}")
            if local_doc:
                features_all_lines.append(f"- Local doc: {local_doc}")
            features_all_lines.append("")

            feature_slug = self._safe_slug(card_title)
            feature_path = self._safe_path(
                "hardware", dirname, "info", f"features_{feature_slug}.md"
            )
            feature_md = [
                f"# {card_title}",
                "",
                f"- Source feature title: {card_title}",
                f"- Linked doc URL: {card_url or 'N/A'}",
                f"- Local path linked page: {local_doc or 'N/A'}",
            ]
            if card_desc:
                feature_md.extend(["", card_desc])
            if not self.cfg.dry_run:
                await self._write_markdown(feature_path, "\n".join(feature_md).strip() + "\n")

            feature_docs.append(
                {
                    "title": card_title,
                    "feature_md": str(feature_path),
                    "linked_doc_url": card_url,
                    "linked_doc_local_md": local_doc,
                }
            )

        if not self.cfg.dry_run:
            await self._write_markdown(features_all_path, "\n".join(features_all_lines).strip() + "\n")
        return feature_cards, feature_docs

    async def _process_hardware_sections(
        self,
        soup: BeautifulSoup,
        tech_specs_path: Path,
        compatibility_path: Path,
        suggested_libraries_path: Path,
    ) -> Dict[str, str]:
        """Extract and persist Tech Specs/Compatibility/Suggested Libraries sections."""
        tech_specs = self._extract_tech_specs(soup)
        compatibility_md = self._extract_named_section_markdown(soup, [r"compatibility"])
        suggested_libraries_md = self._extract_named_section_markdown(
            soup, [r"suggested libraries?", r"libraries?"]
        )

        if not self.cfg.dry_run:
            if tech_specs.get("found"):
                await self._write_markdown(tech_specs_path, tech_specs["markdown"])
            if compatibility_md:
                await self._write_markdown(compatibility_path, compatibility_md)
            if suggested_libraries_md:
                await self._write_markdown(suggested_libraries_path, suggested_libraries_md)

        return {
            "tech_specs": str(tech_specs_path) if tech_specs.get("found") else "",
            "compatibility": str(compatibility_path) if compatibility_md else "",
            "suggested_libraries": str(suggested_libraries_path) if suggested_libraries_md else "",
        }

    async def _parse_hardware_product(
        self, session: aiohttp.ClientSession, url: str, soup: BeautifulSoup, html: str
    ) -> Dict[str, Any]:
        """Parse hardware product page and coordinate assets/features/spec sections."""
        normalized = self._normalize_url(url)
        title = self._extract_title(soup)
        breadcrumbs = self._extract_breadcrumbs(soup)
        description = self._extract_description(soup)
        last_revision = self._extract_last_revision(soup)
        fetched_at = self._now_iso()
        slug = self._safe_slug(Path(urlparse(normalized).path).name)
        dirname = self._slug_to_dirname(title or slug)

        product_dir = self._safe_path("hardware", dirname)
        files_dir = self._safe_path("hardware", dirname, "files")
        info_dir = self._safe_path("hardware", dirname, "info")
        product_dir.mkdir(parents=True, exist_ok=True)
        files_dir.mkdir(parents=True, exist_ok=True)
        info_dir.mkdir(parents=True, exist_ok=True)

        md_path = self._safe_path("hardware", dirname, "_index.md")
        json_path = self._safe_path("hardware", dirname, "page.json")
        html_path = self._safe_path("hardware", dirname, "page.html")
        hw_info_path = self._safe_path("hardware", dirname, "HardwareInfo.json")
        tech_specs_path = self._safe_path("hardware", dirname, "TechSpecs.md")
        compatibility_path = self._safe_path("hardware", dirname, "Compatibility.md")
        suggested_libraries_path = self._safe_path("hardware", dirname, "SuggestedLibraries.md")
        features_all_path = self._safe_path("hardware", dirname, "info", "features_all.md")

        self.url_to_local_path[normalized] = str(md_path)
        content_node = self._content_node(soup)
        markdown_body = self._html_to_markdown(content_node, normalized, product_dir)
        if self.cfg.localize_links:
            markdown_body = self._rewrite_links_to_local(markdown_body, md_path)
        markdown = self._build_front_matter(
            title=title,
            source_url=normalized,
            page_type="hardware_product",
            section="hardware",
            last_revision=last_revision,
            fetched_at=fetched_at,
            description=description,
        ) + markdown_body

        downloads_meta = await self._process_hardware_downloads(
            session=session,
            soup=soup,
            page_url=normalized,
            files_dir=files_dir,
        )
        feature_cards, feature_docs = await self._process_hardware_features(
            soup=soup,
            page_url=normalized,
            dirname=dirname,
            features_all_path=features_all_path,
        )
        section_paths = await self._process_hardware_sections(
            soup=soup,
            tech_specs_path=tech_specs_path,
            compatibility_path=compatibility_path,
            suggested_libraries_path=suggested_libraries_path,
        )

        if not self.cfg.dry_run:
            await self._write_markdown(md_path, markdown)
            await self._write_json(
                json_path,
                {
                    "title": title,
                    "normalized_url": normalized,
                    "page_type": "hardware_product",
                    "section": "hardware",
                    "breadcrumbs": breadcrumbs,
                    "description": description,
                    "last_revision": last_revision,
                    "local_markdown_path": str(md_path),
                    "local_html_path": str(html_path) if self.cfg.save_html_snapshot else "",
                    "asset_paths": [asset["local_path"] for asset in downloads_meta],
                    "outgoing_links": self._extract_all_links(soup, normalized),
                    "fetched_at": fetched_at,
                    "content_hash": self._hash_bytes(markdown.encode("utf-8")),
                    "metadata_path": str(json_path),
                    "source_url": normalized,
                },
            )
            if self.cfg.save_html_snapshot:
                await self._write_html_snapshot(html_path, html)

        hardware_info = {
            "name": title,
            "slug": slug,
            "section": "hardware",
            "family": title.split()[0] if title else "",
            "type": (
                "shield"
                if "shield" in title.lower()
                else "module"
                if "module" in title.lower()
                else "accessory"
                if "accessory" in title.lower()
                else "board"
            ),
            "source_url": normalized,
            "breadcrumbs": breadcrumbs,
            "description": description,
            "local_dir": str(product_dir),
            "markdown_files": {
                "index": str(md_path),
                "tech_specs": section_paths["tech_specs"],
                "compatibility": section_paths["compatibility"],
                "suggested_libraries": section_paths["suggested_libraries"],
                "features_all": str(features_all_path),
            },
            "downloads": downloads_meta,
            "feature_docs": feature_docs,
            "assets": downloads_meta,
            "fetched_at": fetched_at,
            "last_updated": last_revision,
        }
        if not self.cfg.dry_run:
            await self._write_json(hw_info_path, hardware_info)

        metadata: PageRecord = {
            "title": title,
            "normalized_url": normalized,
            "page_type": "hardware_product",
            "section": "hardware",
            "breadcrumbs": breadcrumbs,
            "description": description,
            "last_revision": last_revision,
            "local_markdown_path": str(md_path),
            "local_html_path": str(html_path) if self.cfg.save_html_snapshot else "",
            "metadata_path": str(json_path),
            "asset_paths": [asset["local_path"] for asset in downloads_meta],
            "outgoing_links": self._extract_all_links(soup, normalized),
            "fetched_at": fetched_at,
            "content_hash": self._hash_bytes(markdown.encode("utf-8")),
            "source_url": normalized,
        }
        self._register_index_record(metadata)
        self._upsert_by_key(
            self.hardware_index,
            {
                "title": title,
                "normalized_url": normalized,
                "page_type": "hardware_product",
                "local_path": str(md_path),
                "section": "hardware",
                "breadcrumbs": breadcrumbs,
                "fetched_at": fetched_at,
                "slug": slug,
                "local_dir": str(product_dir),
            },
            "normalized_url",
        )

        links = self._extract_all_links(soup, normalized)
        for card in feature_cards:
            if card.get("url"):
                links.append(self._normalize_url(card["url"]))

        return {
            "status": "ok",
            "normalized_url": normalized,
            "page_type": "hardware_product",
            "section": "hardware",
            "local_markdown_path": str(md_path),
            "links": sorted(set(links)),
            "assets": downloads_meta,
            "metadata": metadata,
            "hardware_info": hardware_info,
        }

    async def _parse_software_root(
        self, session: aiohttp.ClientSession, url: str, soup: BeautifulSoup, html: str
    ) -> Dict[str, Any]:
        """Parse /software root hub page."""
        return await self._parse_generic_page(session, url, soup, html, "section_root")

    async def _parse_software_tool_page(
        self, session: aiohttp.ClientSession, url: str, soup: BeautifulSoup, html: str
    ) -> Dict[str, Any]:
        """Parse software tool page and save SoftwareInfo metadata."""
        parsed = await self._parse_generic_page(session, url, soup, html, "software_tool")
        title = parsed["metadata"]["title"]
        normalized = parsed["metadata"]["normalized_url"]
        tool_dir = Path(parsed["metadata"]["local_markdown_path"]).parent
        tutorials = [
            link for link in parsed["metadata"]["outgoing_links"] if "/tutorials/" in link
        ]
        info = {
            "name": title,
            "source_url": normalized,
            "section": "software",
            "tutorial_links": tutorials,
            "local_dir": str(tool_dir),
            "fetched_at": parsed["metadata"]["fetched_at"],
        }
        if not self.cfg.dry_run:
            await self._write_json(tool_dir / "SoftwareInfo.json", info)
        return parsed

    async def _parse_programming_root(
        self, session: aiohttp.ClientSession, url: str, soup: BeautifulSoup, html: str
    ) -> Dict[str, Any]:
        """Parse /programming root hub page."""
        return await self._parse_generic_page(session, url, soup, html, "section_root")

    async def _parse_reference_page(
        self, session: aiohttp.ClientSession, url: str, soup: BeautifulSoup, html: str
    ) -> Dict[str, Any]:
        """Parse language/reference documentation page."""
        return await self._parse_generic_page(session, url, soup, html, "reference_page")

    async def _parse_learn_root(
        self, session: aiohttp.ClientSession, url: str, soup: BeautifulSoup, html: str
    ) -> Dict[str, Any]:
        """Parse /learn root page."""
        return await self._parse_generic_page(session, url, soup, html, "section_root")

    async def _parse_article_page(
        self, session: aiohttp.ClientSession, url: str, soup: BeautifulSoup, html: str
    ) -> Dict[str, Any]:
        """Parse generic article-like documentation page."""
        return await self._parse_generic_page(session, url, soup, html, "article_page")

    async def _parse_tutorial_page(
        self, session: aiohttp.ClientSession, url: str, soup: BeautifulSoup, html: str
    ) -> Dict[str, Any]:
        """Parse tutorial page into markdown/json."""
        return await self._parse_generic_page(session, url, soup, html, "tutorial_page")
    # ---------------------------------------------------------------------
    # crawl API
    # ---------------------------------------------------------------------
    @asynccontextmanager
    async def _managed_session(self, session: Optional[aiohttp.ClientSession]):
        if session is not None:
            yield session
            return
        async with aiohttp.ClientSession() as created:
            yield created

    async def crawl_url(
        self, session: Optional[aiohttp.ClientSession], url: str
    ) -> Dict[str, Any]:
        normalized = self._normalize_url(url)
        if not self._is_allowed_url(normalized):
            return {"status": "skipped", "reason": "not_allowed", "url": normalized}
        if self._is_visited(normalized):
            return {"status": "skipped", "reason": "visited", "url": normalized}

        async with self._managed_session(session) as active_session:
            logger.info("Crawling URL: %s", normalized)
            if self._is_asset_url(normalized):
                asset = await self._download_asset(active_session, normalized, self._assets_dir)
                self._mark_visited(normalized)
                await self._save_state_async()
                return {
                    "status": "ok",
                    "normalized_url": normalized,
                    "page_type": "asset",
                    "asset": asset,
                    "links": [],
                }

            try:
                html, _content_type = await self._fetch_text(active_session, normalized)
                soup = await asyncio.to_thread(BeautifulSoup, html, "html.parser")
                page_type = self._detect_page_type(normalized, soup, html)
                logger.info("Detected page type: %s -> %s", normalized, page_type)

                if page_type == "section_root":
                    section = self._url_to_section(normalized)
                    if section == "software":
                        result = await self._parse_software_root(active_session, normalized, soup, html)
                    elif section == "programming":
                        result = await self._parse_programming_root(active_session, normalized, soup, html)
                    elif section == "learn":
                        result = await self._parse_learn_root(active_session, normalized, soup, html)
                    elif section == "hardware":
                        result = await self._parse_hardware_index(active_session, normalized, soup, html)
                    else:
                        result = await self._parse_article_page(active_session, normalized, soup, html)
                elif page_type == "hardware_product":
                    result = await self._parse_hardware_product(active_session, normalized, soup, html)
                elif page_type == "hardware_index_like":
                    result = await self._parse_hardware_index(active_session, normalized, soup, html)
                elif page_type == "software_tool":
                    result = await self._parse_software_tool_page(active_session, normalized, soup, html)
                elif page_type == "reference_page":
                    result = await self._parse_reference_page(active_session, normalized, soup, html)
                elif page_type == "tutorial_page":
                    result = await self._parse_tutorial_page(active_session, normalized, soup, html)
                else:
                    result = await self._parse_article_page(active_session, normalized, soup, html)

                self._mark_visited(normalized)
                await self._save_state_async()
                return result
            except Exception as exc:
                self.failures[normalized] = str(exc)
                await self._save_state_async()
                logger.error("Failed URL: %s (%s)", normalized, exc)
                return {"status": "error", "url": normalized, "error": str(exc), "links": []}

    async def crawl_section(
        self, session: Optional[aiohttp.ClientSession], section: str
    ) -> Dict[str, Any]:
        section_token = section.strip().lower()
        if section_token.startswith("http://") or section_token.startswith("https://"):
            start_url = self._normalize_url(section_token)
            section_name = self._url_to_section(start_url)
        else:
            section_name = section_token.replace("-", "_")
            mapped = {
                "language_reference": "https://docs.arduino.cc/language-reference/",
                "tutorials": "https://docs.arduino.cc/tutorials/",
            }
            start_url = mapped.get(
                section_name, f"{self.cfg.base_url.rstrip('/')}/{section_name.strip('/')}/"
            )
            start_url = self._normalize_url(start_url)

        visited_in_run = 0
        downloaded_assets = 0
        queue: List[str] = [start_url]
        queued: set[str] = {start_url}

        async with self._managed_session(session) as active_session:
            while queue:
                current = queue.pop(0)
                queued.discard(current)
                if self._is_visited(current):
                    continue
                result = await self.crawl_url(active_session, current)
                if result.get("status") != "ok":
                    continue
                visited_in_run += 1
                downloaded_assets += len(result.get("assets", []))
                for link in result.get("links", []):
                    normalized = self._normalize_url(link)
                    if not self._is_allowed_url(normalized):
                        continue
                    if self._is_visited(normalized):
                        continue
                    if normalized in queued:
                        continue
                    queue.append(normalized)
                    queued.add(normalized)

        await self._save_state_async()
        return {
            "status": "ok",
            "section": section_name,
            "start_url": start_url,
            "visited_pages": visited_in_run,
            "downloaded_assets": downloaded_assets,
            "queued_left": len(queue),
        }

    async def crawl_all(self, session: Optional[aiohttp.ClientSession]) -> Dict[str, Any]:
        summary: Dict[str, Any] = {"status": "ok", "sections": [], "total_pages": 0}
        async with self._managed_session(session) as active_session:
            for start in self.cfg.start_sections:
                section_name = self._url_to_section(start)
                result = await self.crawl_section(active_session, section_name)
                summary["sections"].append(result)
                summary["total_pages"] += result.get("visited_pages", 0)
        return summary

    async def refresh(
        self,
        session: Optional[aiohttp.ClientSession],
        url: Optional[str] = None,
        section: Optional[str] = None,
    ) -> Dict[str, Any]:
        if url:
            normalized = self._normalize_url(url)
            self.visited_urls.discard(normalized)
            await self._save_state_async()
            return await self.crawl_url(session, normalized)
        if section:
            section_token = section.strip().lower().replace("-", "_")
            to_remove = [
                u
                for u in self.visited_urls
                if self._url_to_section(u) == section_token or section_token in u
            ]
            for target in to_remove:
                self.visited_urls.discard(target)
            await self._save_state_async()
            return await self.crawl_section(session, section_token)

        self.visited_urls.clear()
        await self._save_state_async()
        return await self.crawl_all(session)

    # ---------------------------------------------------------------------
    # verification / repair / info helpers
    # ---------------------------------------------------------------------
    def _verify_asset(self, record: Dict[str, Any]) -> Tuple[bool, str]:
        path = Path(record.get("local_path", ""))
        if not path.exists():
            return False, "missing_file"
        expected_size = record.get("size_bytes")
        if isinstance(expected_size, int) and expected_size >= 0 and path.stat().st_size != expected_size:
            return False, "size_mismatch"
        expected_hash = record.get("sha256")
        if expected_hash and self._hash_file(path) != expected_hash:
            return False, "hash_mismatch"
        return True, "ok"

    def _verify_markdown(self, path: Path) -> Tuple[bool, str]:
        if not path.exists():
            return False, "missing_markdown"
        if path.stat().st_size == 0:
            return False, "empty_markdown"
        return True, "ok"

    def _verify_json(self, path: Path) -> Tuple[bool, str]:
        if not path.exists():
            return False, "missing_json"
        try:
            json.loads(path.read_text(encoding="utf-8"))
            return True, "ok"
        except Exception:
            return False, "invalid_json"

    def _collect_local_stats(self) -> Dict[str, Any]:
        file_count = 0
        total_size = 0
        for file_path in self.data_dir.rglob("*"):
            if file_path.is_file():
                file_count += 1
                total_size += file_path.stat().st_size
        return {
            "root": str(self.data_dir),
            "files": file_count,
            "total_size_bytes": total_size,
            "total_size_human": self._format_size(total_size),
            "visited_urls": len(self.visited_urls),
            "pages_index": len(self.pages_index),
            "assets_index": len(self.assets_index),
        }

    async def verify(self, session: Optional[aiohttp.ClientSession] = None) -> Dict[str, Any]:
        report: Dict[str, Any] = {
            "status": "ok",
            "missing_markdown": [],
            "invalid_json": [],
            "missing_assets": [],
            "asset_problems": [],
            "hardware_missing_required": [],
            "stats": self._collect_local_stats(),
        }

        for page in self.pages_index:
            md_path = Path(page.get("local_markdown_path", ""))
            ok_md, md_reason = self._verify_markdown(md_path)
            if not ok_md:
                report["missing_markdown"].append(
                    {"url": page.get("normalized_url", ""), "path": str(md_path), "reason": md_reason}
                )
            json_path = Path(page.get("metadata_path", ""))
            ok_json, json_reason = self._verify_json(json_path)
            if not ok_json:
                report["invalid_json"].append(
                    {"url": page.get("normalized_url", ""), "path": str(json_path), "reason": json_reason}
                )

        for asset in self.assets_index:
            ok_asset, reason = self._verify_asset(asset)
            if not ok_asset:
                if reason == "missing_file":
                    report["missing_assets"].append(asset)
                else:
                    report["asset_problems"].append({"asset": asset, "reason": reason})

        for info_file in self._safe_path("hardware").rglob("HardwareInfo.json"):
            base = info_file.parent
            required = [
                base / "HardwareInfo.json",
                base / "TechSpecs.md",
                base / "info" / "features_all.md",
            ]
            missing = [str(p) for p in required if not p.exists()]
            if missing:
                report["hardware_missing_required"].append({"product_dir": str(base), "missing": missing})

        if (
            report["missing_markdown"]
            or report["invalid_json"]
            or report["missing_assets"]
            or report["asset_problems"]
            or report["hardware_missing_required"]
        ):
            report["status"] = "issues_found"
        return report

    async def repair(self, session: Optional[aiohttp.ClientSession]) -> Dict[str, Any]:
        report = await self.verify(session=session)
        repaired_assets = 0
        repaired_pages = 0

        async with self._managed_session(session) as active_session:
            for asset in report.get("missing_assets", []):
                source_url = asset.get("source_url") or asset.get("normalized_url")
                local_path = asset.get("local_path")
                if not source_url or not local_path:
                    continue
                try:
                    dest = Path(local_path)
                    await self._download_file(active_session, source_url, dest)
                    asset["size_bytes"] = dest.stat().st_size
                    asset["sha256"] = self._hash_file(dest)
                    repaired_assets += 1
                except Exception as exc:
                    self.failures[self._normalize_url(source_url)] = f"repair asset failed: {exc}"

            for missing_page in report.get("missing_markdown", []):
                target_url = missing_page.get("url")
                if not target_url:
                    continue
                result = await self.crawl_url(active_session, target_url)
                if result.get("status") == "ok":
                    repaired_pages += 1

        self._assets_by_url = {
            self._url_lookup_key(rec["normalized_url"]): rec
            for rec in self.assets_index
            if rec.get("normalized_url")
        }
        self.rebuild_indexes()
        await self._save_state_async()
        return {
            "status": "ok",
            "repaired_assets": repaired_assets,
            "repaired_pages": repaired_pages,
            "verify_before": report,
        }

    def rebuild_indexes(self) -> Dict[str, Any]:
        self.pages_index = []
        self.assets_index = []
        self.hardware_index = []
        self.software_index = []
        self.programming_index = []
        self.learn_index = []
        self.url_to_local_path = {}
        self._assets_by_url = {}

        for json_file in self.data_dir.rglob("*.json"):
            if json_file.is_relative_to(self._shared_dir):
                continue
            try:
                data = json.loads(json_file.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(data, dict):
                continue

            if data.get("normalized_url") and data.get("local_markdown_path"):
                record: PageRecord = {
                    "title": data.get("title", ""),
                    "normalized_url": data.get("normalized_url", ""),
                    "page_type": data.get("page_type", ""),
                    "section": data.get("section", ""),
                    "breadcrumbs": data.get("breadcrumbs", []),
                    "description": data.get("description", ""),
                    "last_revision": data.get("last_revision", ""),
                    "local_markdown_path": data.get("local_markdown_path", ""),
                    "local_html_path": data.get("local_html_path", ""),
                    "metadata_path": data.get("metadata_path", str(json_file)),
                    "asset_paths": data.get("asset_paths", []),
                    "outgoing_links": data.get("outgoing_links", []),
                    "fetched_at": data.get("fetched_at", ""),
                    "content_hash": data.get("content_hash", ""),
                    "source_url": data.get("source_url", data.get("normalized_url", "")),
                }
                self._register_index_record(record)
                self.url_to_local_path[record["normalized_url"]] = record["local_markdown_path"]

            for key in ("downloads", "assets"):
                for asset in data.get(key, []):
                    if isinstance(asset, dict) and asset.get("local_path"):
                        self._register_asset_record(asset)  # type: ignore[arg-type]

            if json_file.name == "HardwareInfo.json":
                self._upsert_by_key(
                    self.hardware_index,
                    {
                        "title": data.get("name", ""),
                        "normalized_url": data.get("source_url", ""),
                        "page_type": "hardware_product",
                        "section": "hardware",
                        "local_path": data.get("markdown_files", {}).get("index", ""),
                        "breadcrumbs": data.get("breadcrumbs", []),
                        "fetched_at": data.get("fetched_at", ""),
                        "slug": data.get("slug", ""),
                        "local_dir": data.get("local_dir", ""),
                    },
                    "normalized_url",
                )

        self._save_state()
        return {
            "status": "ok",
            "pages_index": len(self.pages_index),
            "assets_index": len(self.assets_index),
            "hardware_index": len(self.hardware_index),
            "software_index": len(self.software_index),
            "programming_index": len(self.programming_index),
            "learn_index": len(self.learn_index),
        }

    def get_info(self, target: Optional[str] = None) -> Dict[str, Any]:
        if not target:
            return {
                "mirror": "arduino",
                "stats": self._collect_local_stats(),
                "sections": {
                    "hardware": len(self.hardware_index),
                    "software": len(self.software_index),
                    "programming": len(self.programming_index),
                    "learn": len(self.learn_index),
                },
                "failures": len(self.failures),
            }

        key = target.strip()
        lower = key.lower().replace("-", "_")
        if lower in {"hardware", "software", "programming", "learn", "tutorials", "language_reference"}:
            section_path = self._safe_path(lower)
            return {
                "target": lower,
                "path": str(section_path),
                "exists": section_path.exists(),
                "items": sorted([p.name for p in section_path.iterdir()]) if section_path.exists() else [],
            }

        if key.startswith("http://") or key.startswith("https://"):
            normalized = self._normalize_url(key)
            for record in self.pages_index:
                if record.get("normalized_url") == normalized:
                    return {"target": normalized, "record": record}
            return {"target": normalized, "record": None}

        slug = self._safe_slug(key)
        for record in self.hardware_index + self.software_index + self.programming_index + self.learn_index:
            title_slug = self._safe_slug(record.get("title", ""))
            if slug == title_slug or slug == self._safe_slug(record.get("normalized_url", "")):
                return {"target": key, "record": record}
        return {"target": key, "record": None}

    def list_local(self, include_files: bool = False) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for section in (
            "hardware",
            "software",
            "programming",
            "learn",
            "tutorials",
            "language_reference",
            "_shared",
        ):
            root = self._safe_path(section)
            if not root.exists():
                result[section] = []
                continue
            if include_files:
                result[section] = sorted(
                    [
                        str(path.relative_to(root)).replace("\\", "/")
                        for path in root.rglob("*")
                        if path.is_file()
                    ]
                )
            else:
                result[section] = sorted(
                    [path.name for path in root.iterdir() if path.is_dir()]
                )
        return result

    def get_path(self, section: str, slug: Optional[str] = None) -> Optional[Path]:
        section_token = section.strip().lower().replace("-", "_")
        section_path = self._safe_path(section_token)
        if not section_path.exists():
            return None
        if not slug:
            return section_path

        slug_token = self._safe_slug(slug)
        if section_token == "hardware":
            dirname = self._slug_to_dirname(slug)
            candidate = section_path / dirname
            if candidate.exists():
                return candidate
        for path in section_path.rglob("*"):
            if self._safe_slug(path.stem) == slug_token or self._safe_slug(path.name) == slug_token:
                return path
        return None

    def delete_target(self, section: str, slug: Optional[str] = None) -> bool:
        target = self.get_path(section, slug)
        if not target:
            return False
        if target.resolve() == self.data_dir.resolve():
            raise ValueError("refusing to delete mirror root")
        if target.is_dir():
            shutil.rmtree(target, ignore_errors=False)
        else:
            target.unlink()

        def _keep_record(record: Dict[str, Any]) -> bool:
            local_path = record.get("local_markdown_path") or record.get("local_path") or ""
            if not local_path:
                return True
            try:
                return not Path(local_path).resolve().is_relative_to(target.resolve())
            except Exception:
                return True

        self.pages_index = [rec for rec in self.pages_index if _keep_record(rec)]
        self.assets_index = [rec for rec in self.assets_index if _keep_record(rec)]
        self.hardware_index = [rec for rec in self.hardware_index if _keep_record(rec)]
        self.software_index = [rec for rec in self.software_index if _keep_record(rec)]
        self.programming_index = [rec for rec in self.programming_index if _keep_record(rec)]
        self.learn_index = [rec for rec in self.learn_index if _keep_record(rec)]

        self.url_to_local_path = {
            url: path
            for url, path in self.url_to_local_path.items()
            if not Path(path).resolve().is_relative_to(target.resolve())
        }
        self._assets_by_url = {
            self._url_lookup_key(rec["normalized_url"]): rec
            for rec in self.assets_index
            if rec.get("normalized_url")
        }
        self._save_state()
        return True

    # ---------------------------------------------------------------------
    # demo/debug block
    # ---------------------------------------------------------------------
    async def _debug_menu(self) -> None:
        async with aiohttp.ClientSession() as session:
            while True:
                print("\n--- AsyncArduinoMirror ---")
                print("1. crawl_all")
                print("2. crawl_section")
                print("3. crawl_url")
                print("4. verify")
                print("5. rebuild_indexes")
                print("6. get_info")
                print("7. delete_target")
                print("0. exit")
                choice = input("Choice: ").strip()

                if choice == "1":
                    result = await self.crawl_all(session)
                    print(json.dumps(result, indent=2, ensure_ascii=False))
                elif choice == "2":
                    section = input("Section (hardware/software/programming/learn): ").strip()
                    result = await self.crawl_section(session, section)
                    print(json.dumps(result, indent=2, ensure_ascii=False))
                elif choice == "3":
                    url = input("URL: ").strip()
                    result = await self.crawl_url(session, url)
                    print(json.dumps(result, indent=2, ensure_ascii=False))
                elif choice == "4":
                    result = await self.verify(session=session)
                    print(json.dumps(result, indent=2, ensure_ascii=False))
                elif choice == "5":
                    result = self.rebuild_indexes()
                    print(json.dumps(result, indent=2, ensure_ascii=False))
                elif choice == "6":
                    target = input("Target (empty for summary): ").strip() or None
                    print(json.dumps(self.get_info(target), indent=2, ensure_ascii=False))
                elif choice == "7":
                    section = input("Section: ").strip()
                    slug = input("Slug (optional): ").strip() or None
                    ok = self.delete_target(section, slug)
                    print({"deleted": ok})
                elif choice == "0":
                    break


if __name__ == "__main__":
    try:
        cfg = MirrorConfig()
        mirror = AsyncArduinoMirror(cfg)
        asyncio.run(mirror._debug_menu())
    except KeyboardInterrupt:
        pass
