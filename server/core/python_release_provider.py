"""
Python release provider — fetches and caches version/file metadata from python.org.
This is the only component that knows about python.org HTML structure.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

import aiohttp
from bs4 import BeautifulSoup

if TYPE_CHECKING:
    from server.core.cache import CacheManager

logger = logging.getLogger("multiplex.python_mirror.provider")


# ---------------------------------------------------------------------------
# File classification patterns
# ---------------------------------------------------------------------------


@dataclass
class ReleaseFile:
    """Metadata for a single Python distribution file."""

    filename: str
    version: str
    os_type: str  # "windows" | "macos" | "source"
    arch: str  # "amd64" | "arm64" | ""
    file_type: str  # "installer" | "zip" | "pkg" | "tarball"
    download_url: str
    md5: str | None = None


# (compiled_pattern, os_type, arch, file_type)
_FILE_PATTERNS: list[tuple[re.Pattern[str], str, str, str]] = [
    (
        re.compile(r"python-(\d+\.\d+\.\d+(?:[abc]\d+)?)-amd64\.exe", re.IGNORECASE),
        "windows",
        "amd64",
        "installer",
    ),
    (
        re.compile(r"python-(\d+\.\d+\.\d+(?:[abc]\d+)?)-arm64\.exe", re.IGNORECASE),
        "windows",
        "arm64",
        "installer",
    ),
    (
        re.compile(r"python-(\d+\.\d+\.\d+(?:[abc]\d+)?)-amd64\.zip", re.IGNORECASE),
        "windows",
        "amd64",
        "zip",
    ),
    (
        re.compile(r"python-(\d+\.\d+\.\d+(?:[abc]\d+)?)-arm64\.zip", re.IGNORECASE),
        "windows",
        "arm64",
        "zip",
    ),
    # macOS pkg variants: macos11, macos13, macos13arm, etc.
    (
        re.compile(
            r"python-(\d+\.\d+\.\d+(?:[abc]\d+)?)-macos\w+arm\w*\.pkg", re.IGNORECASE
        ),
        "macos",
        "arm64",
        "pkg",
    ),
    (
        re.compile(r"python-(\d+\.\d+\.\d+(?:[abc]\d+)?)-macos\w+\.pkg", re.IGNORECASE),
        "macos",
        "amd64",
        "pkg",
    ),
    # Source tarball
    (
        re.compile(r"Python-(\d+\.\d+\.\d+(?:[abc]\d+)?)\.tar\.xz"),
        "source",
        "",
        "tarball",
    ),
    (re.compile(r"Python-(\d+\.\d+\.\d+(?:[abc]\d+)?)\.tgz"), "source", "", "tarball"),
]

_VERSION_RE = re.compile(r"^\d+\.\d+\.\d+")


def _is_stable_version(v: str) -> bool:
    return bool(re.match(r"^\d+\.\d+\.\d+$", v))


class PythonReleaseProvider:
    """
    Fetches version and file listings from https://www.python.org/ftp/python/.
    Results are cached in CacheManager. Falls back to cache on network errors.
    """

    CACHE_KEY_VERSIONS = "python_mirror:versions"
    CACHE_KEY_FILES = "python_mirror:files:{version}"

    def __init__(
        self,
        ftp_url: str = "https://www.python.org/ftp/python/",
        user_agent: str = "ASFES-Python-Mirror/1.0",
        verify_ssl: bool = True,
        cache_ttl_versions: int = 3600,
        cache_ttl_files: int = 7200,
    ) -> None:
        self._ftp_url = ftp_url.rstrip("/") + "/"
        self._user_agent = user_agent
        self._verify_ssl = verify_ssl
        self._ttl_versions = cache_ttl_versions
        self._ttl_files = cache_ttl_files

    def build_download_url(self, version: str, filename: str) -> str:
        """Construct direct download URL."""
        return f"{self._ftp_url}{version}/{filename}"

    def classify_file(self, filename: str, version: str) -> tuple[str, str, str] | None:
        """Return (os_type, arch, file_type) from FILE_PATTERNS, or None."""
        for pattern, os_type, arch, file_type in _FILE_PATTERNS:
            if pattern.fullmatch(filename):
                return os_type, arch, file_type
        return None

    # ------------------------------------------------------------------
    # Public async API
    # ------------------------------------------------------------------

    async def get_versions(
        self,
        session: aiohttp.ClientSession,
        cache: "CacheManager | None",
    ) -> list[str]:
        """
        Returns list of Python versions sorted descending (newest first).
        Strategy: cache → network → fallback empty list.
        """
        # 1. Try cache
        if cache is not None:
            cached = await cache.get(self.CACHE_KEY_VERSIONS)
            if cached is not None:
                logger.debug("python_versions cache_hit")
                return cached

        # 2. Fetch from python.org
        try:
            async with session.get(
                self._ftp_url,
                ssl=self._verify_ssl,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                resp.raise_for_status()
                html = await resp.text()
            versions = self._parse_versions_html(html)
            logger.debug("python_versions fetched count=%d", len(versions))
            if cache is not None and versions:
                await cache.set(
                    self.CACHE_KEY_VERSIONS, versions, ttl_seconds=self._ttl_versions
                )
            return versions
        except Exception as exc:
            logger.warning("python_versions_fetch_failed error=%s", exc)
            # Fallback: return cached even if expired
            if cache is not None:
                stale = await cache.get(self.CACHE_KEY_VERSIONS)
                if stale:
                    return stale
            return []

    async def get_files(
        self,
        session: aiohttp.ClientSession,
        version: str,
        cache: "CacheManager | None",
    ) -> list[ReleaseFile]:
        """
        Returns list of ReleaseFile for a given version.
        Fetches MD5SUMS page to populate md5 fields.
        """
        cache_key = self.CACHE_KEY_FILES.format(version=version)

        # 1. Try cache
        if cache is not None:
            cached = await cache.get(cache_key)
            if cached is not None:
                logger.debug("python_files cache_hit version=%s", version)
                return [ReleaseFile(**item) for item in cached]

        # 2. Fetch page
        version_url = f"{self._ftp_url}{version}/"
        try:
            async with session.get(
                version_url,
                ssl=self._verify_ssl,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 404:
                    return []
                resp.raise_for_status()
                html = await resp.text()
        except Exception as exc:
            logger.warning(
                "python_files_fetch_failed version=%s error=%s", version, exc
            )
            # Try stale cache
            if cache is not None:
                stale = await cache.get(cache_key)
                if stale:
                    return [ReleaseFile(**item) for item in stale]
            return []

        # 3. Parse filenames
        filenames = self._parse_version_page_files(html, version)

        # 4. Try to fetch MD5SUMS
        md5_map = await self._parse_md5sums(session, version)

        # 5. Build ReleaseFile list
        files: list[ReleaseFile] = []
        for fname in filenames:
            classified = self.classify_file(fname, version)
            if classified is None:
                continue
            os_type, arch, file_type = classified
            files.append(
                ReleaseFile(
                    filename=fname,
                    version=version,
                    os_type=os_type,
                    arch=arch,
                    file_type=file_type,
                    download_url=self.build_download_url(version, fname),
                    md5=md5_map.get(fname),
                )
            )

        # 6. Cache result
        if cache is not None and files:
            serializable = [
                {
                    "filename": f.filename,
                    "version": f.version,
                    "os_type": f.os_type,
                    "arch": f.arch,
                    "file_type": f.file_type,
                    "download_url": f.download_url,
                    "md5": f.md5,
                }
                for f in files
            ]
            await cache.set(cache_key, serializable, ttl_seconds=self._ttl_files)

        logger.debug("python_files fetched version=%s count=%d", version, len(files))
        return files

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _parse_versions_html(self, html: str) -> list[str]:
        """Extract version strings from the FTP index HTML."""
        soup = BeautifulSoup(html, "html.parser")
        versions: list[str] = []
        for link in soup.find_all("a"):
            href = str(link.get("href", "")).rstrip("/")
            if _VERSION_RE.match(href) and href not in versions:
                versions.append(href)
        # Sort descending by semver tuple
        versions.sort(
            key=lambda v: tuple(int(x) for x in v.split(".")[:3] if x.isdigit()),
            reverse=True,
        )
        return versions

    def _parse_version_page_files(self, html: str, version: str) -> list[str]:
        """Extract useful filenames from a version directory page."""
        soup = BeautifulSoup(html, "html.parser")
        files: list[str] = []
        for link in soup.find_all("a"):
            href = str(link.get("href", ""))
            # Only base filenames, no path components
            if "/" in href or not href:
                continue
            if self.classify_file(href, version) is not None:
                files.append(href)
        return files

    async def _parse_md5sums(
        self,
        session: aiohttp.ClientSession,
        version: str,
    ) -> dict[str, str]:
        """
        Fetch and parse MD5SUMS file from the version directory.
        Returns {filename: md5_hex} dict. Empty dict on failure.
        """
        url = f"{self._ftp_url}{version}/MD5SUMS"
        try:
            async with session.get(
                url,
                ssl=self._verify_ssl,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    return {}
                text = await resp.text()
            result: dict[str, str] = {}
            for line in text.splitlines():
                parts = line.strip().split(None, 1)
                if len(parts) == 2:
                    md5_hex, fname = parts
                    # Normalize: strip any path prefix
                    fname = fname.strip().lstrip("./")
                    result[fname] = md5_hex.strip()
            return result
        except Exception as exc:
            logger.debug("md5sums_fetch_failed version=%s error=%s", version, exc)
            return {}
