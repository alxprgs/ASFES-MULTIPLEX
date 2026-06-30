"""
Python Distribution Mirror Service.
Wraps PythonReleaseProvider + DownloadEngine with:
  - Background job tracking (asyncio.Task + MongoDB persistence)
  - SuggestionFilter (filter + priority sort, no scoring)
  - .mirror_cache.json management (cache-only, FS is source-of-truth)
  - Audit logging for destructive operations
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

import aiofiles
import aiohttp

from server.core.cache import CacheManager
from server.core.config import PythonMirrorConfig
from server.core.database import DatabaseManager
from server.core.logging import get_logger
from server.core.proxy_router import ProxyRouter
from server.core.python_release_provider import PythonReleaseProvider, ReleaseFile
from server.models import (
    PythonMirrorFile,
    PythonMirrorJobStatus,
    PythonMirrorListItem,
    PythonMirrorListResponse,
    PythonMirrorStatsResponse,
    PythonMirrorSuggestRequest,
    PythonMirrorSuggestResponse,
    PythonMirrorSuggestion,
    PythonMirrorVersion,
)

LOGGER = get_logger("multiplex.python_mirror")
PYTHON_MIRROR_JOBS = "python_mirror_jobs"
CACHE_FILENAME = ".mirror_cache.json"
CACHE_VERSION = 1


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _fmt_size(size_bytes: int) -> str:
    value = float(max(size_bytes, 0))
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024:
            return f"{value:.2f} {unit}"
        value /= 1024
    return f"{value:.2f} TB"


def _safe_path(base: Path, *parts: str) -> Path:
    """Validate path components and prevent traversal attacks."""
    cleaned: list[str] = []
    for part in parts:
        seg = str(part).strip()
        if not seg or seg in {".", ".."}:
            raise ValueError(f"Invalid path segment: {part!r}")
        if "/" in seg or "\\" in seg:
            raise ValueError(f"Path separators not allowed in segment: {part!r}")
        cleaned.append(seg)
    resolved_base = base.resolve()
    target = resolved_base.joinpath(*cleaned).resolve()
    if not target.is_relative_to(resolved_base):
        raise ValueError(f"Path traversal detected: {target}")
    return target


class IntegrityError(Exception):
    """Raised when file integrity verification fails."""


# ---------------------------------------------------------------------------
# Integrity verifiers
# ---------------------------------------------------------------------------

@dataclass
class VerifyResult:
    ok: bool
    strategy: str
    expected: str | None
    actual: str | None
    error: str | None = None


class VerifierStrategy(ABC):
    @abstractmethod
    async def verify(
        self,
        path: Path,
        expected_md5: str | None = None,
        expected_size: int | None = None,
    ) -> VerifyResult: ...


class ContentLengthVerifier(VerifierStrategy):
    async def verify(self, path, expected_md5=None, expected_size=None) -> VerifyResult:
        if not path.exists():
            return VerifyResult(ok=False, strategy="content_length",
                                expected=str(expected_size), actual=None,
                                error="file not found")
        actual = path.stat().st_size
        if expected_size is None:
            return VerifyResult(ok=True, strategy="content_length",
                                expected=None, actual=str(actual))
        return VerifyResult(
            ok=(actual == expected_size),
            strategy="content_length",
            expected=str(expected_size),
            actual=str(actual),
        )


class MD5Verifier(VerifierStrategy):
    async def verify(self, path, expected_md5=None, expected_size=None) -> VerifyResult:
        if not path.exists():
            return VerifyResult(ok=False, strategy="md5",
                                expected=expected_md5, actual=None,
                                error="file not found")
        actual = await asyncio.to_thread(self._calc_md5, path)
        if expected_md5 is None:
            return VerifyResult(ok=True, strategy="md5",
                                expected=None, actual=actual)
        return VerifyResult(
            ok=(actual == expected_md5),
            strategy="md5",
            expected=expected_md5,
            actual=actual,
        )

    @staticmethod
    def _calc_md5(path: Path) -> str:
        h = hashlib.md5()
        with path.open("rb") as fh:
            while chunk := fh.read(65536):
                h.update(chunk)
        return h.hexdigest()


class HybridVerifier(VerifierStrategy):
    """MD5 if expected_md5 known, ContentLength if expected_size, else existence check."""

    def __init__(self) -> None:
        self._md5 = MD5Verifier()
        self._cl = ContentLengthVerifier()

    async def verify(self, path, expected_md5=None, expected_size=None) -> VerifyResult:
        if expected_md5:
            return await self._md5.verify(path, expected_md5=expected_md5)
        if expected_size is not None:
            return await self._cl.verify(path, expected_size=expected_size)
        # Existence only
        return VerifyResult(
            ok=path.exists() and path.is_file(),
            strategy="hybrid",
            expected=None,
            actual=None,
        )


# ---------------------------------------------------------------------------
# Download Engine
# ---------------------------------------------------------------------------

class DownloadEngine:
    """
    Handles streaming file downloads.
    Computes MD5 inline (no double pass), verifies before atomic rename.
    Semaphore bounds concurrent downloads.
    """

    def __init__(self, config: PythonMirrorConfig, proxy_router: ProxyRouter) -> None:
        self.cfg = config
        self._proxy_router = proxy_router
        self._sem = asyncio.Semaphore(config.parallel)

    async def download(
        self,
        session: aiohttp.ClientSession,
        url: str,
        dest: Path,
        expected_md5: str | None = None,
        expected_size: int | None = None,
        on_progress: Callable[[int], None] | None = None,
    ) -> str:
        """
        Download url to dest atomically.
        Returns actual MD5 hex string.
        Raises IntegrityError if MD5 mismatch.
        """
        async with self._sem:
            dest.parent.mkdir(parents=True, exist_ok=True)
            temp = dest.with_suffix(dest.suffix + ".download.tmp")
            try:
                actual_md5 = await self._stream_to_temp(
                    session, url, temp, on_progress
                )
                if expected_md5 and actual_md5 != expected_md5:
                    raise IntegrityError(
                        f"MD5 mismatch for {dest.name}: "
                        f"expected={expected_md5} actual={actual_md5}"
                    )
                # Atomic rename
                await asyncio.to_thread(os.replace, temp, dest)
                LOGGER.debug("downloaded file=%s md5=%s", dest.name, actual_md5)
                return actual_md5
            except Exception:
                if temp.exists():
                    try:
                        temp.unlink()
                    except OSError:
                        pass
                raise

    async def _stream_to_temp(
        self,
        session: aiohttp.ClientSession,
        url: str,
        temp: Path,
        on_progress: Callable[[int], None] | None,
    ) -> str:
        proxy = self._proxy_router.choose_proxy()
        md5_h = hashlib.md5()
        downloaded = 0
        t0 = time.monotonic()

        # For fallback mode, try candidates in order
        candidates = self._proxy_router.get_fallback_candidates()
        last_exc: Exception | None = None

        for proxy_candidate in candidates:
            try:
                async with session.get(
                    url,
                    proxy=proxy_candidate,
                    ssl=self.cfg.verify_ssl,
                ) as resp:
                    resp.raise_for_status()
                    async with aiofiles.open(temp, "wb") as fh:
                        async for chunk in resp.content.iter_chunked(65536):
                            await fh.write(chunk)
                            md5_h.update(chunk)
                            downloaded += len(chunk)
                            if on_progress:
                                on_progress(downloaded)
                            if self.cfg.rate_limit_mb:
                                await self._throttle(downloaded, t0)
                return md5_h.hexdigest()
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_exc = exc
                LOGGER.debug("download_proxy_fail proxy=%s url=%s error=%s",
                             proxy_candidate, url, exc)
                # Reset temp file for retry
                try:
                    if temp.exists():
                        temp.unlink()
                except OSError:
                    pass
                md5_h = hashlib.md5()
                downloaded = 0
                t0 = time.monotonic()

        raise RuntimeError(f"All download attempts failed for {url}") from last_exc

    async def _throttle(self, downloaded: int, start: float) -> None:
        limit = self.cfg.rate_limit_mb * 1024 * 1024  # type: ignore[operator]
        elapsed = max(time.monotonic() - start, 0.001)
        expected = downloaded / limit
        if expected > elapsed:
            await asyncio.sleep(expected - elapsed)


# ---------------------------------------------------------------------------
# SuggestionFilter
# ---------------------------------------------------------------------------

FILE_TYPE_PRIORITY: dict[str, int] = {
    "installer": 3,
    "pkg": 2,
    "zip": 1,
    "tarball": 0,
}

_VERSION_PREFIX_RE = re.compile(r"^\d+(\.\d+)?$")
_VERSION_EXACT_RE = re.compile(r"^\d+\.\d+\.\d+")


class SuggestionFilter:
    """Simple filter + priority sort. No scoring/ML."""

    def __init__(self, provider: PythonReleaseProvider, data_dir: Path) -> None:
        self._provider = provider
        self._data_dir = data_dir

    async def suggest(
        self,
        session: aiohttp.ClientSession,
        cache: CacheManager | None,
        request: PythonMirrorSuggestRequest,
    ) -> PythonMirrorSuggestResponse:
        all_versions = await self._provider.get_versions(session, cache)
        target_versions = self._resolve_version_query(request.version_query, all_versions)

        candidates: list[PythonMirrorSuggestion] = []
        for version in target_versions[:5]:  # cap at 5 versions
            files = await self._provider.get_files(session, version, cache)
            for f in files:
                # Apply filters
                if request.os_type and f.os_type != request.os_type:
                    continue
                if request.arch and f.arch and f.arch != request.arch:
                    continue
                if request.file_type and f.file_type != request.file_type:
                    continue
                is_installed = (self._data_dir / version / f.filename).exists()
                candidates.append(PythonMirrorSuggestion(
                    version=version,
                    filename=f.filename,
                    os_type=f.os_type,
                    arch=f.arch,
                    file_type=f.file_type,
                    is_installed=is_installed,
                    download_url=f.download_url,
                ))

        # Sort: installed first, then by file_type priority
        candidates.sort(
            key=lambda c: (c.is_installed, FILE_TYPE_PRIORITY.get(c.file_type, 0)),
            reverse=True,
        )
        top = candidates[:10]
        return PythonMirrorSuggestResponse(
            suggestions=top,
            best_match=top[0] if top else None,
            resolved_version=target_versions[0] if target_versions else None,
        )

    def _resolve_version_query(
        self, query: str | None, all_versions: list[str]
    ) -> list[str]:
        """
        None / ""    → all versions (newest first)
        "latest"     → [all_versions[0]]
        "3.12"       → all 3.12.x versions
        "3.12.7"     → ["3.12.7"] if exists
        """
        if not all_versions:
            return []
        q = (query or "").strip().lower()
        if not q or q == "latest":
            return all_versions[:1] if q == "latest" else all_versions
        # Exact match
        if q in all_versions:
            return [q]
        # Prefix match: "3.12" → "3.12."
        prefix = q if q.endswith(".") else q + "."
        matched = [v for v in all_versions if v.startswith(prefix)]
        return matched if matched else []


# ---------------------------------------------------------------------------
# Job tracking
# ---------------------------------------------------------------------------

@dataclass
class PythonMirrorJob:
    job_id: str
    kind: str       # "install" | "verify" | "verify_all" | "repair"
    status: str     # "pending" | "running" | "done" | "error" | "cancelled"
    version: str | None
    total: int = 0
    done: int = 0
    failed: int = 0
    current_file: str | None = None
    message: str | None = None
    retry_count: int = 0
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None
    task: asyncio.Task | None = field(default=None, repr=False)  # type: ignore[type-arg]
    _start_time: float = field(default_factory=time.monotonic, repr=False)

    def to_status(self) -> PythonMirrorJobStatus:
        pct = (self.done / self.total * 100.0) if self.total > 0 else 0.0
        eta: float | None = None
        if self.status == "running" and self.done > 0 and self.total > self.done:
            elapsed = max(time.monotonic() - self._start_time, 0.001)
            rate = self.done / elapsed
            if rate > 0:
                eta = (self.total - self.done) / rate
        return PythonMirrorJobStatus(
            job_id=self.job_id,
            kind=self.kind,
            status=self.status,
            version=self.version,
            total=self.total,
            done=self.done,
            failed=self.failed,
            progress_pct=round(pct, 1),
            eta_seconds=round(eta, 0) if eta is not None else None,
            current_file=self.current_file,
            message=self.message,
            retry_count=self.retry_count,
            started_at=self.started_at.isoformat(),
            updated_at=self.updated_at.isoformat(),
            finished_at=self.finished_at.isoformat() if self.finished_at else None,
        )


# ---------------------------------------------------------------------------
# Main service
# ---------------------------------------------------------------------------

class PythonMirrorService:
    """
    High-level Python distribution mirror service.
    Source of truth: filesystem.
    .mirror_cache.json: cache only, rebuilt on mismatch.
    """

    def __init__(
        self,
        config: PythonMirrorConfig,
        db: DatabaseManager,
        audit: Any,
        cache: CacheManager | None = None,
    ) -> None:
        self.cfg = config
        self.cfg.data_dir.mkdir(parents=True, exist_ok=True)
        self._db = db
        self._audit = audit
        self._cache = cache
        self._proxy_router = ProxyRouter(
            proxies=config.proxies,
            mode=config.network_mode,  # type: ignore[arg-type]
            verify_ssl=config.verify_ssl,
        )
        self._provider = PythonReleaseProvider(
            ftp_url=config.ftp_url,
            user_agent=config.user_agent,
            verify_ssl=config.verify_ssl,
            cache_ttl_versions=config.cache_ttl_versions,
            cache_ttl_files=config.cache_ttl_files,
        )
        self._download_engine = DownloadEngine(config, self._proxy_router)
        self._suggest_filter = SuggestionFilter(self._provider, config.data_dir)
        self._jobs: dict[str, PythonMirrorJob] = {}
        self._session: aiohttp.ClientSession | None = None

    # ------------------------------------------------------------------
    # Session
    # ------------------------------------------------------------------

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(
                total=self.cfg.request_timeout,
                connect=self.cfg.connect_timeout,
            )
            self._session = aiohttp.ClientSession(
                headers={"User-Agent": self.cfg.user_agent},
                timeout=timeout,
                connector=aiohttp.TCPConnector(ssl=self.cfg.verify_ssl),
            )
        return self._session

    # ------------------------------------------------------------------
    # Disk guard
    # ------------------------------------------------------------------

    def _check_disk_space(self, required_bytes: int) -> None:
        _, _, free = shutil.disk_usage(self.cfg.data_dir)
        min_bytes = self.cfg.min_safe_space_gb * 1024 ** 3
        if (free - required_bytes) < min_bytes:
            raise IOError(
                f"Insufficient disk space: free={_fmt_size(int(free))}, "
                f"required={_fmt_size(required_bytes)}, "
                f"reserve={self.cfg.min_safe_space_gb}GB"
            )

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    async def get_stats(self) -> PythonMirrorStatsResponse:
        versions_count = 0
        files_count = 0
        total_size = 0
        if self.cfg.data_dir.exists():
            for ver_dir in self.cfg.data_dir.iterdir():
                if not ver_dir.is_dir() or ver_dir.name.startswith("."):
                    continue
                versions_count += 1
                for fp in ver_dir.iterdir():
                    if fp.is_file() and fp.name != CACHE_FILENAME:
                        files_count += 1
                        total_size += fp.stat().st_size

        _, _, free = shutil.disk_usage(self.cfg.data_dir)
        active_jobs = sum(
            1 for j in self._jobs.values() if j.status in {"pending", "running"}
        )
        return PythonMirrorStatsResponse(
            versions_count=versions_count,
            files_count=files_count,
            total_size_bytes=total_size,
            total_size_human=_fmt_size(total_size),
            disk_free_human=_fmt_size(int(free)),
            active_jobs=active_jobs,
        )

    # ------------------------------------------------------------------
    # Installed versions
    # ------------------------------------------------------------------

    async def list_versions(self) -> PythonMirrorListResponse:
        items: list[PythonMirrorListItem] = []
        if not self.cfg.data_dir.exists():
            return PythonMirrorListResponse(items=[], count=0)

        for ver_dir in sorted(self.cfg.data_dir.iterdir(), reverse=True):
            if not ver_dir.is_dir() or ver_dir.name.startswith("."):
                continue
            files = [
                fp for fp in ver_dir.iterdir()
                if fp.is_file() and fp.name != CACHE_FILENAME
            ]
            total_size = sum(fp.stat().st_size for fp in files)
            items.append(PythonMirrorListItem(
                version=ver_dir.name,
                files_count=len(files),
                total_size_human=_fmt_size(total_size),
            ))

        return PythonMirrorListResponse(items=items, count=len(items))

    async def get_version_info(self, version: str) -> PythonMirrorVersion | None:
        try:
            ver_dir = _safe_path(self.cfg.data_dir, version)
        except ValueError:
            return None
        if not ver_dir.exists() or not ver_dir.is_dir():
            return None

        cache_data = await self._read_version_cache(version)
        files: list[PythonMirrorFile] = []
        total_size = 0

        for fp in sorted(ver_dir.iterdir()):
            if not fp.is_file() or fp.name == CACHE_FILENAME:
                continue
            size = fp.stat().st_size
            total_size += size
            cached_meta = (cache_data or {}).get("files", {}).get(fp.name, {})
            classified = self._provider.classify_file(fp.name, version)
            if classified:
                os_type, arch, file_type = classified
            else:
                os_type, arch, file_type = "source", "", "tarball"

            files.append(PythonMirrorFile(
                name=fp.name,
                os_type=os_type,
                arch=arch,
                file_type=file_type,
                size_bytes=size,
                size_human=_fmt_size(size),
                md5=cached_meta.get("md5"),
                downloaded_at=cached_meta.get("downloaded_at"),
            ))

        return PythonMirrorVersion(
            version=version,
            files=files,
            files_count=len(files),
            total_size_bytes=total_size,
            total_size_human=_fmt_size(total_size),
        )

    # ------------------------------------------------------------------
    # Remote versions
    # ------------------------------------------------------------------

    async def get_remote_versions(self) -> list[str]:
        session = await self._get_session()
        return await self._provider.get_versions(session, self._cache)

    # ------------------------------------------------------------------
    # Operations — return Job status immediately
    # ------------------------------------------------------------------

    def install_version(
        self, version: str, actor: Any = None
    ) -> PythonMirrorJobStatus:
        job = self._create_job("install", version=version)
        job.task = asyncio.create_task(
            self._run_with_error_handling(job, self._run_install, version)
        )
        LOGGER.info("job_created kind=install version=%s job_id=%s", version, job.job_id)
        return job.to_status()

    def verify_version(self, version: str) -> PythonMirrorJobStatus:
        job = self._create_job("verify", version=version)
        job.task = asyncio.create_task(
            self._run_with_error_handling(job, self._run_verify, version)
        )
        return job.to_status()

    def verify_all(self) -> PythonMirrorJobStatus:
        job = self._create_job("verify_all", version=None)
        job.task = asyncio.create_task(
            self._run_with_error_handling(job, self._run_verify_all)
        )
        return job.to_status()

    def repair_version(self, version: str) -> PythonMirrorJobStatus:
        job = self._create_job("repair", version=version)
        job.task = asyncio.create_task(
            self._run_with_error_handling(job, self._run_repair, version)
        )
        return job.to_status()

    async def delete_version(
        self,
        version: str,
        actor: Any = None,
        request_meta: Any = None,
    ) -> bool:
        try:
            ver_dir = _safe_path(self.cfg.data_dir, version)
        except ValueError:
            return False
        if not ver_dir.exists():
            return False
        try:
            await asyncio.to_thread(shutil.rmtree, str(ver_dir))
            LOGGER.info("deleted version=%s", version)
            # Audit log
            try:
                if hasattr(self._audit, "log"):
                    await self._audit.log(
                        event_type="python_mirror.version.deleted",
                        actor=actor,
                        payload={"version": version},
                        context=request_meta,
                    )
            except Exception:
                pass
            return True
        except OSError as exc:
            LOGGER.error("delete_version_error version=%s error=%s", version, exc)
            return False

    # ------------------------------------------------------------------
    # Suggest
    # ------------------------------------------------------------------

    async def suggest(
        self, request: PythonMirrorSuggestRequest
    ) -> PythonMirrorSuggestResponse:
        session = await self._get_session()
        return await self._suggest_filter.suggest(session, self._cache, request)

    # ------------------------------------------------------------------
    # Jobs API
    # ------------------------------------------------------------------

    def get_job(self, job_id: str) -> PythonMirrorJob | None:
        return self._jobs.get(job_id)

    def get_active_jobs(self) -> list[PythonMirrorJob]:
        return [j for j in self._jobs.values() if j.status in {"pending", "running"}]

    def get_recent_jobs(self, limit: int = 20) -> list[PythonMirrorJob]:
        sorted_jobs = sorted(
            self._jobs.values(),
            key=lambda j: j.started_at,
            reverse=True,
        )
        return sorted_jobs[:limit]

    def cancel_job(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        if job is None or job.status not in {"pending", "running"}:
            return False
        if job.task and not job.task.done():
            job.task.cancel()
        job.status = "cancelled"
        job.updated_at = datetime.now(UTC)
        job.finished_at = datetime.now(UTC)
        asyncio.create_task(self._persist_job(job))
        return True

    def get_file_path(self, version: str, filename: str) -> Path | None:
        try:
            path = _safe_path(self.cfg.data_dir, version, filename)
        except ValueError:
            return None
        return path if path.exists() and path.is_file() else None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def recover_jobs(self) -> None:
        """On startup: mark any MongoDB jobs stuck in 'running' as 'error'."""
        try:
            collection = self._db.collection(PYTHON_MIRROR_JOBS)
            await collection.update_many(
                {"status": "running"},
                {"$set": {
                    "status": "error",
                    "message": "Interrupted by server restart",
                    "updated_at": datetime.now(UTC).isoformat(),
                    "finished_at": datetime.now(UTC).isoformat(),
                }},
            )
            LOGGER.info("python_mirror.recover_jobs completed")
        except Exception as exc:
            LOGGER.warning("python_mirror.recover_jobs failed error=%s", exc)

    async def shutdown(self) -> None:
        """Cancel all active tasks and close HTTP session."""
        for job in list(self._jobs.values()):
            if job.task and not job.task.done():
                job.task.cancel()
        if self._session and not self._session.closed:
            await self._session.close()
        LOGGER.info("python_mirror_service shutdown complete")

    # ------------------------------------------------------------------
    # Cache management (.mirror_cache.json)
    # ------------------------------------------------------------------

    async def _read_version_cache(self, version: str) -> dict | None:
        """Read .mirror_cache.json. Returns None if missing or invalid."""
        try:
            cache_path = _safe_path(self.cfg.data_dir, version, CACHE_FILENAME)
            if not cache_path.exists():
                return None
            text = await asyncio.to_thread(cache_path.read_text, "utf-8")
            data = json.loads(text)
            if data.get("_cache_version") != CACHE_VERSION:
                return None
            return data
        except Exception as exc:
            LOGGER.debug("read_version_cache error version=%s error=%s", version, exc)
            return None

    async def _rebuild_version_cache(self, version: str) -> dict:
        """Scan FS, rebuild cache dict, write atomically. Returns new cache dict."""
        ver_dir = _safe_path(self.cfg.data_dir, version)
        cache: dict = {
            "_cache_version": CACHE_VERSION,
            "_generated_at": datetime.now(UTC).isoformat(),
            "version": version,
            "files": {},
        }
        for fp in ver_dir.iterdir():
            if not fp.is_file() or fp.name == CACHE_FILENAME:
                continue
            cache["files"][fp.name] = {
                "size_bytes": fp.stat().st_size,
                "md5": None,
                "downloaded_at": None,
            }
        await self._write_version_cache(version, cache)
        return cache

    async def _update_version_cache(
        self,
        version: str,
        filename: str,
        md5: str | None,
        size_bytes: int,
    ) -> None:
        """Update a single file entry in the cache."""
        existing = await self._read_version_cache(version) or {
            "_cache_version": CACHE_VERSION,
            "_generated_at": datetime.now(UTC).isoformat(),
            "version": version,
            "files": {},
        }
        existing["files"][filename] = {
            "size_bytes": size_bytes,
            "md5": md5,
            "downloaded_at": datetime.now(UTC).isoformat(),
        }
        await self._write_version_cache(version, existing)

    async def _write_version_cache(self, version: str, data: dict) -> None:
        """Atomic write of cache file."""
        try:
            cache_path = _safe_path(self.cfg.data_dir, version, CACHE_FILENAME)
            temp = cache_path.with_suffix(".tmp")
            text = json.dumps(data, indent=2, ensure_ascii=False)
            await asyncio.to_thread(temp.write_text, text, "utf-8")
            await asyncio.to_thread(os.replace, temp, cache_path)
        except Exception as exc:
            LOGGER.warning("write_version_cache error version=%s error=%s", version, exc)

    # ------------------------------------------------------------------
    # Job management internals
    # ------------------------------------------------------------------

    def _create_job(self, kind: str, version: str | None) -> PythonMirrorJob:
        job = PythonMirrorJob(
            job_id=str(uuid4()),
            kind=kind,
            status="running",
            version=version,
        )
        self._jobs[job.job_id] = job
        asyncio.create_task(self._persist_job(job))
        return job

    async def _run_with_error_handling(
        self,
        job: PythonMirrorJob,
        coro_fn: Any,
        *args: Any,
    ) -> None:
        """Wrapper: run job coroutine, handle exceptions, update status."""
        try:
            await coro_fn(job, *args)
        except asyncio.CancelledError:
            job.status = "cancelled"
            job.finished_at = datetime.now(UTC)
            job.updated_at = datetime.now(UTC)
            await self._persist_job(job)
        except Exception as exc:
            LOGGER.error("job_error job_id=%s kind=%s error=%s",
                         job.job_id, job.kind, exc, exc_info=True)
            job.status = "error"
            job.message = str(exc)
            job.finished_at = datetime.now(UTC)
            job.updated_at = datetime.now(UTC)
            await self._persist_job(job)

    async def _persist_job(self, job: PythonMirrorJob) -> None:
        """Upsert job to MongoDB. Errors only logged, never raised."""
        try:
            collection = self._db.collection(PYTHON_MIRROR_JOBS)
            await collection.update_one(
                {"job_id": job.job_id},
                {"$set": {
                    "job_id": job.job_id,
                    "kind": job.kind,
                    "status": job.status,
                    "version": job.version,
                    "total": job.total,
                    "done": job.done,
                    "failed": job.failed,
                    "message": job.message,
                    "retry_count": job.retry_count,
                    "started_at": job.started_at.isoformat(),
                    "updated_at": datetime.now(UTC).isoformat(),
                    "finished_at": job.finished_at.isoformat() if job.finished_at else None,
                }},
                upsert=True,
            )
        except Exception as exc:
            LOGGER.debug("persist_job failed job_id=%s error=%s", job.job_id, exc)

    # ------------------------------------------------------------------
    # Job runners
    # ------------------------------------------------------------------

    async def _run_install(self, job: PythonMirrorJob, version: str) -> None:
        session = await self._get_session()
        # 1. Get file list from provider
        files = await self._provider.get_files(session, version, self._cache)
        if not files:
            raise RuntimeError(f"No downloadable files found for Python {version}")

        job.total = len(files)
        job.updated_at = datetime.now(UTC)
        await self._persist_job(job)

        # 2. Estimate disk space
        total_estimated = len(files) * 30 * 1024 * 1024  # ~30MB per file estimate
        self._check_disk_space(total_estimated)

        # 3. Create version directory
        ver_dir = _safe_path(self.cfg.data_dir, version)
        ver_dir.mkdir(parents=True, exist_ok=True)

        # 4. Download each file
        for f in files:
            dest = ver_dir / f.filename
            job.current_file = f.filename
            job.updated_at = datetime.now(UTC)

            # Check if already downloaded and valid
            if dest.exists():
                cache_data = await self._read_version_cache(version)
                cached_meta = (cache_data or {}).get("files", {}).get(f.filename, {})
                cached_md5 = cached_meta.get("md5")
                if cached_md5 and f.md5 and cached_md5 == f.md5:
                    LOGGER.debug("skip_already_cached file=%s", f.filename)
                    job.done += 1
                    await self._persist_job(job)
                    continue

            # Download
            for attempt in range(self.cfg.max_retries):
                try:
                    actual_md5 = await self._download_engine.download(
                        session=session,
                        url=f.download_url,
                        dest=dest,
                        expected_md5=f.md5,
                    )
                    await self._update_version_cache(
                        version, f.filename, actual_md5, dest.stat().st_size
                    )
                    job.done += 1
                    await self._persist_job(job)
                    break
                except IntegrityError as exc:
                    LOGGER.warning("integrity_error file=%s attempt=%d error=%s",
                                   f.filename, attempt + 1, exc)
                    if attempt == self.cfg.max_retries - 1:
                        job.failed += 1
                        await self._persist_job(job)
                except Exception as exc:
                    LOGGER.warning("download_error file=%s attempt=%d error=%s",
                                   f.filename, attempt + 1, exc)
                    if attempt < self.cfg.max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        job.failed += 1
                        await self._persist_job(job)

        job.status = "done" if job.failed == 0 else "error"
        job.message = (
            None if job.failed == 0
            else f"{job.failed} file(s) failed to download"
        )
        job.current_file = None
        job.finished_at = datetime.now(UTC)
        job.updated_at = datetime.now(UTC)
        await self._persist_job(job)
        LOGGER.info("install_done version=%s done=%d failed=%d",
                    version, job.done, job.failed)

    async def _run_verify(self, job: PythonMirrorJob, version: str) -> None:
        ver_dir = _safe_path(self.cfg.data_dir, version)
        if not ver_dir.exists():
            raise RuntimeError(f"Version {version} is not installed")

        files = [
            fp for fp in ver_dir.iterdir()
            if fp.is_file() and fp.name != CACHE_FILENAME
        ]
        job.total = len(files)
        job.updated_at = datetime.now(UTC)
        await self._persist_job(job)

        verifier = HybridVerifier()
        cache_data = await self._read_version_cache(version)
        failed_files: list[str] = []

        for fp in files:
            job.current_file = fp.name
            cached_meta = (cache_data or {}).get("files", {}).get(fp.name, {})
            result = await verifier.verify(
                fp,
                expected_md5=cached_meta.get("md5"),
                expected_size=cached_meta.get("size_bytes"),
            )
            if result.ok:
                job.done += 1
            else:
                job.failed += 1
                failed_files.append(fp.name)
                LOGGER.warning("verify_failed file=%s strategy=%s", fp.name, result.strategy)
            job.updated_at = datetime.now(UTC)
            await self._persist_job(job)

        job.status = "done"
        job.message = (
            "All files OK"
            if not failed_files
            else f"Failed: {', '.join(failed_files)}"
        )
        job.current_file = None
        job.finished_at = datetime.now(UTC)
        job.updated_at = datetime.now(UTC)
        await self._persist_job(job)

    async def _run_verify_all(self, job: PythonMirrorJob) -> None:
        installed = [
            d.name for d in self.cfg.data_dir.iterdir()
            if d.is_dir() and not d.name.startswith(".")
        ] if self.cfg.data_dir.exists() else []

        total_files = 0
        for v in installed:
            ver_dir = _safe_path(self.cfg.data_dir, v)
            total_files += sum(
                1 for fp in ver_dir.iterdir()
                if fp.is_file() and fp.name != CACHE_FILENAME
            )
        job.total = total_files
        job.updated_at = datetime.now(UTC)
        await self._persist_job(job)

        verifier = HybridVerifier()
        for v in installed:
            ver_dir = _safe_path(self.cfg.data_dir, v)
            cache_data = await self._read_version_cache(v)
            for fp in ver_dir.iterdir():
                if not fp.is_file() or fp.name == CACHE_FILENAME:
                    continue
                job.current_file = f"{v}/{fp.name}"
                cached_meta = (cache_data or {}).get("files", {}).get(fp.name, {})
                result = await verifier.verify(
                    fp,
                    expected_md5=cached_meta.get("md5"),
                    expected_size=cached_meta.get("size_bytes"),
                )
                if result.ok:
                    job.done += 1
                else:
                    job.failed += 1
                job.updated_at = datetime.now(UTC)
                await self._persist_job(job)

        job.status = "done"
        job.message = f"Checked {job.done + job.failed} files, {job.failed} failed"
        job.current_file = None
        job.finished_at = datetime.now(UTC)
        job.updated_at = datetime.now(UTC)
        await self._persist_job(job)

    async def _run_repair(self, job: PythonMirrorJob, version: str) -> None:
        """Re-download files that fail verification."""
        ver_dir = _safe_path(self.cfg.data_dir, version)
        if not ver_dir.exists():
            raise RuntimeError(f"Version {version} is not installed")

        session = await self._get_session()
        files_remote = await self._provider.get_files(session, version, self._cache)
        remote_map = {f.filename: f for f in files_remote}

        verifier = HybridVerifier()
        cache_data = await self._read_version_cache(version)

        # Find broken files
        to_repair: list[Path] = []
        for fp in ver_dir.iterdir():
            if not fp.is_file() or fp.name == CACHE_FILENAME:
                continue
            cached_meta = (cache_data or {}).get("files", {}).get(fp.name, {})
            result = await verifier.verify(
                fp,
                expected_md5=cached_meta.get("md5"),
                expected_size=cached_meta.get("size_bytes"),
            )
            if not result.ok:
                to_repair.append(fp)

        job.total = len(to_repair)
        job.updated_at = datetime.now(UTC)
        await self._persist_job(job)

        for fp in to_repair:
            job.current_file = fp.name
            remote_file = remote_map.get(fp.name)
            if remote_file is None:
                job.failed += 1
                job.updated_at = datetime.now(UTC)
                await self._persist_job(job)
                continue

            try:
                actual_md5 = await self._download_engine.download(
                    session=session,
                    url=remote_file.download_url,
                    dest=fp,
                    expected_md5=remote_file.md5,
                )
                await self._update_version_cache(
                    version, fp.name, actual_md5, fp.stat().st_size
                )
                job.done += 1
            except Exception as exc:
                LOGGER.error("repair_error file=%s error=%s", fp.name, exc)
                job.failed += 1
            job.updated_at = datetime.now(UTC)
            await self._persist_job(job)

        job.status = "done" if job.failed == 0 else "error"
        job.message = f"Repaired {job.done}/{job.total} files"
        job.current_file = None
        job.finished_at = datetime.now(UTC)
        job.updated_at = datetime.now(UTC)
        await self._persist_job(job)
