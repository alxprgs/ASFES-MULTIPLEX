"""
PyPI Mirror service — wraps AsyncPypiMirror with blocklist (MongoDB),
background Job tracking, and on-demand proxy support.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from packaging import version as pkg_version

from server.core.config import PyPIConfig
from server.core.database import DatabaseManager
from server.core.cache import CacheManager
from server.core.logging import get_logger
from server.core.pypi_mirror import AsyncPypiMirror, MirrorConfig, normalize_package_name
from server.audit import AuditContext
from server.models import (
    PyPIBlocklistResponse,
    PyPIJobStatus,
    PyPIPackage,
    PyPIPackageListItem,
    PyPIPackageListResponse,
    PyPIPackageVersion,
    PyPIStatsResponse,
    UserPrincipal,
)

LOGGER = get_logger("multiplex.pypi")
PYPI_BLOCKLIST = "pypi_blocklist"


# ---------------------------------------------------------------------------
# Job tracking
# ---------------------------------------------------------------------------

@dataclass
class PyPIJob:
    job_id: str
    kind: str
    name: str | None
    job_fingerprint: str | None = None
    status: str = "running"  # pending | running | done | error | cancelled
    lock_owner: str | None = None
    lock_expires_at: str | None = None
    total: int = 0
    done: int = 0
    failed: int = 0
    message: str | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None
    remaining_packages: list[str] = field(default_factory=list)
    task: asyncio.Task | None = field(default=None, repr=False)  # type: ignore[type-arg]
    _start_time: float = field(default_factory=time.monotonic, repr=False)

    def to_status(self) -> PyPIJobStatus:
        progress_pct = (self.done / self.total * 100.0) if self.total > 0 else 0.0
        eta: float | None = None
        if self.status == "running" and self.done > 0 and self.total > self.done:
            elapsed = max(time.monotonic() - self._start_time, 0.001)
            rate = self.done / elapsed
            if rate > 0:
                eta = (self.total - self.done) / rate
        return PyPIJobStatus(
            job_id=self.job_id,
            job_fingerprint=self.job_fingerprint,
            kind=self.kind,
            status=self.status,
            lock_owner=self.lock_owner,
            lock_expires_at=self.lock_expires_at,
            name=self.name,
            total=self.total,
            done=self.done,
            failed=self.failed,
            progress_pct=round(progress_pct, 1),
            eta_seconds=round(eta, 0) if eta is not None else None,
            message=self.message,
            started_at=self.started_at.isoformat(),
            finished_at=self.finished_at.isoformat() if self.finished_at else None,
            remaining_packages=list(self.remaining_packages),
        )


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------

class PyPIMirrorService:
    """
    High-level service for the PyPI mirror. Wraps AsyncPypiMirror and adds:
    - Blocklist persistence in MongoDB
    - Background Job tracking with progress
    - On-demand proxy support
    - Audit logging for destructive operations
    """

    def __init__(self, config: PyPIConfig, db: DatabaseManager, audit: Any, cache: CacheManager | None = None) -> None:
        self.config = config
        self.db = db
        self.audit = audit
        self.cache = cache

        mirror_cfg = MirrorConfig(
            api_base=config.api_base,
            data_dir=config.data_dir,
            proxies=config.proxies if config.proxies else None,
            network_mode=config.network_mode,
            rate_limit_mb=config.rate_limit_mb,
            parallel=config.parallel,
            max_retries=config.max_retries,
            min_safe_space_gb=config.min_safe_space_gb,
            request_timeout=config.request_timeout,
            connect_timeout=config.connect_timeout,
            verify_ssl=config.verify_ssl,
            user_agent=config.user_agent,
        )
        self._mirror = AsyncPypiMirror(mirror_cfg, cache=self.cache)
        self._jobs: dict[str, PyPIJob] = {}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _invalidate_cache(self, name: str | None = None) -> None:
        if self.cache is not None:
            await self.cache.delete("pypi:html:root")
            if name:
                norm = normalize_package_name(name)
                await self.cache.delete(f"pypi:html:pkg:{norm}")

    def _format_size(self, size_bytes: int) -> str:
        return self._mirror._format_size(size_bytes)

    # ------------------------------------------------------------------
    # Blocklist
    # ------------------------------------------------------------------

    async def _get_blocklist_doc(self) -> dict[str, Any]:
        doc = await self.db.collection(PYPI_BLOCKLIST).find_one({"_id": "blocklist"})
        if doc is None:
            return {"_id": "blocklist", "blocked_packages": [], "blocked_versions": {}}
        return doc

    async def is_blocked(self, name: str, version: str | None = None) -> bool:
        norm = normalize_package_name(name)
        doc = await self._get_blocklist_doc()
        if norm in doc.get("blocked_packages", []):
            return True
        if version:
            return version in doc.get("blocked_versions", {}).get(norm, [])
        return False

    async def get_blocklist(self) -> PyPIBlocklistResponse:
        doc = await self._get_blocklist_doc()
        return PyPIBlocklistResponse(
            blocked_packages=doc.get("blocked_packages", []),
            blocked_versions=doc.get("blocked_versions", {}),
        )

    async def block(
        self,
        name: str,
        version: str | None = None,
        *,
        actor: UserPrincipal,
        request_meta: AuditContext,
    ) -> None:
        norm = normalize_package_name(name)
        if version is None:
            await self.db.collection(PYPI_BLOCKLIST).update_one(
                {"_id": "blocklist"},
                {"$addToSet": {"blocked_packages": norm}},
                upsert=True,
            )
        else:
            await self.db.collection(PYPI_BLOCKLIST).update_one(
                {"_id": "blocklist"},
                {"$addToSet": {f"blocked_versions.{norm}": version}},
                upsert=True,
            )
        await self.audit.record(
            "pypi.block",
            actor=actor,
            request_meta=request_meta,
            target={"package": norm, "version": version},
        )
        await self._invalidate_cache(norm)
        LOGGER.info("pypi.block package=%s version=%s actor=%s", norm, version, actor.username)

    async def unblock(
        self,
        name: str,
        version: str | None = None,
        *,
        actor: UserPrincipal,
        request_meta: AuditContext,
    ) -> None:
        norm = normalize_package_name(name)
        if version is None:
            await self.db.collection(PYPI_BLOCKLIST).update_one(
                {"_id": "blocklist"},
                {
                    "$pull": {"blocked_packages": norm},
                    "$unset": {f"blocked_versions.{norm}": ""},
                },
            )
        else:
            await self.db.collection(PYPI_BLOCKLIST).update_one(
                {"_id": "blocklist"},
                {"$pull": {f"blocked_versions.{norm}": version}},
            )
        await self.audit.record(
            "pypi.unblock",
            actor=actor,
            request_meta=request_meta,
            target={"package": norm, "version": version},
        )
        await self._invalidate_cache(norm)

    # ------------------------------------------------------------------
    # Simple API (PEP 503, pip-compatible)
    # ------------------------------------------------------------------

    async def simple_api_root_html(self) -> str:
        """Return PEP 503 root index HTML listing all local packages."""
        cache_key = "pypi:html:root"
        if self.cache is not None:
            cached = await self.cache.get(cache_key)
            if cached is not None:
                return cached

        packages = self._mirror.list_packages(include_versions=False)
        simple_path = self.config.simple_path.rstrip("/")
        lines = [
            f'  <a href="{simple_path}/simple/{item["name"]}/">{item["name"]}</a>'
            for item in packages
        ]
        body = "\n".join(lines)
        html_resp = (
            "<!DOCTYPE html>\n"
            "<html>\n"
            "  <head><title>Simple Package Index</title></head>\n"
            "  <body>\n"
            "<h1>Simple Package Index</h1>\n"
            f"{body}\n"
            "  </body>\n"
            "</html>"
        )
        if self.cache is not None:
            await self.cache.set(cache_key, html_resp, ttl_seconds=3600)
        return html_resp

    async def simple_api_package_html(self, name: str) -> str | None:
        """
        Return PEP 503 package-level HTML for pip.

        Returns None when:
        - Package not found locally AND on_demand_proxy is disabled.

        Raises ValueError when:
        - Package is blocked → caller maps to HTTP 404.
        """
        import aiohttp

        norm = normalize_package_name(name)
        cache_key = f"pypi:html:pkg:{norm}"
        if self.cache is not None:
            cached = await self.cache.get(cache_key)
            if cached is not None:
                return cached

        # Enforce blocklist
        doc = await self._get_blocklist_doc()
        if norm in doc.get("blocked_packages", []):
            raise ValueError(f"Package {norm!r} is blocked")

        blocked_versions: list[str] = doc.get("blocked_versions", {}).get(norm, [])
        simple_path = self.config.simple_path.rstrip("/")

        # Build list of local files with their sha256 hashes
        pkg_dir = self._mirror._get_pkg_dir(norm)
        local_files: list[dict[str, str]] = []

        if pkg_dir.exists():
            for ver_dir in pkg_dir.iterdir():
                if not ver_dir.is_dir() or ver_dir.name in blocked_versions:
                    continue
                for f in ver_dir.iterdir():
                    if f.is_file() and not f.name.endswith(".tmp"):
                        local_files.append(
                            {"filename": f.name, "version": ver_dir.name, "sha256": ""}
                        )

        files: list[dict[str, str]] = []

        if local_files:
            # Enrich with sha256 from upstream metadata
            try:
                async with aiohttp.ClientSession(
                    headers={"User-Agent": self.config.user_agent}
                ) as session:
                    metadata = await self._mirror._fetch_metadata(session, norm)
                    releases = (metadata or {}).get("releases", {})
                    sha_map: dict[str, str] = {}
                    for ver_files in releases.values():
                        for fi in ver_files:
                            fname = fi.get("filename", "")
                            sha = (fi.get("digests") or {}).get("sha256", "")
                            if fname and sha:
                                sha_map[fname] = sha
            except Exception:
                sha_map = {}

            for lf in local_files:
                lf["sha256"] = sha_map.get(lf["filename"], "")
                files.append(lf)

        elif self.config.on_demand_proxy:
            # Fetch metadata from PyPI and build virtual index (no files downloaded yet)
            try:
                async with aiohttp.ClientSession(
                    headers={"User-Agent": self.config.user_agent}
                ) as session:
                    metadata = await self._mirror._fetch_metadata(session, norm)
            except Exception:
                metadata = None

            if not metadata:
                return None

            for ver, ver_files in metadata.get("releases", {}).items():
                if ver in blocked_versions:
                    continue
                for fi in ver_files:
                    fname = fi.get("filename", "")
                    sha = (fi.get("digests") or {}).get("sha256", "")
                    if fname:
                        files.append({"filename": fname, "version": ver, "sha256": sha})
        else:
            return None

        # Build PEP 503 HTML with rewritten local links and sha256 fragments
        links: list[str] = []
        for f in files:
            href = f"{simple_path}/files/{norm}/{f['version']}/{f['filename']}"
            if f["sha256"]:
                href += f"#sha256={f['sha256']}"
            links.append(f'  <a href="{href}">{f["filename"]}</a>')

        body = "\n".join(links)
        html_resp = (
            "<!DOCTYPE html>\n"
            "<html>\n"
            f"  <head><title>Links for {norm}</title></head>\n"
            "  <body>\n"
            f"<h1>Links for {norm}</h1>\n"
            f"{body}\n"
            "  </body>\n"
            "</html>"
        )
        if self.cache is not None:
            await self.cache.set(cache_key, html_resp, ttl_seconds=3600)
        return html_resp

    async def get_file_path(self, name: str, version: str, filename: str) -> Path | None:
        """
        Resolve a file path for serving to pip.

        Raises PermissionError if the package or version is blocked.
        Returns None if the file does not exist and on-demand proxy is disabled.
        Downloads on-demand if configured.
        """
        import aiohttp

        norm = normalize_package_name(name)
        doc = await self._get_blocklist_doc()

        if norm in doc.get("blocked_packages", []):
            raise PermissionError(f"Package {norm!r} is blocked")

        blocked_vers: list[str] = doc.get("blocked_versions", {}).get(norm, [])
        if version in blocked_vers:
            raise PermissionError(f"Version {version!r} of {norm!r} is blocked")

        local_path = self._mirror._get_ver_dir(norm, version) / filename
        if local_path.exists():
            return local_path

        if not self.config.on_demand_proxy:
            return None

        # On-demand: download the specific file
        try:
            async with aiohttp.ClientSession(
                headers={"User-Agent": self.config.user_agent}
            ) as session:
                metadata = await self._mirror._fetch_metadata(session, norm)
                if not metadata:
                    return None

                file_url: str | None = None
                expected_sha: str | None = None
                for fi in metadata.get("releases", {}).get(version, []):
                    if fi.get("filename") == filename:
                        file_url = fi.get("url")
                        expected_sha = (fi.get("digests") or {}).get("sha256")
                        break

                if not file_url:
                    return None

                local_path.parent.mkdir(parents=True, exist_ok=True)
                
                # V3.3 Redis Download Coordination
                redis = self.cache._redis if self.cache.should_use_redis() else None
                lock_key = f"pypi:download:{norm}:{version}:{filename}"
                stream_key = f"pypi:events:{norm}:{version}:{filename}"
                
                if redis:
                    # Attempt to acquire lock
                    lock_acquired = await redis.set(lock_key, "1", nx=True, ex=300)
                    if not lock_acquired:
                        # Wait for completion event
                        LOGGER.info("Waiting for other worker to download %s", filename)
                        last_id = "0"
                        for _ in range(60): # wait up to 60s
                            try:
                                streams = await redis.xread({stream_key: last_id}, count=1, block=5000)
                                if streams:
                                    break
                            except Exception:
                                pass
                        return local_path if local_path.exists() else None

                try:
                    await self._mirror._download_file(session, file_url, local_path)
                    
                    if expected_sha and not await self._mirror._verify_hash(local_path, expected_sha):
                        LOGGER.warning("on_demand hash mismatch file=%s", filename)
                        if local_path.exists():
                            local_path.unlink()
                        return None
                finally:
                    if redis:
                        await redis.delete(lock_key)
                        try:
                            await redis.xadd(stream_key, {"status": "success"}, maxlen=10)
                            await redis.expire(stream_key, 60)
                        except Exception:
                            pass

        except Exception as exc:
            LOGGER.warning("on_demand_fail file=%s error=%s", filename, exc)
            return None

        return local_path if local_path.exists() else None

    # ------------------------------------------------------------------
    # Stats & listing
    # ------------------------------------------------------------------

    async def get_stats(self) -> PyPIStatsResponse:
        raw = self._mirror.get_stats()
        doc = await self._get_blocklist_doc()
        blocked_pkgs = len(doc.get("blocked_packages", []))
        blocked_vers = sum(len(v) for v in doc.get("blocked_versions", {}).values())
        active_jobs = sum(1 for j in self._jobs.values() if j.status == "running")
        return PyPIStatsResponse(
            packages_count=raw["packages_count"],
            versions_count=raw["versions_count"],
            files_count=raw["files_count"],
            total_size_bytes=raw["total_size_bytes"],
            total_size_human=raw["total_size_human"],
            blocked_packages=blocked_pkgs,
            blocked_versions=blocked_vers,
            active_jobs=active_jobs,
        )

    async def list_packages(
        self,
        search: str = "",
        page: int = 1,
        per_page: int = 25,
    ) -> PyPIPackageListResponse:
        doc = await self._get_blocklist_doc()
        blocked_pkgs: list[str] = doc.get("blocked_packages", [])
        blocked_vers_map: dict[str, list[str]] = doc.get("blocked_versions", {})

        if search.strip():
            all_pkgs = self._mirror.search_packages(search.strip())
        else:
            all_pkgs = self._mirror.list_packages(include_versions=True)

        total = len(all_pkgs)
        start = (page - 1) * per_page
        page_pkgs = all_pkgs[start : start + per_page]

        items: list[PyPIPackageListItem] = []
        for pkg in page_pkgs:
            nm = pkg["name"]
            versions: list[str] = pkg.get("versions", [])

            # Compute total size
            size_bytes = 0
            pkg_dir = self._mirror._get_pkg_dir(nm)
            if pkg_dir.exists():
                for f in pkg_dir.glob("**/*"):
                    if f.is_file():
                        size_bytes += f.stat().st_size

            # Latest version via PEP 440
            latest: str | None = None
            if versions:
                try:
                    latest = str(max(versions, key=lambda v: pkg_version.parse(v)))
                except Exception:
                    latest = sorted(versions)[-1] if versions else None

            items.append(
                PyPIPackageListItem(
                    name=nm,
                    versions_count=len(versions),
                    total_size_human=self._format_size(size_bytes),
                    latest_version=latest,
                    is_blocked=nm in blocked_pkgs,
                    has_blocked_versions=bool(blocked_vers_map.get(nm)),
                )
            )

        return PyPIPackageListResponse(items=items, total=total, page=page, per_page=per_page)

    async def get_package(self, name: str) -> PyPIPackage | None:
        norm = normalize_package_name(name)
        pkg_dir = self._mirror._get_pkg_dir(norm)
        if not pkg_dir.exists():
            return None

        doc = await self._get_blocklist_doc()
        blocked_pkgs: list[str] = doc.get("blocked_packages", [])
        blocked_vers_for_pkg: list[str] = doc.get("blocked_versions", {}).get(norm, [])

        versions_list: list[PyPIPackageVersion] = []
        total_size = 0

        for ver_dir in sorted(pkg_dir.iterdir(), key=lambda p: p.name):
            if not ver_dir.is_dir():
                continue
            files = [f for f in ver_dir.iterdir() if f.is_file()]
            ver_size = sum(f.stat().st_size for f in files)
            total_size += ver_size
            versions_list.append(
                PyPIPackageVersion(
                    version=ver_dir.name,
                    files_count=len(files),
                    size_bytes=ver_size,
                    size_human=self._format_size(ver_size),
                    is_blocked=ver_dir.name in blocked_vers_for_pkg,
                )
            )

        # Sort by PEP 440
        try:
            versions_list.sort(key=lambda v: pkg_version.parse(v.version))
        except Exception:
            pass

        return PyPIPackage(
            name=norm,
            versions=versions_list,
            total_versions=len(versions_list),
            total_size_bytes=total_size,
            total_size_human=self._format_size(total_size),
            is_blocked=norm in blocked_pkgs,
            blocked_versions=blocked_vers_for_pkg,
        )

    # ------------------------------------------------------------------
    # Job factory helpers
    # ------------------------------------------------------------------

    async def _sync_job(self, job: PyPIJob) -> None:
        if self.cache is not None:
            await self.cache.set(f"pypi:job:{job.job_id}", job.to_status().model_dump(), ttl_seconds=86400)

    def _make_job(self, kind: str, name: str | None) -> PyPIJob:
        job_id = uuid4().hex
        job = PyPIJob(job_id=job_id, kind=kind, name=name)
        self._jobs[job_id] = job
        
        if self.cache is not None:
            async def _sync_loop() -> None:
                while job.status in ("running", "pending"):
                    await asyncio.sleep(2)
                    await self._sync_job(job)
                await self._sync_job(job)
            asyncio.create_task(_sync_loop())
            
        return job

    async def _resolve_and_download(self, session: Any, job: PyPIJob, specs: list[str]) -> None:
        from packaging.requirements import Requirement
        from packaging.version import parse as parse_version

        queue = list(specs)
        visited: set[str] = set()

        job.total = len(queue)
        job.remaining_packages = list(queue)

        while queue and job.status != "cancelled":
            spec_str = queue.pop(0)
            if spec_str in job.remaining_packages:
                job.remaining_packages.remove(spec_str)

            try:
                req = Requirement(spec_str)
            except Exception as exc:
                LOGGER.warning("Invalid requirement spec: %s - %s", spec_str, exc)
                job.failed += 1
                continue

            norm = normalize_package_name(req.name)
            visit_key = f"{norm}:{str(req.specifier)}"
            if visit_key in visited:
                job.done += 1
                continue
            visited.add(visit_key)

            job.message = f"Resolving {spec_str}"
            metadata = await self._mirror._fetch_metadata(session, norm)
            if not metadata:
                job.failed += 1
                continue

            releases = metadata.get("releases", {})
            if not releases:
                job.failed += 1
                continue

            available_versions = []
            for v in releases.keys():
                try:
                    available_versions.append(parse_version(v))
                except Exception:
                    pass

            matched = list(req.specifier.filter(available_versions)) if req.specifier else available_versions
            if not matched:
                job.failed += 1
                continue

            best_version = str(max(matched))
            job.message = f"Downloading {norm}=={best_version}"
            
            ok = await self._mirror.download_version(session, norm, best_version)
            if ok:
                job.done += 1
                requires_dist = metadata.get("info", {}).get("requires_dist") or []
                for dist in requires_dist:
                    try:
                        dep_req = Requirement(dist)
                        if dep_req.marker and 'extra' in str(dep_req.marker):
                            continue
                        dep_str = str(dep_req)
                        queue.append(dep_str)
                        if dep_str not in job.remaining_packages:
                            job.remaining_packages.append(dep_str)
                            job.total += 1
                    except Exception:
                        pass
            else:
                job.failed += 1

    # ------------------------------------------------------------------
    # Install operations
    # ------------------------------------------------------------------

    def install_version(self, name: str, version: str, with_dependencies: bool = False) -> PyPIJob:
        norm = normalize_package_name(name)
        job = self._make_job("install", norm)

        async def _run() -> None:
            import aiohttp

            job.message = f"Starting {norm}=={version}"
            try:
                async with aiohttp.ClientSession(
                    headers={"User-Agent": self.config.user_agent}
                ) as session:
                    if with_dependencies:
                        await self._resolve_and_download(session, job, [f"{norm}=={version}"])
                    else:
                        job.total = 1
                        job.message = f"Downloading {norm}=={version}"
                        ok = await self._mirror.download_version(session, norm, version)
                        if ok:
                            job.done = 1
                        else:
                            job.failed = 1
                            
                    if job.status != "cancelled":
                        job.status = "done" if job.failed == 0 else "error"
                        job.message = f"Done: {job.done} ok, {job.failed} failed"
            except Exception as exc:
                job.status = "error"
                job.message = str(exc)
                LOGGER.error("install_version error package=%s version=%s error=%s", norm, version, exc)
            finally:
                job.finished_at = datetime.now(UTC)
                await self._invalidate_cache(norm)

        job.task = asyncio.create_task(_run())
        return job

    def install_all_versions(self, name: str, with_dependencies: bool = False) -> PyPIJob:
        norm = normalize_package_name(name)
        job = self._make_job("install_all", norm)

        async def _run() -> None:
            import aiohttp

            job.message = f"Fetching metadata for {norm}"
            try:
                async with aiohttp.ClientSession(
                    headers={"User-Agent": self.config.user_agent}
                ) as session:
                    metadata = await self._mirror._fetch_metadata(session, norm)
                    versions = list((metadata or {}).get("releases", {}).keys())
                    job.total = len(versions)
                    job.remaining_packages = [f"{norm}=={v}" for v in versions]

                    for ver in versions:
                        if job.status == "cancelled":
                            break
                        job.message = f"Downloading {norm}=={ver}"
                        ok = await self._mirror.download_version(session, norm, ver)
                        if ok:
                            job.done += 1
                        else:
                            job.failed += 1
                        spec = f"{norm}=={ver}"
                        if spec in job.remaining_packages:
                            job.remaining_packages.remove(spec)

                    if job.status != "cancelled":
                        job.status = "done"
                        job.message = f"Completed: {job.done} ok, {job.failed} failed"
                        
                        if with_dependencies and job.done > 0:
                            job.message = f"Resolving dependencies for {norm}"
                            requires_dist = metadata.get("info", {}).get("requires_dist") or []
                            deps_to_install = []
                            from packaging.requirements import Requirement
                            for dist in requires_dist:
                                try:
                                    dep_req = Requirement(dist)
                                    if not (dep_req.marker and 'extra' in str(dep_req.marker)):
                                        deps_to_install.append(str(dep_req))
                                except Exception:
                                    pass
                            
                            if deps_to_install:
                                await self._resolve_and_download(session, job, deps_to_install)
                                job.status = "done" if job.failed == 0 else "error"
                                job.message = f"Completed with deps: {job.done} ok, {job.failed} failed"

            except Exception as exc:
                job.status = "error"
                job.message = str(exc)
                LOGGER.error("install_all_versions error package=%s error=%s", norm, exc)
            finally:
                job.finished_at = datetime.now(UTC)
                job.remaining_packages = []
                await self._invalidate_cache(norm)

        job.task = asyncio.create_task(_run())
        return job

    def bulk_install(self, packages: list[str], with_dependencies: bool = False) -> PyPIJob:
        """Install packages from a list of specs: 'flask==2.0.0', 'requests', etc."""
        job = self._make_job("bulk_install", None)

        async def _run() -> None:
            import aiohttp

            try:
                async with aiohttp.ClientSession(
                    headers={"User-Agent": self.config.user_agent}
                ) as session:
                    if with_dependencies:
                        await self._resolve_and_download(session, job, packages)
                    else:
                        job.total = len(packages)
                        job.remaining_packages = list(packages)
                        for pkg_spec in list(packages):
                            if job.status == "cancelled":
                                break
                            name_parsed, version_parsed = _parse_pkg_spec(pkg_spec)
                            norm = normalize_package_name(name_parsed)
                            job.message = f"Downloading {pkg_spec}"
                            try:
                                if version_parsed:
                                    ok = await self._mirror.download_version(session, norm, version_parsed)
                                else:
                                    await self._mirror.download_all_versions(session, norm)
                                    ok = True
                                if ok:
                                    job.done += 1
                                else:
                                    job.failed += 1
                            except Exception as exc:
                                job.failed += 1
                                LOGGER.warning("bulk_install error pkg=%s error=%s", pkg_spec, exc)
                            if pkg_spec in job.remaining_packages:
                                job.remaining_packages.remove(pkg_spec)

                    if job.status != "cancelled":
                        job.status = "done" if job.failed == 0 else "error"
                        job.message = f"Bulk install done: {job.done} ok, {job.failed} failed"
            except Exception as exc:
                job.status = "error"
                job.message = str(exc)
            finally:
                job.finished_at = datetime.now(UTC)
                await self._invalidate_cache()

        job.task = asyncio.create_task(_run())
        return job

    def bulk_download_refresh(self) -> PyPIJob:
        """Re-download all packages already registered in the mirror storage."""
        job = self._make_job("bulk_download", None)

        async def _run() -> None:
            import aiohttp

            existing = self._mirror.list_packages(include_versions=True)
            specs: list[str] = []
            for pkg in existing:
                nm = pkg["name"]
                for ver in pkg.get("versions", []):
                    specs.append(f"{nm}=={ver}")

            job.total = len(specs)
            job.remaining_packages = list(specs)
            job.message = "Starting storage refresh"

            try:
                async with aiohttp.ClientSession(
                    headers={"User-Agent": self.config.user_agent}
                ) as session:
                    for spec in list(specs):
                        if job.status == "cancelled":
                            break
                        name_parsed, version_parsed = _parse_pkg_spec(spec)
                        job.message = f"Refreshing {spec}"
                        try:
                            ok = await self._mirror.download_version(
                                session, name_parsed, version_parsed or ""
                            )
                            if ok:
                                job.done += 1
                            else:
                                job.failed += 1
                        except Exception as exc:
                            job.failed += 1
                            LOGGER.warning("bulk_refresh error spec=%s error=%s", spec, exc)
                        if spec in job.remaining_packages:
                            job.remaining_packages.remove(spec)

                if job.status != "cancelled":
                    job.status = "done"
                    job.message = f"Refresh done: {job.done} ok, {job.failed} failed"
            except Exception as exc:
                job.status = "error"
                job.message = str(exc)
            finally:
                job.finished_at = datetime.now(UTC)
                await self._invalidate_cache()

        job.task = asyncio.create_task(_run())
        return job

    # ------------------------------------------------------------------
    # Delete operations
    # ------------------------------------------------------------------

    async def delete_version(
        self,
        name: str,
        version: str,
        *,
        actor: UserPrincipal,
        request_meta: AuditContext,
    ) -> bool:
        norm = normalize_package_name(name)
        result = self._mirror.delete_target(norm, version)
        if result.get("deleted"):
            await self.audit.record(
                "pypi.delete_version",
                actor=actor,
                request_meta=request_meta,
                target={"package": norm, "version": version},
            )
            LOGGER.info("pypi.delete_version package=%s version=%s actor=%s", norm, version, actor.username)
            await self._invalidate_cache(norm)
        return bool(result.get("deleted"))

    async def delete_package(
        self,
        name: str,
        *,
        actor: UserPrincipal,
        request_meta: AuditContext,
    ) -> bool:
        norm = normalize_package_name(name)
        result = self._mirror.delete_target(norm)
        if result.get("deleted"):
            await self.audit.record(
                "pypi.delete_package",
                actor=actor,
                request_meta=request_meta,
                target={"package": norm},
            )
            LOGGER.info("pypi.delete_package package=%s actor=%s", norm, actor.username)
            await self._invalidate_cache(norm)
        return bool(result.get("deleted"))

    # ------------------------------------------------------------------
    # Verification
    # ------------------------------------------------------------------

    def verify_package(self, name: str) -> PyPIJob:
        norm = normalize_package_name(name)
        job = self._make_job("verify", norm)

        async def _run() -> None:
            import aiohttp

            job.message = f"Verifying {norm}"
            try:
                async with aiohttp.ClientSession(
                    headers={"User-Agent": self.config.user_agent}
                ) as session:
                    result = await self._mirror.check_integrity(session, name=norm)
                job.total = result["checked_files"]
                issues = len(result["missing_files"]) + len(result["corrupted_files"])
                job.done = result["checked_files"] - issues
                job.failed = issues
                job.status = "done"
                job.message = "OK" if result["ok"] else f"{issues} issues found"
            except Exception as exc:
                job.status = "error"
                job.message = str(exc)
            finally:
                job.finished_at = datetime.now(UTC)

        job.task = asyncio.create_task(_run())
        return job

    def verify_all(self) -> PyPIJob:
        job = self._make_job("verify_all", None)

        async def _run() -> None:
            import aiohttp

            job.message = "Verifying all packages"
            try:
                async with aiohttp.ClientSession(
                    headers={"User-Agent": self.config.user_agent}
                ) as session:
                    result = await self._mirror.check_integrity(session)
                job.total = result["checked_files"]
                issues = len(result["missing_files"]) + len(result["corrupted_files"])
                job.done = result["checked_files"] - issues
                job.failed = issues
                job.status = "done"
                job.message = "All OK" if result["ok"] else f"{issues} issues found"
            except Exception as exc:
                job.status = "error"
                job.message = str(exc)
            finally:
                job.finished_at = datetime.now(UTC)

        job.task = asyncio.create_task(_run())
        return job

    # ------------------------------------------------------------------
    # Job management
    # ------------------------------------------------------------------

    def get_job(self, job_id: str) -> PyPIJob | None:
        return self._jobs.get(job_id)

    def cancel_job(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        if not job or job.status not in ("running", "pending"):
            return False
        if job.task:
            job.task.cancel()
        job.status = "cancelled"
        job.finished_at = datetime.now(UTC)
        LOGGER.info("pypi.job_cancelled job_id=%s kind=%s", job_id, job.kind)
        return True

    def list_active_jobs(self) -> list[PyPIJob]:
        return [j for j in self._jobs.values() if j.status in ("running", "pending")]

    async def shutdown(self) -> None:
        """Cancel all running jobs gracefully on application shutdown."""
        for job in list(self._jobs.values()):
            if job.status in ("running", "pending") and job.task:
                job.task.cancel()
        LOGGER.info("pypi_mirror.shutdown jobs_cancelled=%d", len(self._jobs))


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_pkg_spec(spec: str) -> tuple[str, str | None]:
    """Parse 'flask==2.0.0' → ('flask', '2.0.0'). Only exact pinned versions extracted."""
    if "==" in spec:
        parts = spec.split("==", 1)
        return parts[0].strip(), parts[1].strip()
    # For >=, <=, ~=, != — we only install latest (no pinned version)
    for sep in (">=", "<=", "~=", "!=", ">", "<"):
        if sep in spec:
            return spec.split(sep, 1)[0].strip(), None
    return spec.strip(), None
