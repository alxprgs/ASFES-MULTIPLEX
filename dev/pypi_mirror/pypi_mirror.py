import asyncio
import hashlib
import logging
import os
import random
import shutil
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional, Union

import aiofiles
import aiohttp
from packaging import version as pkg_version
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AsyncPypiMirror")


class MirrorConfig(BaseModel):
    """Configuration for the PyPI mirror."""

    api_base: str = "https://pypi.org/pypi"
    data_dir: Path = Field(default_factory=lambda: Path(__file__).resolve().parent / "pypi_storage")
    proxies: Union[List[str], str, None] = None
    network_mode: str = Field(default="direct", pattern="^(direct|proxy|mix)$")
    rate_limit_mb: Optional[float] = Field(default=None, gt=0)
    parallel: int = Field(default=5, ge=1)
    max_retries: int = Field(default=3, ge=1)
    min_safe_space_gb: float = Field(default=3.0, ge=0)
    request_timeout: float = Field(default=30.0, gt=0)
    connect_timeout: float = Field(default=10.0, gt=0)
    read_timeout: float = Field(default=30.0, gt=0)
    verify_ssl: bool = True
    user_agent: str = "ASFES-PyPI-Mirror/1.0"


class AsyncPypiMirror:
    """Asynchronous PyPI mirror with low-level and high-level APIs."""

    def __init__(self, config: MirrorConfig):
        self.cfg = config
        self.cfg.data_dir.mkdir(parents=True, exist_ok=True)
        self.proxies = self._load_proxies(self.cfg.proxies)
        self._rate_limit_bytes = (self.cfg.rate_limit_mb * 1024 * 1024) if self.cfg.rate_limit_mb else None

    # ------------------------- Core helpers -------------------------

    def _result(self, ok: bool, action: str, **data: Any) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"ok": ok, "action": action}
        payload.update(data)
        return payload

    def _log_result(self, result: Dict[str, Any]) -> None:
        level = logging.INFO if result.get("ok", False) else logging.WARNING
        logger.log(level, "result=%s", result)

    def _build_timeout(self) -> Optional[Any]:
        timeout_cls = getattr(aiohttp, "ClientTimeout", None)
        if timeout_cls is None:
            return None
        return timeout_cls(
            total=self.cfg.request_timeout,
            connect=self.cfg.connect_timeout,
            sock_read=self.cfg.read_timeout,
        )

    @asynccontextmanager
    async def _managed_session(
        self, session: Optional[aiohttp.ClientSession] = None
    ) -> AsyncIterator[aiohttp.ClientSession]:
        if session is not None:
            yield session
            return

        kwargs: Dict[str, Any] = {"headers": {"User-Agent": self.cfg.user_agent}}
        timeout = self._build_timeout()
        if timeout is not None:
            kwargs["timeout"] = timeout

        connector_cls = getattr(aiohttp, "TCPConnector", None)
        if connector_cls is not None:
            kwargs["connector"] = connector_cls(ssl=self.cfg.verify_ssl)

        async with aiohttp.ClientSession(**kwargs) as managed:
            yield managed

    def _load_proxies(self, proxies: Union[List[str], str, None]) -> List[str]:
        if not proxies:
            return []
        if isinstance(proxies, list):
            return [str(proxy).strip() for proxy in proxies if str(proxy).strip()]

        path = Path(str(proxies))
        if path.exists() and path.is_file():
            return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]

        proxy = str(proxies).strip()
        return [proxy] if proxy else []

    def _choose_proxy(self) -> Optional[str]:
        proxies = self.proxies
        if self.cfg.proxies is not None:
            proxies = self._load_proxies(self.cfg.proxies)
            self.proxies = proxies

        if self.cfg.network_mode == "direct" or not proxies:
            return None
        if self.cfg.network_mode == "proxy":
            return random.choice(proxies)
        if self.cfg.network_mode == "mix":
            return random.choice([None, random.choice(proxies)])
        return None

    def _format_size(self, size_bytes: int) -> str:
        value = float(max(size_bytes, 0))
        for unit in ("B", "KB", "MB", "GB"):
            if value < 1024:
                return f"{value:.2f} {unit}"
            value /= 1024
        return f"{value:.2f} TB"

    def _safe_path(self, *parts: str) -> Path:
        cleaned_parts: List[str] = []
        for part in parts:
            segment = str(part).strip()
            if segment in {"", ".", ".."}:
                raise ValueError(f"Invalid path segment: {part!r}")
            if "/" in segment or "\\" in segment:
                raise ValueError(f"Invalid path segment: {part!r}")
            if ".." in Path(segment).parts:
                raise ValueError(f"Invalid path segment: {part!r}")
            cleaned_parts.append(segment)

        base = self.cfg.data_dir.resolve()
        target = base.joinpath(*cleaned_parts).resolve()
        if not target.is_relative_to(base):
            raise ValueError(f"Path traversal attempt: {target}")
        return target

    def _get_pkg_dir(self, name: str) -> Path:
        return self._safe_path(name.lower())

    def _get_ver_dir(self, name: str, ver: str) -> Path:
        return self._safe_path(name.lower(), ver)

    def _collect_local_stats(self) -> Dict[str, Any]:
        packages_count = 0
        versions_count = 0
        files_count = 0
        total_size = 0

        if not self.cfg.data_dir.exists():
            return {
                "packages_count": 0,
                "versions_count": 0,
                "files_count": 0,
                "total_size_bytes": 0,
                "total_size_human": self._format_size(0),
            }

        for pkg_dir in self.cfg.data_dir.iterdir():
            if not pkg_dir.is_dir():
                continue
            packages_count += 1
            for ver_dir in pkg_dir.iterdir():
                if not ver_dir.is_dir():
                    continue
                versions_count += 1
                for file_path in ver_dir.iterdir():
                    if file_path.is_file():
                        files_count += 1
                        total_size += file_path.stat().st_size

        return {
            "packages_count": packages_count,
            "versions_count": versions_count,
            "files_count": files_count,
            "total_size_bytes": total_size,
            "total_size_human": self._format_size(total_size),
        }

    # ------------------------- Low-level methods -------------------------

    async def _verify_hash(self, file_path: Path, expected_sha256: str) -> bool:
        if not file_path.exists() or not file_path.is_file():
            return False

        def _calc_sha256(path: Path) -> str:
            sha256_hash = hashlib.sha256()
            with path.open("rb") as fh:
                while chunk := fh.read(8192):
                    sha256_hash.update(chunk)
            return sha256_hash.hexdigest()

        actual_sha256 = await asyncio.to_thread(_calc_sha256, file_path)
        return actual_sha256 == expected_sha256

    async def _fetch_metadata(self, session: aiohttp.ClientSession, name: str) -> Optional[dict]:
        url = f"{self.cfg.api_base}/{name}/json"
        try:
            async with session.get(url, proxy=self._choose_proxy(), ssl=self.cfg.verify_ssl) as resp:
                if resp.status == 200:
                    return await resp.json()
                return None
        except Exception as exc:
            logger.warning("metadata_error name=%s error=%s", name, exc)
            return None

    async def _check_disk_space(self, required_bytes: int) -> None:
        _, _, free = shutil.disk_usage(self.cfg.data_dir)
        min_bytes = self.cfg.min_safe_space_gb * 1024**3
        if (free - required_bytes) < min_bytes:
            raise IOError(f"Not enough free space: need={required_bytes} bytes")

    async def _download_file(self, session: aiohttp.ClientSession, url: str, dest: Path) -> None:
        proxy = self._choose_proxy()
        temp = dest.with_suffix(dest.suffix + ".tmp")
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            async with session.get(url, proxy=proxy, ssl=self.cfg.verify_ssl) as resp:
                resp.raise_for_status()
                start = time.time()
                downloaded = 0
                async with aiofiles.open(temp, "wb") as fh:
                    async for chunk in resp.content.iter_chunked(65536):
                        await fh.write(chunk)
                        downloaded += len(chunk)
                        if self._rate_limit_bytes:
                            elapsed = max(time.time() - start, 0.001)
                            expected = downloaded / self._rate_limit_bytes
                            if expected > elapsed:
                                await asyncio.sleep(expected - elapsed)

            if dest.exists():
                dest.unlink()
            await asyncio.to_thread(os.replace, temp, dest)
        except Exception:
            if temp.exists():
                try:
                    temp.unlink()
                except OSError:
                    pass
            raise

    async def download_version(self, session: aiohttp.ClientSession, name: str, ver: str) -> bool:
        metadata = await self._fetch_metadata(session, name)
        releases = (metadata or {}).get("releases", {})
        if ver not in releases:
            logger.error("version_not_found name=%s version=%s", name, ver)
            return False

        release_files = releases.get(ver, [])
        ver_dir = self._get_ver_dir(name, ver)
        ver_dir.mkdir(parents=True, exist_ok=True)

        success = True
        for file_info in release_files:
            file_url = file_info.get("url")
            file_name = file_info.get("filename")
            expected_hash = (file_info.get("digests") or {}).get("sha256")
            if not file_url or not file_name or not expected_hash:
                success = False
                continue

            dest = ver_dir / file_name
            if await self._verify_hash(dest, expected_hash):
                continue

            await self._check_disk_space(int(file_info.get("size", 0)))
            try:
                await self._download_file(session, file_url, dest)
                if not await self._verify_hash(dest, expected_hash):
                    success = False
                    if dest.exists():
                        dest.unlink()
            except Exception as exc:
                logger.error("download_error file=%s error=%s", file_name, exc)
                success = False

        return success

    async def download_all_versions(self, session: aiohttp.ClientSession, name: str) -> None:
        metadata = await self._fetch_metadata(session, name)
        if not metadata:
            return

        versions_list = list(metadata.get("releases", {}).keys())
        sem = asyncio.Semaphore(self.cfg.parallel)

        async def task(ver: str) -> bool:
            async with sem:
                return await self.download_version(session, name, ver)

        await asyncio.gather(*(task(ver) for ver in versions_list))

    async def check_integrity(self, session: aiohttp.ClientSession, name: Optional[str] = None) -> Dict[str, Any]:
        libs = [name] if name else [d.name for d in self.cfg.data_dir.iterdir() if d.is_dir()]
        checked_files = 0
        missing_files: List[str] = []
        corrupted_files: List[str] = []

        for lib in libs:
            metadata = await self._fetch_metadata(session, lib)
            releases = (metadata or {}).get("releases", {})
            if not releases:
                continue

            pkg_dir = self._get_pkg_dir(lib)
            if not pkg_dir.exists():
                continue

            for ver_path in pkg_dir.iterdir():
                if not ver_path.is_dir() or ver_path.name not in releases:
                    continue
                for file_info in releases[ver_path.name]:
                    file_name = file_info.get("filename")
                    expected_hash = (file_info.get("digests") or {}).get("sha256")
                    if not file_name or not expected_hash:
                        continue
                    target = ver_path / file_name
                    checked_files += 1
                    if not target.exists():
                        missing_files.append(str(target))
                        logger.warning("integrity_missing file=%s", target)
                        continue
                    if not await self._verify_hash(target, expected_hash):
                        corrupted_files.append(str(target))
                        logger.warning("integrity_corrupted file=%s", target)

        return {
            "checked_files": checked_files,
            "missing_files": missing_files,
            "corrupted_files": corrupted_files,
            "ok": not missing_files and not corrupted_files,
        }

    def delete_library(self, name: str, ver: Optional[str] = None) -> None:
        target = self._get_ver_dir(name, ver) if ver else self._get_pkg_dir(name)
        if target.exists():
            shutil.rmtree(target)
            logger.info("deleted=%s", target)

    async def get_info(self, session: aiohttp.ClientSession, name: str) -> Dict[str, Any]:
        metadata = await self._fetch_metadata(session, name)
        pkg_dir = self._get_pkg_dir(name)
        downloaded = [d.name for d in pkg_dir.iterdir() if d.is_dir()] if pkg_dir.exists() else []
        remote = list((metadata or {}).get("releases", {}).keys())
        return {
            "name": name,
            "is_downloaded": pkg_dir.exists(),
            "downloaded_versions": downloaded,
            "missing_versions": [ver for ver in remote if ver not in downloaded],
        }

    def list_libraries(self, include_versions: bool = False) -> Dict[str, List[str]]:
        packages = self.list_packages(include_versions=include_versions)
        result: Dict[str, List[str]] = {}
        for item in packages:
            if include_versions:
                result[item["name"]] = item.get("versions", [])
            else:
                result[item["name"]] = []
        return result

    def get_path(self, name: str, version_str: Optional[str] = None) -> Optional[Path]:
        pkg_dir = self._get_pkg_dir(name)
        if not pkg_dir.exists():
            return None

        if version_str:
            path = pkg_dir / version_str
            return path if path.exists() else None

        versions_dirs = [d for d in pkg_dir.iterdir() if d.is_dir()]
        if not versions_dirs:
            return None

        try:
            return max(versions_dirs, key=lambda p: pkg_version.parse(p.name))
        except Exception:
            return sorted(versions_dirs, key=lambda p: p.name)[-1]

    # ------------------------- Public high-level API -------------------------

    async def download_version_public(
        self, name: str, ver: str, session: Optional[aiohttp.ClientSession] = None
    ) -> Dict[str, Any]:
        try:
            async with self._managed_session(session) as managed:
                ok = await self.download_version(managed, name, ver)
            path = self.get_path(name, ver)
            result = self._result(
                ok=ok,
                action="download_version",
                name=name,
                version=ver,
                path=str(path) if path else None,
                path_exists=bool(path and path.exists()),
            )
            self._log_result(result)
            return result
        except Exception as exc:
            result = self._result(False, "download_version", name=name, version=ver, error=str(exc))
            self._log_result(result)
            return result

    async def download_all_versions_public(
        self, name: str, session: Optional[aiohttp.ClientSession] = None
    ) -> Dict[str, Any]:
        try:
            async with self._managed_session(session) as managed:
                metadata = await self._fetch_metadata(managed, name)
                releases = (metadata or {}).get("releases", {})
                versions_list = sorted(releases.keys(), key=lambda v: pkg_version.parse(v))
                sem = asyncio.Semaphore(self.cfg.parallel)

                async def task(ver: str) -> Dict[str, Any]:
                    async with sem:
                        ok = await self.download_version(managed, name, ver)
                        return {"version": ver, "ok": ok}

                details = await asyncio.gather(*(task(ver) for ver in versions_list))

            success_count = sum(1 for item in details if item["ok"])
            result = self._result(
                ok=success_count == len(details),
                action="download_all_versions",
                name=name,
                total_versions=len(details),
                success_versions=success_count,
                failed_versions=len(details) - success_count,
                details=details,
            )
            self._log_result(result)
            return result
        except Exception as exc:
            result = self._result(False, "download_all_versions", name=name, error=str(exc))
            self._log_result(result)
            return result

    async def verify(
        self, name: Optional[str] = None, session: Optional[aiohttp.ClientSession] = None
    ) -> Dict[str, Any]:
        try:
            async with self._managed_session(session) as managed:
                integrity = await self.check_integrity(managed, name=name)
            result = self._result(
                ok=integrity["ok"],
                action="verify",
                name=name,
                checked_files=integrity["checked_files"],
                missing_count=len(integrity["missing_files"]),
                corrupted_count=len(integrity["corrupted_files"]),
                missing_files=integrity["missing_files"],
                corrupted_files=integrity["corrupted_files"],
            )
            self._log_result(result)
            return result
        except Exception as exc:
            result = self._result(False, "verify", name=name, error=str(exc))
            self._log_result(result)
            return result

    async def repair(
        self, name: Optional[str] = None, session: Optional[aiohttp.ClientSession] = None
    ) -> Dict[str, Any]:
        try:
            repaired = 0
            failed = 0
            checked = 0

            async with self._managed_session(session) as managed:
                libs = [name] if name else [d.name for d in self.cfg.data_dir.iterdir() if d.is_dir()]
                for lib in libs:
                    metadata = await self._fetch_metadata(managed, lib)
                    releases = (metadata or {}).get("releases", {})
                    pkg_dir = self._get_pkg_dir(lib)
                    if not releases or not pkg_dir.exists():
                        continue

                    for ver_dir in [d for d in pkg_dir.iterdir() if d.is_dir()]:
                        release_files = releases.get(ver_dir.name, [])
                        for file_info in release_files:
                            file_name = file_info.get("filename")
                            file_url = file_info.get("url")
                            expected_hash = (file_info.get("digests") or {}).get("sha256")
                            if not file_name or not file_url or not expected_hash:
                                continue

                            checked += 1
                            target = ver_dir / file_name
                            if await self._verify_hash(target, expected_hash):
                                continue
                            try:
                                await self._check_disk_space(int(file_info.get("size", 0)))
                                await self._download_file(managed, file_url, target)
                                if await self._verify_hash(target, expected_hash):
                                    repaired += 1
                                else:
                                    failed += 1
                            except Exception:
                                failed += 1

            result = self._result(
                ok=failed == 0,
                action="repair",
                name=name,
                checked_files=checked,
                repaired_files=repaired,
                failed_files=failed,
            )
            self._log_result(result)
            return result
        except Exception as exc:
            result = self._result(False, "repair", name=name, error=str(exc))
            self._log_result(result)
            return result

    async def get_package_info(
        self, name: str, session: Optional[aiohttp.ClientSession] = None
    ) -> Dict[str, Any]:
        try:
            async with self._managed_session(session) as managed:
                metadata = await self._fetch_metadata(managed, name)
            releases = (metadata or {}).get("releases", {})
            local_path = self._get_pkg_dir(name)
            downloaded_versions = (
                sorted([d.name for d in local_path.iterdir() if d.is_dir()], key=lambda v: pkg_version.parse(v))
                if local_path.exists()
                else []
            )
            remote_versions = sorted(releases.keys(), key=lambda v: pkg_version.parse(v)) if releases else []
            info = (metadata or {}).get("info", {})

            result = self._result(
                ok=True,
                action="get_package_info",
                name=name,
                summary=info.get("summary"),
                home_page=info.get("home_page"),
                latest_version=info.get("version"),
                remote_versions=remote_versions,
                downloaded_versions=downloaded_versions,
                missing_versions=[v for v in remote_versions if v not in downloaded_versions],
                local_path=str(local_path),
                local_exists=local_path.exists(),
            )
            self._log_result(result)
            return result
        except Exception as exc:
            result = self._result(False, "get_package_info", name=name, error=str(exc))
            self._log_result(result)
            return result

    async def get_version_info(
        self, name: str, ver: str, session: Optional[aiohttp.ClientSession] = None
    ) -> Dict[str, Any]:
        try:
            async with self._managed_session(session) as managed:
                metadata = await self._fetch_metadata(managed, name)
            releases = (metadata or {}).get("releases", {})
            release_files = releases.get(ver, [])
            local_dir = self._get_ver_dir(name, ver)

            files: List[Dict[str, Any]] = []
            for file_info in release_files:
                file_name = file_info.get("filename")
                expected_hash = (file_info.get("digests") or {}).get("sha256")
                if not file_name:
                    continue
                local_path = local_dir / file_name
                local_exists = local_path.exists()
                valid_hash = bool(expected_hash and local_exists and await self._verify_hash(local_path, expected_hash))
                files.append(
                    {
                        "filename": file_name,
                        "url": file_info.get("url"),
                        "size": file_info.get("size"),
                        "local_path": str(local_path),
                        "local_exists": local_exists,
                        "valid_hash": valid_hash,
                    }
                )

            result = self._result(
                ok=True,
                action="get_version_info",
                name=name,
                version=ver,
                local_path=str(local_dir),
                local_exists=local_dir.exists(),
                files=files,
            )
            self._log_result(result)
            return result
        except Exception as exc:
            result = self._result(False, "get_version_info", name=name, version=ver, error=str(exc))
            self._log_result(result)
            return result

    def list_packages(self, include_versions: bool = False) -> List[Dict[str, Any]]:
        packages: List[Dict[str, Any]] = []
        if not self.cfg.data_dir.exists():
            return packages

        for pkg_dir in sorted(self.cfg.data_dir.iterdir(), key=lambda p: p.name):
            if not pkg_dir.is_dir():
                continue
            item: Dict[str, Any] = {"name": pkg_dir.name, "path": str(pkg_dir)}
            if include_versions:
                versions = [v.name for v in pkg_dir.iterdir() if v.is_dir()]
                item["versions"] = sorted(versions, key=lambda v: pkg_version.parse(v))
            packages.append(item)
        return packages

    def search_packages(self, query: str) -> List[Dict[str, Any]]:
        q = query.strip().lower()
        if not q:
            return []

        matched: List[Dict[str, Any]] = []
        for item in self.list_packages(include_versions=True):
            if q in item["name"].lower():
                matched.append(item)
                continue
            versions = item.get("versions", [])
            if any(q in str(ver).lower() for ver in versions):
                matched.append(item)
        return matched

    def get_path_info(self, name: str, version_str: Optional[str] = None) -> Dict[str, Any]:
        try:
            path = self.get_path(name, version_str)
            if not path:
                return self._result(
                    ok=False,
                    action="get_path_info",
                    name=name,
                    version=version_str,
                    path=None,
                    exists=False,
                )

            if path.is_file():
                total_size = path.stat().st_size
                files_count = 1
            else:
                file_paths = [f for f in path.glob("**/*") if f.is_file()]
                total_size = sum(f.stat().st_size for f in file_paths)
                files_count = len(file_paths)

            result = self._result(
                ok=True,
                action="get_path_info",
                name=name,
                version=version_str,
                path=str(path),
                exists=True,
                is_dir=path.is_dir(),
                files_count=files_count,
                size_bytes=total_size,
                size_human=self._format_size(total_size),
            )
            self._log_result(result)
            return result
        except Exception as exc:
            result = self._result(False, "get_path_info", name=name, version=version_str, error=str(exc))
            self._log_result(result)
            return result

    def delete_target(self, name: str, ver: Optional[str] = None) -> Dict[str, Any]:
        try:
            target = self._get_ver_dir(name, ver) if ver else self._get_pkg_dir(name)
            if not target.exists():
                result = self._result(
                    ok=False,
                    action="delete_target",
                    name=name,
                    version=ver,
                    deleted=False,
                    message="Target does not exist",
                )
                self._log_result(result)
                return result

            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()

            result = self._result(
                ok=True,
                action="delete_target",
                name=name,
                version=ver,
                deleted=True,
                path=str(target),
            )
            self._log_result(result)
            return result
        except Exception as exc:
            result = self._result(False, "delete_target", name=name, version=ver, error=str(exc))
            self._log_result(result)
            return result

    def get_stats(self) -> Dict[str, Any]:
        stats = self._collect_local_stats()
        result = self._result(ok=True, action="get_stats", **stats)
        self._log_result(result)
        return result


async def main() -> None:
    config = MirrorConfig(rate_limit_mb=5.0)
    mirror = AsyncPypiMirror(config)
    result = await mirror.download_version_public("pydantic", "2.6.0")
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
