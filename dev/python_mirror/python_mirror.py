import asyncio
import logging
import os
import random
import re
import shutil
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional, Union

import aiofiles
import aiohttp
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field, HttpUrl

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AsyncPythonMirror")


class MirrorConfig(BaseModel):
    """Configuration for Python distributions mirror."""

    url_ftp: HttpUrl = Field(default="https://www.python.org/ftp/python/")
    data_dir: Path = Field(default_factory=lambda: Path(__file__).resolve().parent.parent / "data" / "python-ver")
    proxies: Union[List[str], str, None] = None
    network_mode: str = Field(default="direct", pattern="^(direct|proxy|mix)$")
    rate_limit_mb: Optional[float] = Field(default=None, gt=0)
    parallel: int = Field(default=4, ge=1, le=20)
    show_progress: bool = True
    max_retries: int = Field(default=3, ge=1)
    min_safe_space_gb: float = Field(default=3.0, ge=0)
    request_timeout: float = Field(default=30.0, gt=0)
    connect_timeout: float = Field(default=10.0, gt=0)
    read_timeout: float = Field(default=30.0, gt=0)
    verify_ssl: bool = True
    user_agent: str = "ASFES-Python-Mirror/1.0"


class AsyncPythonMirror:
    """Asynchronous mirror for Python installers and source archives."""

    FILE_PATTERNS = [
        "Python-{version}.tar.xz",
        "python-{version}-amd64.exe",
        "python-{version}-arm64.exe",
        "python-{version}-amd64.zip",
        "python-{version}-arm64.zip",
        "python-{version}-macos11.pkg",
    ]

    def __init__(self, config: MirrorConfig):
        self.cfg = config
        self.cfg.data_dir.mkdir(parents=True, exist_ok=True)

        self.data_dir = self.cfg.data_dir
        self.url_ftp = str(self.cfg.url_ftp)
        if not self.url_ftp.endswith("/"):
            self.url_ftp += "/"

        self.network_mode = self.cfg.network_mode
        self.show_progress = self.cfg.show_progress
        self.parallel = self.cfg.parallel
        self.max_retries = self.cfg.max_retries
        self.proxies = self._load_proxies(self.cfg.proxies)
        self._rate_limit_bytes = (self.cfg.rate_limit_mb * 1024 * 1024) if self.cfg.rate_limit_mb else None
        self.rate_limit = self._rate_limit_bytes

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

        base = self.data_dir.resolve()
        target = base.joinpath(*cleaned_parts).resolve()
        if not target.is_relative_to(base):
            raise ValueError(f"Path traversal attempt: {target}")
        return target

    def _load_proxies(self, proxies: Union[List[str], str, None]) -> List[str]:
        if not proxies:
            return []
        if isinstance(proxies, list):
            return [str(proxy).strip() for proxy in proxies if str(proxy).strip()]

        path = Path(str(proxies))
        if path.exists() and path.is_file():
            return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]

        value = str(proxies).strip()
        return [value] if value else []

    def _choose_proxy(self) -> Optional[str]:
        if self.network_mode == "direct" or not self.proxies:
            return None
        if self.network_mode == "proxy":
            return random.choice(self.proxies)
        if self.network_mode == "mix":
            return random.choice([None, random.choice(self.proxies)])
        return None

    def _collect_local_stats(self) -> Dict[str, Any]:
        versions_count = 0
        files_count = 0
        total_size = 0

        if not self.data_dir.exists():
            return {
                "versions_count": 0,
                "files_count": 0,
                "total_size_bytes": 0,
                "total_size_human": self._format_size(0),
            }

        for ver_dir in self.data_dir.iterdir():
            if not ver_dir.is_dir():
                continue
            versions_count += 1
            for file_path in ver_dir.glob("**/*"):
                if file_path.is_file():
                    files_count += 1
                    total_size += file_path.stat().st_size

        return {
            "versions_count": versions_count,
            "files_count": files_count,
            "total_size_bytes": total_size,
            "total_size_human": self._format_size(total_size),
        }

    async def _fetch_version_page(self, session: aiohttp.ClientSession, version: str) -> Optional[str]:
        try:
            async with session.get(
                f"{self.url_ftp}{version}/",
                proxy=self._choose_proxy(),
                ssl=self.cfg.verify_ssl,
            ) as resp:
                resp.raise_for_status()
                return await resp.text()
        except Exception as exc:
            logger.warning("version_page_error version=%s error=%s", version, exc)
            return None

    def _parse_version_page_files(self, html: str, version: str) -> List[str]:
        soup = BeautifulSoup(html, "html.parser")
        files: List[str] = []
        for link in soup.find_all("a"):
            href = link.get("href")
            if href and self._is_useful_file(href, version):
                files.append(href)
        return files

    # ------------------------- Low-level methods -------------------------

    async def _get_remote_file_size(self, session: aiohttp.ClientSession, version: str, name: str) -> int:
        url = f"{self.url_ftp}{version}/{name}"
        try:
            async with session.head(url, proxy=self._choose_proxy(), ssl=self.cfg.verify_ssl) as resp:
                if resp.status == 200:
                    return int(resp.headers.get("Content-Length", 0))
        except Exception:
            pass
        return 0

    async def _check_disk_space(self, session: aiohttp.ClientSession, version: str, file_names: List[str]) -> None:
        min_safe_free_space = self.cfg.min_safe_space_gb * 1024**3
        sizes = await asyncio.gather(*(self._get_remote_file_size(session, version, name) for name in file_names))
        total_required = sum(sizes)
        if total_required == 0:
            return

        _, _, free_space = shutil.disk_usage(self.data_dir)
        projected_usage = total_required * 2
        remaining_after_download = free_space - projected_usage
        if remaining_after_download < min_safe_free_space:
            raise IOError(
                "Insufficient disk space: "
                f"free={self._format_size(int(free_space))}, required={self._format_size(int(projected_usage))}"
            )

    def _is_useful_file(self, name: str, version: str) -> bool:
        for pattern in self.FILE_PATTERNS:
            regex = pattern.format(version=re.escape(version))
            if re.fullmatch(regex, name):
                return True
        return False

    async def _check_file_integrity(self, session: aiohttp.ClientSession, version: str, name: str, dest: Path) -> bool:
        if not dest.exists() or not dest.is_file():
            return False
        url = f"{self.url_ftp}{version}/{name}"
        try:
            async with session.head(url, proxy=self._choose_proxy(), timeout=3, ssl=self.cfg.verify_ssl) as resp:
                if resp.status == 200:
                    server_size = int(resp.headers.get("Content-Length", 0))
                    return dest.stat().st_size == server_size
                return False
        except Exception:
            return False

    async def _download_file(self, session: aiohttp.ClientSession, url: str, dest: Path) -> bool:
        proxy = self._choose_proxy()
        temp = dest.with_suffix(".download.tmp")
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
                        if self.rate_limit:
                            elapsed = max(time.time() - start, 0.001)
                            expected = downloaded / self.rate_limit
                            if expected > elapsed:
                                await asyncio.sleep(expected - elapsed)

            if dest.exists():
                dest.unlink()
            await asyncio.to_thread(os.replace, temp, dest)
            return True
        except Exception:
            if temp.exists():
                try:
                    temp.unlink()
                except OSError:
                    pass
            raise

    async def _download_single(self, session: aiohttp.ClientSession, version: str, name: str, sem: asyncio.Semaphore) -> bool:
        async with sem:
            dest = self._safe_path(version, name)
            if await self._check_file_integrity(session, version, name, dest):
                if self.show_progress:
                    logger.info("already_ok file=%s", name)
                return True

            url = f"{self.url_ftp}{version}/{name}"
            try:
                await self._download_file(session, url, dest)
                if self.show_progress:
                    logger.info("downloaded file=%s", name)
                return True
            except Exception as exc:
                logger.error("download_single_error file=%s error=%s", name, exc)
                return False

    async def get_versions(self, session: aiohttp.ClientSession) -> List[str]:
        try:
            async with session.get(
                self.url_ftp,
                proxy=self._choose_proxy(),
                timeout=5,
                ssl=self.cfg.verify_ssl,
            ) as resp:
                resp.raise_for_status()
                html = await resp.text()
                soup = BeautifulSoup(html, "html.parser")
                versions = []
                for link in soup.find_all("a"):
                    href = str(link.get("href", "")).rstrip("/")
                    if re.match(r"^\d+\.\d+\.\d+$", href):
                        versions.append(href)
                return sorted(versions, key=lambda v: [int(p) for p in v.split(".")], reverse=True)
        except (aiohttp.ClientError, asyncio.TimeoutError):
            logger.warning("network_unavailable_fallback_to_local")
            return await self.list_installed()

    async def install_version(self, session: aiohttp.ClientSession, version: str) -> bool:
        session_dir = self._safe_path(version)
        session_dir.mkdir(parents=True, exist_ok=True)

        for tmp_file in session_dir.glob("*.tmp*"):
            try:
                tmp_file.unlink()
            except OSError:
                pass

        html = await self._fetch_version_page(session, version)
        if not html:
            return False

        files_to_download = self._parse_version_page_files(html, version)
        if not files_to_download:
            logger.warning("files_not_found version=%s", version)
            return False

        await self._check_disk_space(session, version, files_to_download)
        sem = asyncio.Semaphore(self.parallel)

        for attempt in range(self.max_retries):
            tasks = [self._download_single(session, version, name, sem) for name in files_to_download]
            results = await asyncio.gather(*tasks)
            failed_files = [files_to_download[i] for i, ok in enumerate(results) if not ok]
            if not failed_files:
                if self.show_progress:
                    logger.info("install_success version=%s", version)
                return True
            files_to_download = failed_files
            logger.warning(
                "install_retry version=%s attempt=%s failed_files=%s",
                version,
                attempt + 1,
                len(failed_files),
            )
            await asyncio.sleep(2)

        raise RuntimeError(f"Failed to download all files for version {version}")

    async def list_installed(
        self, session: Optional[aiohttp.ClientSession] = None, check_integrity: bool = False
    ) -> List[str]:
        installed_versions = [d.name for d in self.data_dir.iterdir() if d.is_dir()]
        if not check_integrity:
            return installed_versions

        if not session:
            raise ValueError("session is required when check_integrity=True")

        valid_versions = []
        for version in installed_versions:
            files = [f.name for f in self._safe_path(version).iterdir() if f.is_file()]
            is_ok = True
            for file_name in files:
                if not await self._check_file_integrity(session, version, file_name, self._safe_path(version, file_name)):
                    is_ok = False
                    break
            if is_ok and files:
                valid_versions.append(version)
        return valid_versions

    async def repair_all(self, session: aiohttp.ClientSession) -> None:
        installed = await self.list_installed()
        for version in installed:
            await self.install_version(session, version)

    def get_file_path(
        self,
        version: str,
        filename_or_os_type: str,
        arch: Optional[str] = None,
        is_executable: Optional[bool] = None,
    ) -> Path:
        # New API: get_file_path(version, filename)
        # Backward compatibility: a 2-argument call with value like "windows"/"linux"/"macos"
        # (or extension-less value) is treated as old os_type-based API.
        use_legacy = arch is not None or is_executable is not None or "." not in filename_or_os_type
        if not use_legacy:
            return self._safe_path(version, filename_or_os_type)

        # Backward compatible API: get_file_path(version, os_type, arch="amd64", is_executable=True)
        os_type = filename_or_os_type.lower()
        arch_value = (arch or "amd64").lower()
        executable = True if is_executable is None else bool(is_executable)
        filename: Optional[str] = None

        if os_type == "windows":
            ext = "exe" if executable else "zip"
            filename = f"python-{version}-{arch_value}.{ext}"
        elif os_type == "linux":
            filename = f"Python-{version}.tar.xz"
        elif os_type == "macos":
            filename = f"python-{version}-macos11.pkg" if executable else f"Python-{version}.tar.xz"
        else:
            raise ValueError(f"Unsupported os_type: {os_type}")

        return self._safe_path(version, filename)

    def get_version_size(self, version: str) -> int:
        ver_dir = self._safe_path(version)
        if not ver_dir.exists():
            return 0
        return sum(f.stat().st_size for f in ver_dir.glob("**/*") if f.is_file())

    def get_total_size(self) -> int:
        return sum(f.stat().st_size for f in self.data_dir.glob("**/*") if f.is_file())

    def remove_version(self, version: str) -> bool:
        try:
            ver_dir = self._safe_path(version)
            if ver_dir.exists() and ver_dir.is_dir():
                shutil.rmtree(ver_dir)
                logger.info("removed version=%s", version)
                return True
            return False
        except Exception as exc:
            logger.error("remove_version_error version=%s error=%s", version, exc)
            return False

    # ------------------------- Public high-level API -------------------------

    async def get_versions_public(self, session: Optional[aiohttp.ClientSession] = None) -> Dict[str, Any]:
        try:
            async with self._managed_session(session) as managed:
                versions = await self.get_versions(managed)
            installed = await self.list_installed()
            result = self._result(
                ok=True,
                action="get_versions",
                versions=versions,
                installed_versions=installed,
                count=len(versions),
            )
            self._log_result(result)
            return result
        except Exception as exc:
            result = self._result(False, "get_versions", error=str(exc))
            self._log_result(result)
            return result

    async def install_version_public(
        self, version: str, session: Optional[aiohttp.ClientSession] = None
    ) -> Dict[str, Any]:
        try:
            async with self._managed_session(session) as managed:
                ok = await self.install_version(managed, version)
            files = self.list_version_files(version) if ok else []
            size_bytes = self.get_version_size(version) if ok else 0
            result = self._result(
                ok=ok,
                action="install_version",
                version=version,
                files_count=len(files),
                size_bytes=size_bytes,
                size_human=self._format_size(size_bytes),
                files=files,
            )
            self._log_result(result)
            return result
        except Exception as exc:
            result = self._result(False, "install_version", version=version, error=str(exc))
            self._log_result(result)
            return result

    async def verify_version(
        self, version: str, session: Optional[aiohttp.ClientSession] = None
    ) -> Dict[str, Any]:
        try:
            version_dir = self._safe_path(version)
            if not version_dir.exists():
                result = self._result(
                    ok=False,
                    action="verify_version",
                    version=version,
                    message="Version is not installed",
                )
                self._log_result(result)
                return result

            async with self._managed_session(session) as managed:
                html = await self._fetch_version_page(managed, version)
                if not html:
                    result = self._result(
                        ok=False,
                        action="verify_version",
                        version=version,
                        message="Failed to fetch version page",
                    )
                    self._log_result(result)
                    return result

                remote_files = self._parse_version_page_files(html, version)
                missing_files: List[str] = []
                corrupted_files: List[str] = []
                checked_files = 0

                for file_name in remote_files:
                    target = self._safe_path(version, file_name)
                    checked_files += 1
                    if not target.exists():
                        missing_files.append(file_name)
                        continue
                    ok = await self._check_file_integrity(managed, version, file_name, target)
                    if not ok:
                        corrupted_files.append(file_name)

            result = self._result(
                ok=not missing_files and not corrupted_files,
                action="verify_version",
                version=version,
                checked_files=checked_files,
                missing_files=missing_files,
                corrupted_files=corrupted_files,
            )
            self._log_result(result)
            return result
        except Exception as exc:
            result = self._result(False, "verify_version", version=version, error=str(exc))
            self._log_result(result)
            return result

    async def verify_all(self, session: Optional[aiohttp.ClientSession] = None) -> Dict[str, Any]:
        try:
            installed = await self.list_installed()
            details: List[Dict[str, Any]] = []
            async with self._managed_session(session) as managed:
                for ver in installed:
                    details.append(await self.verify_version(ver, session=managed))

            failed = [item["version"] for item in details if not item.get("ok", False)]
            result = self._result(
                ok=len(failed) == 0,
                action="verify_all",
                total_versions=len(installed),
                failed_versions=failed,
                details=details,
            )
            self._log_result(result)
            return result
        except Exception as exc:
            result = self._result(False, "verify_all", error=str(exc))
            self._log_result(result)
            return result

    async def repair_version(
        self, version: str, session: Optional[aiohttp.ClientSession] = None
    ) -> Dict[str, Any]:
        try:
            verify_before = await self.verify_version(version, session=session)
            if verify_before.get("ok"):
                result = self._result(
                    ok=True,
                    action="repair_version",
                    version=version,
                    repaired=False,
                    verify_before=verify_before,
                    verify_after=verify_before,
                )
                self._log_result(result)
                return result

            install_result = await self.install_version_public(version, session=session)
            verify_after = await self.verify_version(version, session=session)
            result = self._result(
                ok=verify_after.get("ok", False),
                action="repair_version",
                version=version,
                repaired=install_result.get("ok", False),
                verify_before=verify_before,
                verify_after=verify_after,
            )
            self._log_result(result)
            return result
        except Exception as exc:
            result = self._result(False, "repair_version", version=version, error=str(exc))
            self._log_result(result)
            return result

    async def repair_all_public(self, session: Optional[aiohttp.ClientSession] = None) -> Dict[str, Any]:
        try:
            installed = await self.list_installed()
            details: List[Dict[str, Any]] = []
            async with self._managed_session(session) as managed:
                for ver in installed:
                    details.append(await self.repair_version(ver, session=managed))

            failed = [item["version"] for item in details if not item.get("ok", False)]
            result = self._result(
                ok=len(failed) == 0,
                action="repair_all",
                total_versions=len(installed),
                failed_versions=failed,
                details=details,
            )
            self._log_result(result)
            return result
        except Exception as exc:
            result = self._result(False, "repair_all", error=str(exc))
            self._log_result(result)
            return result

    async def list_installed_public(
        self, check_integrity: bool = False, session: Optional[aiohttp.ClientSession] = None
    ) -> Dict[str, Any]:
        try:
            if check_integrity:
                async with self._managed_session(session) as managed:
                    versions = await self.list_installed(session=managed, check_integrity=True)
            else:
                versions = await self.list_installed(check_integrity=False)

            items = []
            for ver in sorted(versions, reverse=True):
                size = self.get_version_size(ver)
                items.append(
                    {
                        "version": ver,
                        "size_bytes": size,
                        "size_human": self._format_size(size),
                    }
                )

            result = self._result(
                ok=True,
                action="list_installed",
                check_integrity=check_integrity,
                count=len(items),
                versions=items,
            )
            self._log_result(result)
            return result
        except Exception as exc:
            result = self._result(False, "list_installed", check_integrity=check_integrity, error=str(exc))
            self._log_result(result)
            return result

    async def get_version_info(
        self, version: str, session: Optional[aiohttp.ClientSession] = None
    ) -> Dict[str, Any]:
        try:
            version_path = self._safe_path(version)
            files = self.list_version_files(version) if version_path.exists() else []
            size_bytes = self.get_version_size(version) if version_path.exists() else 0
            verify_result = await self.verify_version(version, session=session) if version_path.exists() else None

            result = self._result(
                ok=version_path.exists(),
                action="get_version_info",
                version=version,
                path=str(version_path),
                exists=version_path.exists(),
                files=files,
                files_count=len(files),
                size_bytes=size_bytes,
                size_human=self._format_size(size_bytes),
                integrity=verify_result,
            )
            self._log_result(result)
            return result
        except Exception as exc:
            result = self._result(False, "get_version_info", version=version, error=str(exc))
            self._log_result(result)
            return result

    def list_version_files(self, version: str) -> List[Dict[str, Any]]:
        version_path = self._safe_path(version)
        if not version_path.exists() or not version_path.is_dir():
            return []

        items: List[Dict[str, Any]] = []
        for file_path in sorted(version_path.glob("**/*")):
            if not file_path.is_file():
                continue
            size = file_path.stat().st_size
            items.append(
                {
                    "name": file_path.name,
                    "path": str(file_path),
                    "relative_path": str(file_path.relative_to(version_path)),
                    "size_bytes": size,
                    "size_human": self._format_size(size),
                }
            )
        return items

    def delete_target(self, version: str) -> Dict[str, Any]:
        removed = self.remove_version(version)
        result = self._result(ok=removed, action="delete_target", version=version, deleted=removed)
        self._log_result(result)
        return result

    def get_stats(self) -> Dict[str, Any]:
        stats = self._collect_local_stats()
        result = self._result(ok=True, action="get_stats", **stats)
        self._log_result(result)
        return result


async def _debug_main() -> None:
    mirror = AsyncPythonMirror(MirrorConfig(parallel=4))
    print(await mirror.get_versions_public())


if __name__ == "__main__":
    try:
        asyncio.run(_debug_main())
    except KeyboardInterrupt:
        pass
