import asyncio
import aiohttp
import aiofiles
import random
import re
import time
import logging
from pathlib import Path
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AsyncPythonMirror")

class AsyncPythonMirror:
    FILE_PATTERNS = [
        "Python-{version}.tar.xz",
        "python-{version}-amd64.exe",
        "python-{version}-arm64.exe",
        "python-{version}-amd64.zip",
        "python-{version}-arm64.zip",
    ]

    def __init__(self, proxies=None, network_mode="direct", rate_limit_mb=None, parallel=4, show_progress=True):
        self.url_ftp = "https://www.python.org/ftp/python/"
        self.root_dir = Path(__file__).resolve().parent.parent.resolve()
        self.data_dir = (self.root_dir / "data" / "python-ver").resolve()
        
        self.proxies = self._load_proxies(proxies)
        self.network_mode = network_mode
        self.rate_limit = rate_limit_mb * 1024 * 1024 if rate_limit_mb else None
        self.parallel = parallel
        self.show_progress = show_progress
        self.max_retries = 3

    def _safe_path(self, *parts) -> Path:
        """Защита от Path Traversal: гарантирует, что путь внутри data_dir."""
        target_path = self.data_dir.joinpath(*parts).resolve()
        if not str(target_path).startswith(str(self.data_dir)):
            raise ValueError(f"Попытка доступа за пределы рабочей директории: {target_path}")
        return target_path

    def _load_proxies(self, proxies):
        if not proxies: return []
        if isinstance(proxies, list): return proxies
        path = Path(proxies)
        if path.exists():
            return [line.strip() for line in path.read_text().splitlines() if line.strip()]
        return [proxies]

    def _choose_proxy(self):
        if self.network_mode == "direct" or not self.proxies:
            return None
        return random.choice(self.proxies)

    async def get_versions(self, session: aiohttp.ClientSession):
        async with session.get(self.url_ftp) as resp:
            resp.raise_for_status()
            html = await resp.text()
        
        soup = await asyncio.to_thread(BeautifulSoup, html, "html.parser")
        
        versions = []
        for a in soup.find_all("a"):
            href = a.get("href", "").strip("/")
            if href and href[0].isdigit() and ".." not in href:
                versions.append(href)
        return versions

    def _is_useful_file(self, name: str, version: str) -> bool:
        for pattern in self.FILE_PATTERNS:
            if re.fullmatch(pattern.format(version=version), name):
                return True
        return False

    async def _download_file(self, session: aiohttp.ClientSession, url: str, dest: Path):
        proxy = self._choose_proxy()
        temp = dest.with_suffix(".download.tmp")
        
        try:
            async with session.get(url, proxy=proxy) as resp:
                resp.raise_for_status()
                start = time.time()
                downloaded = 0
                
                async with aiofiles.open(temp, "wb") as f:
                    async for chunk in resp.content.iter_chunked(65536):
                        await f.write(chunk)
                        downloaded += len(chunk)
                        
                        if self.rate_limit:
                            elapsed = time.time() - start
                            expected = downloaded / self.rate_limit
                            if expected > elapsed:
                                await asyncio.sleep(expected - elapsed)

            if dest.exists():
                dest.unlink()
            temp.rename(dest)
            return True
        except Exception as e:
            if temp.exists():
                try: temp.unlink()
                except: pass
            raise e

    async def _check_file_integrity(self, session: aiohttp.ClientSession, version: str, name: str, dest: Path):
        """HEAD запрос выполняется только если файл существует локально."""
        if not dest.exists():
            return False
        
        url = f"{self.url_ftp}{version}/{name}"
        try:
            async with session.head(url) as resp:
                server_size = int(resp.headers.get('Content-Length', 0))
                return dest.stat().st_size == server_size
        except:
            return False

    async def _download_single(self, session: aiohttp.ClientSession, version: str, name: str, session_dir: Path, sem: asyncio.Semaphore):
        async with sem:
            dest = self._safe_path(version, name)
            
            if await self._check_file_integrity(session, version, name, dest):
                if self.show_progress:
                    logger.info(f"[-] {name} уже корректно скачан.")
                return True

            url = f"{self.url_ftp}{version}/{name}"
            try:
                await self._download_file(session, url, dest)
                if self.show_progress:
                    logger.info(f"[+] {name} успешно скачан.")
                return True
            except Exception as e:
                logger.error(f"[!] Ошибка при обработке {name}: {e}")
                return False

    async def install_version(self, session: aiohttp.ClientSession, version: str):
        session_dir = self._safe_path(version)
        session_dir.mkdir(parents=True, exist_ok=True)
        
        for tmp_file in session_dir.glob("*.tmp*"):
            try: tmp_file.unlink()
            except: pass

        async with session.get(f"{self.url_ftp}{version}/") as resp:
            resp.raise_for_status()
            html = await resp.text()
            
        soup = await asyncio.to_thread(BeautifulSoup, html, "html.parser")
        files_to_download = [a.get("href") for a in soup.find_all("a") 
                             if a.get("href") and self._is_useful_file(a.get("href"), version)]
        
        if self.show_progress:
            logger.info(f"Найдено файлов для {version}: {len(files_to_download)}")
            
        sem = asyncio.Semaphore(self.parallel)
        
        for attempt in range(self.max_retries):
            tasks = [self._download_single(session, version, name, session_dir, sem) for name in files_to_download]
            results = await asyncio.gather(*tasks)
            
            failed_files = [files_to_download[i] for i, success in enumerate(results) if not success]
            
            if not failed_files:
                if self.show_progress:
                    logger.info(f"--- Все файлы для {version} успешно скачаны ---")
                return True
            
            files_to_download = failed_files
            logger.warning(f"Попытка {attempt + 1} завершилась с ошибками для {len(failed_files)} файлов.")
            await asyncio.sleep(2)
        
        raise RuntimeError(f"Не удалось скачать все файлы для версии {version} после {self.max_retries} попыток.")

# --- БЛОК ДЛЯ ОТЛАДКИ ---
async def _debug_main():
    mirror = AsyncPythonMirror(parallel=4)
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        try:
            versions = await mirror.get_versions(session)
            print(f"Доступно версий: {len(versions)}")
            ver = input("Какую версию скачать? ").strip()
            
            if ver in versions:
                await mirror.install_version(session, ver)
                print("\nГотово!")
            else:
                print("Версия не найдена.")
        except Exception as e:
            print(f"\nКритическая ошибка: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(_debug_main())
    except KeyboardInterrupt:
        print("\nПрервано пользователем.")