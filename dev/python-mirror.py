import asyncio
import aiohttp
import aiofiles
import random
import re
import time
import logging
import shutil
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
        self.data_dir.mkdir(parents=True, exist_ok=True) 
        
        self.proxies = self._load_proxies(proxies)
        self.network_mode = network_mode
        self.rate_limit = rate_limit_mb * 1024 * 1024 if rate_limit_mb else None
        self.parallel = parallel
        self.show_progress = show_progress
        self.max_retries = 3

    async def _get_remote_file_size(self, session: aiohttp.ClientSession, version: str, name: str) -> int:
        url = f"{self.url_ftp}{version}/{name}"
        try:
            async with session.head(url, proxy=self._choose_proxy()) as resp:
                if resp.status == 200:
                    return int(resp.headers.get('Content-Length', 0))
        except Exception:
            pass
        return 0

    async def _check_disk_space(self, session: aiohttp.ClientSession, version: str, file_names: list):
        """
        Проверяет, достаточно ли места для файлов (x2) 
        И остается ли после этого защитный порог в 3 ГБ.
        """
        MIN_SAFE_FREE_SPACE = 3 * 1024 * 1024 * 1024
        
        tasks = [self._get_remote_file_size(session, version, name) for name in file_names]
        sizes = await asyncio.gather(*tasks)
        total_required = sum(sizes)
        
        if total_required == 0:
            return

        _, _, free_space = shutil.disk_usage(self.data_dir)
        
        projected_usage = total_required * 2
        remaining_after_download = free_space - projected_usage

        if remaining_after_download < MIN_SAFE_FREE_SPACE:
            needed_extra = (MIN_SAFE_FREE_SPACE - remaining_after_download) / (1024**3)
            free_gb = free_space / (1024**3)
            req_gb = projected_usage / (1024**3)
            
            error_msg = (
                f"ОПАСНО: Недостаточно места для сохранения стабильности системы!\n"
                f"Свободно сейчас: {free_gb:.2f} ГБ\n"
                f"Будет занято (с запасом x2): {req_gb:.2f} ГБ\n"
                f"Остаток будет меньше лимита в 3 ГБ. Нужно еще хотя бы {needed_extra:.2f} ГБ."
            )
            logger.critical(error_msg)
            raise IOError(error_msg)
        
        if self.show_progress:
            expected_free = (remaining_after_download) / (1024**3)
            logger.info(f"Проверка безопасности: OK. После загрузки останется ~{expected_free:.2f} ГБ свободного места.")

    def _safe_path(self, *parts) -> Path:
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

    def _is_useful_file(self, name: str, version: str) -> bool:
        for pattern in self.FILE_PATTERNS:
            if re.fullmatch(pattern.format(version=version), name):
                return True
        return False

    async def _check_file_integrity(self, session: aiohttp.ClientSession, version: str, name: str, dest: Path):
        if not dest.exists(): return False
        url = f"{self.url_ftp}{version}/{name}"
        try:
            async with session.head(url, proxy=self._choose_proxy()) as resp:
                server_size = int(resp.headers.get('Content-Length', 0))
                return dest.stat().st_size == server_size
        except:
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
            if dest.exists(): dest.unlink()
            temp.rename(dest)
            return True
        except Exception as e:
            if temp.exists():
                try: temp.unlink()
                except: pass
            raise e

    async def _download_single(self, session: aiohttp.ClientSession, version: str, name: str, sem: asyncio.Semaphore):
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
        
        if not files_to_download:
            logger.warning(f"Файлы для версии {version} не найдены.")
            return False

        await self._check_disk_space(session, version, files_to_download)
            
        sem = asyncio.Semaphore(self.parallel)
        
        for attempt in range(self.max_retries):
            tasks = [self._download_single(session, version, name, sem) for name in files_to_download]
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
    mirror = AsyncPythonMirror(parallel=8)
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        try:
            versions = await mirror.get_versions(session)
            print(f"Доступно версий на сервере: {len(versions)}")
            
            print("\nВыберите действие:")
            print("1. Скачать конкретную версию (введите номер, например 3.11.0)")
            print("2. Скачать ВЕ СУЩЕСТВУЮЩИЕ версии (введите 'all')")
            print("3. ТОЛЬКО ТЕСТ (проверка локальных файлов без загрузки) (введите 'test')")
            
            choice = input("\nВаш выбор: ").strip().lower()

            if choice == 'test':
                print("\n--- Запуск проверки соответствия серверу ---")
                for ver in versions:
                    ver_dir = mirror.data_dir / ver
                    if ver_dir.exists():
                        print(f"Проверка версии {ver}...")
                        async with session.get(f"{mirror.url_ftp}{ver}/") as resp:
                            html = await resp.text()
                            soup = BeautifulSoup(html, "html.parser")
                            files = [a.get("href") for a in soup.find_all("a") 
                                    if a.get("href") and mirror._is_useful_file(a.get("href"), ver)]
                        
                        for f_name in files:
                            dest = ver_dir / f_name
                            is_ok = await mirror._check_file_integrity(session, ver, f_name, dest)
                            status = "[OK]" if is_ok else "[ERROR/MISSING]"
                            if not is_ok:
                                print(f"  {status} {f_name}")
                print("\n--- Проверка завершена ---")

            elif choice == 'all':
                print(f"Внимание! Будет скачано {len(versions)} версий.")
                confirm = input("Продолжить? (y/n): ")
                if confirm.lower() == 'y':
                    for i, ver in enumerate(versions, 1):
                        print(f"\n[{i}/{len(versions)}] Обработка версии: {ver}")
                        try:
                            await mirror.install_version(session, ver)
                        except Exception as e:
                            print(f"Ошибка в версии {ver}: {e}")
                print("\n--- Массовая загрузка завершена ---")

            elif choice in versions:
                await mirror.install_version(session, choice)
                print("\nГотово!")
            else:
                print("Версия не найдена или команда не распознана.")

        except Exception as e:
            print(f"\nКритическая ошибка: {e}")

if __name__ == "__main__":
    Path("data/python-ver").mkdir(parents=True, exist_ok=True)
    try:
        asyncio.run(_debug_main())
    except KeyboardInterrupt:
        print("\nПрервано пользователем.")