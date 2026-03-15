import asyncio
import aiohttp
import aiofiles
import random
import re
import time
import logging
import shutil
from typing import List, Optional, Union
from pathlib import Path
from pydantic import BaseModel, Field, HttpUrl
from bs4 import BeautifulSoup
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AsyncPythonMirror")

class MirrorConfig(BaseModel):
    """
    Схема валидации конфигурации для зеркала Python.
    Pydantic автоматически проверит типы и значения при инициализации.
    """
    url_ftp: HttpUrl = Field(default="https://www.python.org/ftp/python/")
    data_dir: Path = Field(
        default_factory=lambda: Path(__file__).resolve().parent.parent / "data" / "python-ver"
    )
    proxies: Union[List[str], str, None] = None
    network_mode: str = Field(default="direct", pattern="^(direct|proxy)$")
    rate_limit_mb: Optional[float] = Field(default=None, gt=0)
    parallel: int = Field(default=4, ge=1, le=20)
    show_progress: bool = True
    max_retries: int = Field(default=3, ge=1)

class AsyncPythonMirror:
    """
    Класс для асинхронного зеркалирования дистрибутивов Python.
    Использует Pydantic для управления конфигурацией.
    """
    
    FILE_PATTERNS = [
        "Python-{version}.tar.xz",
        "python-{version}-amd64.exe",
        "python-{version}-arm64.exe",
        "python-{version}-amd64.zip",
        "python-{version}-arm64.zip",
    ]

    def __init__(self, config: MirrorConfig):
        self.cfg = config
        self.cfg.data_dir.mkdir(parents=True, exist_ok=True)

        self.data_dir = self.cfg.data_dir
        self.url_ftp = self.cfg.url_ftp
        if not str(self.url_ftp).endswith('/'):
            self.url_ftp = str(self.url_ftp) + '/'
        self.network_mode = self.cfg.network_mode
        self.show_progress = self.cfg.show_progress
        self.parallel = self.cfg.parallel
        self.max_retries = self.cfg.max_retries

        self.proxies = self._load_proxies(self.cfg.proxies)
        self._rate_limit_bytes = (self.cfg.rate_limit_mb * 1024 * 1024) if self.cfg.rate_limit_mb else None
        self.rate_limit = self._rate_limit_bytes

    async def _get_remote_file_size(self, session: aiohttp.ClientSession, version: str, name: str) -> int:
        """
        Получает размер файла на удаленном сервере через HEAD-запрос.

        :param session: Активная HTTP-сессия.
        :param version: Версия Python (строка).
        :param name: Имя файла.
        :return: Размер файла в байтах или 0 при ошибке.
        """
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
        Проверяет, достаточно ли места на диске для загрузки файлов версии.
        Учитывает двойной объем (для временных файлов) и защитный порог в 3 ГБ.

        :param session: Активная HTTP-сессия.
        :param version: Версия Python.
        :param file_names: Список имен файлов для загрузки.
        :raises IOError: Если места недостаточно для безопасной работы.
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

    def _format_size(self, size_bytes: int) -> str:
        """Вспомогательная функция для красивого вывода размера."""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f} TB"


    def _safe_path(self, *parts) -> Path:
        """
        Создает безопасный путь внутри рабочей директории, предотвращая выход за её пределы (Path Traversal).

        :param parts: Части пути.
        :return: Объект Path.
        :raises ValueError: Если итоговый путь находится вне self.data_dir.
        """
        target_path = self.data_dir.joinpath(*parts).resolve()
        if not str(target_path).startswith(str(self.data_dir)):
            raise ValueError(f"Попытка доступа за пределы рабочей директории: {target_path}")
        return target_path

    def _load_proxies(self, proxies) -> list:
            """
            Парсит входные данные прокси (файл, строка или список).
            """
            if not proxies: 
                return []
            if isinstance(proxies, list): return proxies
            path = Path(proxies)
            if path.exists():
                return [line.strip() for line in path.read_text().splitlines() if line.strip()]
            return [proxies]

    def _choose_proxy(self) -> Optional[str]:
        """
        Выбирает случайный прокси из списка, если включен соответствующий режим.

        :return: Строка прокси или None.
        """
        if self.network_mode == "direct" or not self.proxies:
            return None
        return random.choice(self.proxies)

    def _is_useful_file(self, name: str, version: str) -> bool:
        """
        Проверяет, соответствует ли имя файла заданным шаблонам (exe, zip, tar.xz).

        :param name: Имя файла на сервере.
        :param version: Версия Python.
        :return: True, если файл нужен для зеркала.
        """
        for pattern in self.FILE_PATTERNS:
            regex = pattern.format(version=re.escape(version))
            if re.fullmatch(regex, name):
                return True
        return False

    async def _check_file_integrity(self, session: aiohttp.ClientSession, version: str, name: str, dest: Path) -> bool:
        """
        Сверяет размер локального файла с размером на сервере.

        :param session: Активная HTTP-сессия.
        :param version: Версия Python.
        :param name: Имя файла.
        :param dest: Путь к локальному файлу.
        :return: True, если файл существует и размеры совпадают.
        """
        if not dest.exists(): 
            return False
        
        url = f"{self.url_ftp}{version}/{name}"
        try:
            async with session.head(url, proxy=self._choose_proxy(), timeout=3) as resp:
                if resp.status == 200:
                    server_size = int(resp.headers.get('Content-Length', 0))
                    return dest.stat().st_size == server_size
                return False
        except Exception:
            return False

    async def _download_file(self, session: aiohttp.ClientSession, url: str, dest: Path) -> bool:
        """
        Загружает файл по ссылке с поддержкой ограничения скорости и временных файлов.

        :param session: Активная HTTP-сессия.
        :param url: Прямая ссылка на файл.
        :param dest: Конечный путь сохранения.
        :return: True при успехе.
        :raises Exception: При сетевых ошибках или ошибках записи.
        """
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
            await asyncio.to_thread(os.replace, temp, dest)
            return True
        except Exception as e:
            if temp.exists():
                try: temp.unlink()
                except: pass
            raise e

    async def _download_single(self, session: aiohttp.ClientSession, version: str, name: str, sem: asyncio.Semaphore) -> bool:
        """
        Обертка для загрузки одного файла с использованием семафора и проверки целостности.

        :param session: Активная HTTP-сессия.
        :param version: Версия Python.
        :param name: Имя файла.
        :param sem: Семафор для ограничения параллелизма.
        :return: True, если файл скачан или уже существует.
        """
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

    async def get_versions(self, session: aiohttp.ClientSession) -> List[str]:
        """Получает список версий (из сети или локально, если сети нет)."""
        try:
            async with session.get(self.url_ftp, timeout=5) as resp:
                resp.raise_for_status()
                html = await resp.text()
                soup = BeautifulSoup(html, "html.parser")
                versions = []
                for a in soup.find_all("a"):
                    href = a.get("href", "").rstrip("/")
                    if re.match(r'^\d+\.\d+\.\d+$', href):
                        versions.append(href)
                return sorted(versions, key=lambda x: list(map(int, x.split('.'))), reverse=True)
        except (aiohttp.ClientError, asyncio.TimeoutError):
            logger.warning(" Сеть недоступна. Использую список локально установленных версий.")
            return await self.list_installed()

    async def install_version(self, session: aiohttp.ClientSession, version: str) -> bool:
        """
        Скачивает все необходимые файлы для указанной версии Python.
        Включает парсинг страницы версии, очистку временных файлов и повторные попытки.

        :param session: Активная HTTP-сессия.
        :param version: Строка версии (например, '3.10.5').
        :return: True, если все файлы успешно загружены.
        :raises RuntimeError: Если после всех попыток файлы не скачаны.
        """
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

    async def list_installed(self, session: aiohttp.ClientSession = None, check_integrity: bool = False) -> list:
        """
        Возвращает список версий, которые уже скачаны локально.

        :param session: Сессия (нужна только если check_integrity=True).
        :param check_integrity: Если True, проверяет каждый файл версии на соответствие размеру на сервере.
        :return: Список строк версий.
        """
        installed_versions = [d.name for d in self.data_dir.iterdir() if d.is_dir()]
        if not check_integrity:
            return installed_versions

        if not session:
            raise ValueError("Для проверки целостности необходима aiohttp.ClientSession")

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
    
    async def repair_all(self, session: aiohttp.ClientSession):
        """
        Проверяет все локально установленные версии и исправляет их.
        Перекачивает отсутствующие или поврежденные файлы.

        :param session: Активная HTTP-сессия.
        """
        installed = await self.list_installed()
        logger.info(f"Запуск проверки и восстановления для {len(installed)} версий...")
        
        for version in installed:
            logger.info(f"Проверка версии {version}...")
            await self.install_version(session, version)
        
        logger.info("Восстановление завершено.")

    def get_file_path(self, version: str, os_type: str, arch: str = "amd64", is_executable: bool = True) -> Path:
        """
        Формирует путь к конкретному файлу дистрибутива на основе параметров.

        :param version: Версия Python.
        :param os_type: Тип ОС ('windows', 'linux', 'macos').
        :param arch: Архитектура ('amd64', 'arm64').
        :param is_executable: Если True, ищет исполняемый файл (exe/pkg), иначе архив (zip/tar.xz).
        :return: Объект Path к файлу (даже если его нет на диске).
        :raises ValueError: Если ОС или параметры не поддерживаются.
        """
        os_type = os_type.lower()
        arch = arch.lower()
        
        filename = None
        
        if os_type == "windows":
            ext = "exe" if is_executable else "zip"
            filename = f"python-{version}-{arch}.{ext}"
        elif os_type == "linux":
            filename = f"Python-{version}.tar.xz"
        elif os_type == "macos":
            filename = f"python-{version}-macos11.pkg" if is_executable else f"Python-{version}.tar.xz"

        if not filename:
            raise ValueError(f"Не удалось определить имя файла для {os_type} {arch}")

        parts = (version, os_type, arch, filename)

        target_path = self.data_dir.joinpath(*parts).resolve()
        if not target_path.is_relative_to(self.data_dir.resolve()):
            raise ValueError(...)

        if not target_path.exists():
            logger.warning(f"Файл {filename} не найден локально. Возможно, он не был скачан.")
            
        return target_path
    
    def get_version_size(self, version: str) -> int:
        """Возвращает размер версии в байтах."""
        ver_dir = self._safe_path(version)
        if not ver_dir.exists():
            return 0
        return sum(f.stat().st_size for f in ver_dir.glob("**/*") if f.is_file())

    def get_total_size(self) -> int:
        """Возвращает суммарный размер всех установленных версий."""
        return sum(f.stat().st_size for f in self.data_dir.glob("**/*") if f.is_file())

    def remove_version(self, version: str) -> bool:
        """Полностью удаляет директорию указанной версии."""
        try:
            ver_dir = self._safe_path(version)
            if ver_dir.exists() and ver_dir.is_dir():
                shutil.rmtree(ver_dir)
                logger.info(f"[-] Версия {version} успешно удалена.")
                return True
            logger.warning(f" Попытка удаления: Версия {version} не найдена.")
            return False
        except Exception as e:
            logger.error(f" Ошибка при удалении версии {version}: {e}")
            return False



# --- БЛОК ДЛЯ ОТЛАДКИ ---
async def _debug_main():
    config = MirrorConfig(parallel=8)
    mirror = AsyncPythonMirror(config)
    
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        while True:
            print(f"\n--- МЕНЕДЖЕР ЗЕРКАЛА (Всего занято: {mirror._format_size(mirror.get_total_size())}) ---")
            print("1. Показать доступные версии (Сеть/Локально)")
            print("2. Скачать/Обновить версию")
            print("3. Удалить версию")
            print("4. Узнать вес версии")
            print("0. Выход")
            
            choice = input("\nВыбор: ").strip()

            if choice == '1':
                versions = await mirror.get_versions(session)
                installed = await mirror.list_installed()
                for v in versions:
                    status = "[УСТАНОВЛЕНО]" if v in installed else "[ДОСТУПНО]"
                    size = mirror._format_size(mirror.get_version_size(v)) if v in installed else ""
                    print(f"{status} {v} {size}")

            elif choice == '2':
                ver = input("Введите версию для загрузки: ").strip()
                await mirror.install_version(session, ver)

            elif choice == '3':
                ver = input("Какую версию удалить?: ").strip()
                mirror.remove_version(ver)

            elif choice == '4':
                ver = input("Введите версию: ").strip()
                size = mirror.get_version_size(ver)
                print(f"Размер {ver}: {mirror._format_size(size)}")

            elif choice == '0':
                break

if __name__ == "__main__":
    try:
        asyncio.run(_debug_main())
    except KeyboardInterrupt:
        pass