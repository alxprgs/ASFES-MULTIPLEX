import asyncio
import aiohttp
import aiofiles
import random
import hashlib
import logging
import shutil
import time
from typing import List, Optional, Dict
from pathlib import Path
from pydantic import BaseModel, Field
from packaging import version

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AsyncPypiMirror")

class MirrorConfig(BaseModel):
    """
    Конфигурация зеркала PyPI с валидацией через Pydantic.
    """
    api_base: str = "https://pypi.org/pypi"
    data_dir: Path = Field(default_factory=lambda: Path(__file__).resolve().parent / "pypi_storage")
    proxies: List[str] = Field(default_factory=list)
    network_mode: str = Field(default="direct", pattern="^(direct|proxy|mix)$")
    rate_limit_mb: Optional[float] = Field(default=None, gt=0)
    parallel: int = Field(default=5, ge=1)
    max_retries: int = Field(default=3)
    min_safe_space_gb: float = 3.0

class AsyncPypiMirror:
    """
    Класс для асинхронного зеркалирования библиотек из репозитория PyPI.
    Поддерживает проверку целостности, управление прокси и ограничение скорости.
    """

    def __init__(self, config: MirrorConfig):
        """
        Инициализирует экземпляр зеркала.

        :param config: Объект конфигурации MirrorConfig.
        """
        self.cfg = config
        self.cfg.data_dir.mkdir(parents=True, exist_ok=True)
        self._rate_limit_bytes = (self.cfg.rate_limit_mb * 1024 * 1024) if self.cfg.rate_limit_mb else None

    # --- СЕРВИСНЫЕ МЕТОДЫ ---

    def _choose_proxy(self) -> Optional[str]:
        """
        Выбирает прокси-сервер в зависимости от установленного режима сети.

        :return: Строка с адресом прокси или None для прямого соединения.
        """
        if self.cfg.network_mode == "direct" or not self.cfg.proxies:
            return None
        if self.cfg.network_mode == "proxy":
            return random.choice(self.cfg.proxies)
        if self.cfg.network_mode == "mix":
            return random.choice([None, random.choice(self.cfg.proxies)])
        return None

    def _get_pkg_dir(self, name: str) -> Path:
        """
        Возвращает путь к корневой директории библиотеки.

        :param name: Имя библиотеки.
        :return: Объект Path.
        """
        return self.cfg.data_dir / name.lower()

    def _get_ver_dir(self, name: str, ver: str) -> Path:
        """
        Возвращает путь к директории конкретной версии библиотеки.

        :param name: Имя библиотеки.
        :param ver: Строка версии.
        :return: Объект Path.
        """
        return self._get_pkg_dir(name) / ver

    async def _verify_hash(self, file_path: Path, expected_sha256: str) -> bool:
        """
        Проверяет целостность файла, сравнивая его SHA256-хеш с ожидаемым.

        :param file_path: Путь к проверяемому файлу.
        :param expected_sha256: Ожидаемое значение хеша в формате hex.
        :return: True, если хеши совпадают или файл корректен.
        """
        if not file_path.exists():
            return False
        sha256_hash = hashlib.sha256()
        async with aiofiles.open(file_path, "rb") as f:
            while chunk := await f.read(8192):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest() == expected_sha256

    async def _fetch_metadata(self, session: aiohttp.ClientSession, name: str) -> Optional[dict]:
        """
        Запрашивает метаданные библиотеки через PyPI JSON API.

        :param session: Активная HTTP-сессия.
        :param name: Имя библиотеки.
        :return: Словарь с данными из JSON или None при ошибке/отсутствии сети.
        """
        url = f"{self.cfg.api_base}/{name}/json"
        try:
            async with session.get(url, proxy=self._choose_proxy(), timeout=10) as resp:
                if resp.status == 200:
                    return await resp.json()
        except Exception as e:
            logger.warning(f"Не удалось получить данные из PyPI для {name}: {e}")
        return None

    async def _check_disk_space(self, required_bytes: int):
        """
        Проверяет наличие свободного места на диске с учетом защитного порога.

        :param required_bytes: Объем данных в байтах, который планируется скачать.
        :raises IOError: Если места недостаточно для безопасной работы системы.
        """
        _, _, free = shutil.disk_usage(self.cfg.data_dir)
        min_bytes = self.cfg.min_safe_space_gb * 1024**3
        if (free - required_bytes) < min_bytes:
            raise IOError(f"Критически мало места! Требуется {required_bytes/1024**2:.2f}MB")

    # --- ОСНОВНЫЕ ФУНКЦИИ ---

    async def download_version(self, session: aiohttp.ClientSession, name: str, ver: str) -> bool:
        """
        Скачивает все файлы конкретной версии библиотеки с проверкой хешей.

        :param session: Активная HTTP-сессия.
        :param name: Имя библиотеки.
        :param ver: Строка версии.
        :return: True, если все файлы версии успешно загружены и проверены.
        """
        metadata = await self._fetch_metadata(session, name)
        if not metadata or ver not in metadata['releases']:
            logger.error(f"Версия {ver} для {name} не найдена.")
            return False

        release_files = metadata['releases'][ver]
        ver_dir = self._get_ver_dir(name, ver)
        ver_dir.mkdir(parents=True, exist_ok=True)

        success = True
        for file_info in release_files:
            file_url = file_info['url']
            file_name = file_info['filename']
            expected_hash = file_info['digests']['sha256']
            dest = ver_dir / file_name

            if await self._verify_hash(dest, expected_hash):
                continue

            await self._check_disk_space(file_info['size'])
            
            try:
                await self._download_file(session, file_url, dest)
                if not await self._verify_hash(dest, expected_hash):
                    logger.error(f"Ошибка хеша: {file_name}")
                    dest.unlink()
                    success = False
            except Exception as e:
                logger.error(f"Ошибка скачивания {file_name}: {e}")
                success = False
        
        return success

    async def _download_file(self, session: aiohttp.ClientSession, url: str, dest: Path):
        """
        Низкоуровневая функция загрузки файла с поддержкой ограничения скорости.

        :param session: Активная HTTP-сессия.
        :param url: Прямая ссылка на файл.
        :param dest: Путь для сохранения.
        """
        proxy = self._choose_proxy()
        temp = dest.with_suffix(".tmp")
        async with session.get(url, proxy=proxy) as resp:
            resp.raise_for_status()
            start = time.time()
            downloaded = 0
            async with aiofiles.open(temp, "wb") as f:
                async for chunk in resp.content.iter_chunked(65536):
                    await f.write(chunk)
                    downloaded += len(chunk)
                    if self._rate_limit_bytes:
                        elapsed = time.time() - start
                        expected = downloaded / self._rate_limit_bytes
                        if expected > elapsed:
                            await asyncio.sleep(expected - elapsed)
        if dest.exists(): dest.unlink()
        temp.rename(dest)

    async def download_all_versions(self, session: aiohttp.ClientSession, name: str):
        """
        Скачивает все существующие в PyPI версии указанной библиотеки.

        :param session: Активная HTTP-сессия.
        :param name: Имя библиотеки.
        """
        metadata = await self._fetch_metadata(session, name)
        if not metadata: return
        
        versions_list = list(metadata['releases'].keys())
        logger.info(f"Найдено {len(versions_list)} версий для {name}.")
        
        sem = asyncio.Semaphore(self.cfg.parallel)
        async def task(v):
            async with sem:
                return await self.download_version(session, name, v)
        
        await asyncio.gather(*(task(v) for v in versions_list))

    async def check_integrity(self, session: aiohttp.ClientSession, name: Optional[str] = None):
        """
        Проверяет целостность скачанных файлов, сопоставляя их с данными API.

        :param session: Активная HTTP-сессия.
        :param name: Имя конкретной библиотеки (если None — проверит все).
        """
        libs = [name] if name else [d.name for d in self.cfg.data_dir.iterdir() if d.is_dir()]
        
        for lib in libs:
            metadata = await self._fetch_metadata(session, lib)
            if not metadata: continue
                
            pkg_dir = self._get_pkg_dir(lib)
            for ver_path in pkg_dir.iterdir():
                if ver_path.is_dir() and ver_path.name in metadata['releases']:
                    for file_info in metadata['releases'][ver_path.name]:
                        dest = ver_path / file_info['filename']
                        if not await self._verify_hash(dest, file_info['digests']['sha256']):
                            logger.warning(f"[!] Файл поврежден: {dest}")

    def delete_library(self, name: str, ver: Optional[str] = None):
        """
        Удаляет локальные файлы библиотеки или её конкретной версии.

        :param name: Имя библиотеки.
        :param ver: Опциональная строка версии.
        """
        target = self._get_ver_dir(name, ver) if ver else self._get_pkg_dir(name)
        if target.exists():
            shutil.rmtree(target)
            logger.info(f"Удалено: {target}")

    async def get_info(self, session: aiohttp.ClientSession, name: str) -> dict:
        """
        Собирает информацию о статусе библиотеки (локальном и удаленном).

        :param session: Активная HTTP-сессия.
        :param name: Имя библиотеки.
        :return: Словарь со списками скачанных и отсутствующих версий.
        """
        metadata = await self._fetch_metadata(session, name)
        pkg_dir = self._get_pkg_dir(name)
        
        downloaded = [d.name for d in pkg_dir.iterdir() if d.is_dir()] if pkg_dir.exists() else []
        remote = list(metadata['releases'].keys()) if metadata else []
        
        return {
            "name": name,
            "is_downloaded": pkg_dir.exists(),
            "downloaded_versions": downloaded,
            "missing_versions": [v for v in remote if v not in downloaded]
        }

    def list_libraries(self, include_versions: bool = False) -> Dict[str, List[str]]:
        """
        Возвращает список всех библиотек в локальном хранилище.

        :param include_versions: Нужно ли включать список версий для каждой библиотеки.
        :return: Словарь {имя_библиотеки: [список_версий]}.
        """
        result = {}
        for lib_dir in self.cfg.data_dir.iterdir():
            if lib_dir.is_dir():
                result[lib_dir.name] = [v.name for v in lib_dir.iterdir() if v.is_dir()] if include_versions else []
        return result

    def get_path(self, name: str, version_str: Optional[str] = None) -> Optional[Path]:
        """
        Возвращает путь к директории библиотеки. 
        Если версия не указана, возвращает путь к самой свежей версии.

        :param name: Имя библиотеки.
        :param version_str: Опциональная строка версии.
        :return: Объект Path или None, если библиотека не найдена.
        """
        pkg_dir = self._get_pkg_dir(name)
        if not pkg_dir.exists(): return None
        if version_str:
            path = pkg_dir / version_str
            return path if path.exists() else None
        
        versions_dirs = [d for d in pkg_dir.iterdir() if d.is_dir()]
        if not versions_dirs: return None

        try:
            return max(versions_dirs, key=lambda x: version.parse(x.name))
        except:
            return sorted(versions_dirs)[-1]

# --- ПРИМЕР ---
async def main():
    config = MirrorConfig(rate_limit_mb=5.0)
    mirror = AsyncPypiMirror(config)
    async with aiohttp.ClientSession() as session:
        await mirror.download_version(session, "pydantic", "2.6.0")
        print(f"Путь к последней версии: {mirror.get_path('pydantic')}")

if __name__ == "__main__":
    asyncio.run(main())