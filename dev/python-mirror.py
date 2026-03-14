import requests
import hashlib
import random
from pathlib import Path
from bs4 import BeautifulSoup
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import re

class PythonMirror:
    FILE_TYPES = (".tar.xz", ".zip", ".exe")

    def __init__(self, proxies=None):
        self.url_ftp = "https://www.python.org/ftp/python/"
        self.versions = []
        self.root_dir = Path(__file__).resolve().parent.parent

        self.proxies = self._setup_proxies(proxies)
        self.session = self._create_session()

    def _is_useful_file(self, name: str, version: str) -> bool:
        patterns = [
            rf"Python-{version}\.tar\.xz$",
            rf"python-{version}-amd64\.exe$",
            rf"python-{version}-arm64\.exe$",
            rf"python-{version}-amd64\.zip$",
            rf"python-{version}-arm64\.zip$",
        ]

        for p in patterns:
            if re.fullmatch(p, name):
                return True

        return False

    def _setup_proxies(self, proxies):
        if not proxies:
            return []
        
        if isinstance(proxies, list):
            return proxies

        path = Path(proxies)
        if path.exists():
            return [i.strip() for i in path.read_text().splitlines() if i.strip()]

        return [proxies]

    def _create_session(self):
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        if self.proxies:
            proxy = random.choice(self.proxies)
            session.proxies = {"http": proxy, "https": proxy}

        return session

    def get_versions(self, update=False):
        if self.versions and not update:
            return self.versions

        r = self.session.get(self.url_ftp, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        self.versions = [
            a.get("href").strip("/")
            for a in soup.find_all("a")
            if a.get("href") and a.get("href")[0].isdigit()
        ]

        return self.versions

    def _sha256(self, path):
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()

    def _download(self, url, dest):
        temp = dest.with_suffix(".tmp")
        sha = hashlib.sha256()
        print(f"Скачиваем {dest.name}...")
        with self.session.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(temp, "wb") as f:
                for chunk in r.iter_content(65536):
                    if not chunk:
                        continue
                    f.write(chunk)
                    sha.update(chunk)
        temp.rename(dest)
        return sha.hexdigest()
    
    def install_version(self, version):
        if version not in self.get_versions():
            print("Версия не найдена")
            return

        target = self.root_dir / "data" / "python-ver" / version
        target.mkdir(parents=True, exist_ok=True)
        version_url = f"{self.url_ftp}{version}/"
        r = self.session.get(version_url)
        soup = BeautifulSoup(r.text, "html.parser")

        files = []
        for a in soup.find_all("a"):
            name = a.get("href")
            if not name:
                continue
            if self._is_useful_file(name, version):
                files.append(name)

        print(f"Найдено файлов: {len(files)}")
        for name in files:
            url = version_url + name
            dest = target / name
            if dest.exists():
                print(f"Проверяем {name}...")
                local_hash = self._sha256(dest)
                print(f"SHA256: {local_hash}")
                continue
            sha = self._download(url, dest)
            print(f"SHA256: {sha}\n")


if __name__ == "__main__":
    mirror = PythonMirror()
    versions = mirror.get_versions()
    print(f"Доступно версий: {len(versions)}")
    v = input("Какую версию скачать? ").strip()
    mirror.install_version(v)