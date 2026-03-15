import pytest
import shutil
from pathlib import Path
from unittest.mock import MagicMock, AsyncMock, patch
from python_mirror import MirrorConfig, AsyncPythonMirror

# Фикстура для базового конфига
@pytest.fixture
def base_config(tmp_path):
    return MirrorConfig(
        data_dir=tmp_path,
        parallel=2,
        show_progress=False
    )

# Фикстура для инициализированного класса
@pytest.fixture
def mirror(base_config):
    return AsyncPythonMirror(base_config)

## --- Тесты конфигурации ---

def test_config_path_validation():
    # Проверка, что путь создается автоматически, если не передан
    config = MirrorConfig()
    assert config.data_dir is not None
    assert isinstance(config.data_dir, Path)

def test_config_invalid_parallel():
    with pytest.raises(ValueError):
        MirrorConfig(parallel=25)  # Превышает le=20

## --- Тесты безопасности и путей ---

def test_safe_path_protection(mirror, tmp_path):
    # Проверка защиты от выхода за пределы директории
    with pytest.raises(ValueError, match="Попытка доступа за пределы"):
        mirror._safe_path("../etc/passwd")
    
    # Проверка нормального пути
    valid_path = mirror._safe_path("3.12.0", "python.exe")
    assert str(valid_path).startswith(str(tmp_path))

def test_is_useful_file(mirror):
    assert mirror._is_useful_file("Python-3.11.0.tar.xz", "3.11.0") is True
    assert mirror._is_useful_file("python-3.11.0-amd64.exe", "3.11.0") is True
    assert mirror._is_useful_file("random_file.txt", "3.11.0") is False

## --- Асинхронные тесты ---

@pytest.mark.asyncio
async def test_check_disk_space_insufficient(mirror, mocker):
    # Имитируем ситуацию: нужно 10 ГБ, а свободно только 5 ГБ
    # Лимит безопасности — 3 ГБ. 
    # Свободно 5 - (10 * 2) = -15 (меньше 3)
    
    mock_session = AsyncMock()
    
    # Мокаем размер удаленных файлов (суммарно 10 ГБ)
    mocker.patch.object(mirror, '_get_remote_file_size', new=AsyncMock(return_value=10 * 1024**3))
    
    # Мокаем свободное место на диске (5 ГБ)
    mocker.patch('shutil.disk_usage', return_value=(0, 0, 5 * 1024**3))

    with pytest.raises(IOError, match="Недостаточно места"):
        await mirror._check_disk_space(mock_session, "3.12.0", ["file1"])

@pytest.mark.asyncio
async def test_get_versions_parsing(mirror, mocker):
    # Тест парсинга HTML версий
    mock_html = """
    <html>
        <a href="3.10.1/">3.10.1/</a>
        <a href="3.11.0/">3.11.0/</a>
        <a href="latest/">latest/</a>
    </html>
    """
    
    mock_resp = AsyncMock()
    mock_resp.text.return_value = mock_html
    mock_resp.raise_for_status = lambda: None
    mock_resp.__aenter__.return_value = mock_resp
    
    mock_session = MagicMock()
    mock_session.get.return_value = mock_resp

    versions = await mirror.get_versions(mock_session)
    
    assert "3.11.0" in versions
    assert "3.10.1" in versions
    assert "latest" not in versions
    assert versions[0] == "3.11.0"  # Проверка сортировки

@pytest.mark.asyncio
async def test_download_file_success(mirror, tmp_path, mocker):
    dest = tmp_path / "test.exe"
    url = "https://fake.url/test.exe"

    # Настройка мока ответа
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.raise_for_status = lambda: None

    async def mock_iter(chunk_size):
        for chunk in [b"data1", b"data2"]:
            yield chunk

    mock_resp.content.iter_chunked = MagicMock(side_effect=mock_iter)
    mock_resp.__aenter__.return_value = mock_resp

    mock_session = MagicMock()
    mock_session.get.return_value = mock_resp

    # Мок для aiofiles.open
    file_mock = AsyncMock()
    aiofiles_open_mock = mocker.patch("aiofiles.open", return_value=file_mock)
    file_mock.__aenter__.return_value = file_mock

    # Мок для os.replace
    with patch("os.replace") as mock_replace:
        success = await mirror._download_file(mock_session, url, dest)

        assert success is True

        # Проверяем, что aiofiles.open вызван с правильным временным файлом
        temp_path = dest.with_suffix(".download.tmp")
        aiofiles_open_mock.assert_called_once_with(temp_path, "wb")

        # Проверяем, что данные записаны
        assert file_mock.write.call_count == 2
        file_mock.write.assert_any_call(b"data1")
        file_mock.write.assert_any_call(b"data2")

        # Проверяем вызов os.replace
        mock_replace.assert_called_once()
        args, _ = mock_replace.call_args
        assert args[0] == temp_path  # временный файл
        assert args[1] == dest       # целевой файл