# Разработка плагинов для ASFES Multiplex

ASFES Multiplex спроектирован так, чтобы интеграция новых плагинов и инструментов была максимально простой и изолированной. Для добавления плагина не нужно изменять существующий код приложения или конфигурации — достаточно создать один Python-файл в директории `server/mcp/plugins/`.

---

## 1. Введение и архитектурный обзор

Система плагинов ASFES Multiplex построена на автоматическом обнаружении и динамической загрузке модулей. При запуске сервер сканирует директорию плагинов и загружает все объявленные манифесты и инструменты.

### Жизненный цикл плагина в системе

```
[ Старт приложения ]
        │
        ▼
[ PluginRegistry.load_plugins() ]
        │
        ├─► Сканирование server/mcp/plugins/ (pkgutil.iter_modules)
        ├─► Игнорирование файлов, начинающихся с "_" (например, _common.py)
        │
        ▼
[ Загрузка модуля плагина ]
        │
        ├─► importlib.import_module()
        ├─► Извлечение объекта PLUGIN (PluginDefinition)
        │
        ▼
[ Регистрация в системе ]
        │
        ├─► Регистрация прав в PermissionCatalog
        ├─► Сохранение метаданных плагина в MongoDB (коллекция PLUGINS)
        ├─► Создание глобальных политик для инструментов (TOOL_POLICIES)
        │
        ▼
[ Инициализация (Lifecycle Startup) ]
        │
        └─► Вызов plugin.startup(services) — если объявлен
        │
        ▼
[ Готов к работе / Вызовы инструментов ]
        │
        ▼
[ Завершение работы приложения ]
        │
        ▼
[ Вызов plugin.shutdown(services) ] — если объявлен
```

---

## 2. Анатомия типов данных

Все структуры данных для плагинов описаны в файле [models.py](../server/models.py). Ниже приведено описание ключевых полей каждого класса.

### 2.1 PluginManifest
Описывает метаданные самого плагина.

| Поле | Тип | Описание | Значение по умолчанию |
| :--- | :--- | :--- | :--- |
| `key` | `str` | Уникальный идентификатор плагина (snake_case). | *Обязательно* |
| `name` | `str` | Человекочитаемое название плагина для UI. | *Обязательно* |
| `version` | `str` | Версия плагина (SemVer, например `"1.0.0"`). | *Обязательно* |
| `description` | `str` | Краткое описание назначения плагина. | *Обязательно* |
| `os_support` | `list[str]` | Список поддерживаемых ОС. Допустимо: `"linux"`, `"windows"`. | `["linux", "windows"]` |
| `enabled_by_default` | `bool` | Включен ли плагин глобально при первом запуске. | `True` |
| `permissions` | `list[PermissionDefinition]` | Список прав, регистрируемых этим плагином. | `[]` |
| `required_backends` | `list[str]` | Необходимые системные CLI-утилиты или библиотеки. | `[]` |
| `providers` | `list[str]` | Список провайдеров ресурсов. | `[]` |

### 2.2 PermissionDefinition
Описывает право доступа.

| Поле | Тип | Описание |
| :--- | :--- | :--- |
| `key` | `str` | Уникальный строковый идентификатор права (например, `docker.containers.read`). |
| `description` | `str` | Пояснение, за что отвечает данное право (отображается в админ-панели). |

### 2.3 MCPToolManifest
Описывает метаданные конкретного MCP-инструмента.

| Поле | Тип | Описание | Значение по умолчанию |
| :--- | :--- | :--- | :--- |
| `key` | `str` | Уникальный ключ инструмента. Формат: `<plugin_key>.<action_name>`. | *Обязательно* |
| `name` | `str` | Название инструмента для отображения. | *Обязательно* |
| `description` | `str` | Описание инструмента (используется AI-клиентами для понимания функции). | *Обязательно* |
| `input_schema` | `dict` | Схема аргументов в формате JSON Schema. | *Обязательно* |
| `permissions` | `list[str]` | Список ключей прав, необходимых пользователю для вызова. | *Обязательно* |
| `tags` | `list[str]` | Теги для категоризации инструмента. | `[]` |
| `read_only` | `bool` | Является ли инструмент безопасным для чтения (без изменения состояния). | `False` |
| `default_global_enabled` | `bool` | Включать ли инструмент глобально по умолчанию. | `True` |
| `os_support` | `list[str]` | Поддерживаемые операционные системы для этого инструмента. | `["linux", "windows"]` |
| `required_backends` | `list[str]` | Необходимые зависимости для конкретного инструмента. | `[]` |
| `providers` | `list[str]` | Провайдеры инструмента. | `[]` |
| `audit_redact_fields` | `list[str]` | Список ключей из аргументов, значения которых нужно затереть в логах аудита (секреты, пароли). | `[]` |
| `audit_max_string_length` | `int` | Максимальная длина строки аргументов при выводе в лог аудита. | `512` |

> [!IMPORTANT]
> При первой регистрации инструмента, его начальное включенное состояние вычисляется как: `default_global_enabled AND read_only`. 
> Инструменты с `read_only=False` (выполняющие запись или деструктивные действия) всегда будут выключены глобально по умолчанию. Администратор должен явно включить их через интерфейс управления.

### 2.4 MCPTool
Связывает манифест инструмента с его кодовой реализацией и логикой доступности.

| Поле | Тип | Описание |
| :--- | :--- | :--- |
| `manifest` | `MCPToolManifest` | Манифест инструмента. |
| `handler` | `ToolHandler` | Асинхронная функция, обрабатывающая вызов. |
| `availability` | `AvailabilityHandler \| None` | Функция проверки доступности в текущей среде. |

### 2.5 PluginDefinition
Главный объект плагина, экспортируемый как `PLUGIN`.

| Поле | Тип | Описание |
| :--- | :--- | :--- |
| `manifest` | `PluginManifest` | Манифест плагина. |
| `tools` | `dict[str, MCPTool]` | Словарь инструментов плагина, где ключ — `tool_key`. |
| `startup` | `Callable[[ApplicationServices], Awaitable[None]] \| None` | Асинхронный хук инициализации. |
| `shutdown` | `Callable[[ApplicationServices], Awaitable[None]] \| None` | Асинхронный хук очистки ресурсов. |
| `availability` | `AvailabilityHandler \| None` | Функция проверки доступности всего плагина. |

---

## 3. Context выполнения (ToolExecutionContext)

Каждому обработчику инструмента при вызове передается объект `context: ToolExecutionContext`, предоставляющий безопасный доступ к состоянию системы и текущему пользователю:

*   `context.user`: Объект `UserPrincipal`
    *   `user_id`: Уникальный идентификатор пользователя.
    *   `username`: Имя пользователя.
    *   `is_root`: Флаг суперпользователя.
    *   `permissions`: Список прав пользователя.
*   `context.services`: Объект `ApplicationServices`
    *   Предоставляет доступ ко всем основным службам ядра приложения:
        *   `services.host_ops`: Выполнение CLI-команд, проверка прав доступа к путям.
        *   `services.db`: Доступ к базе данных MongoDB (через `DatabaseManager`).
        *   `services.alerts`: Управление правилами и событиями оповещений.
        *   `services.mailer`: Отправка email-уведомлений.
        *   `services.settings`: Конфигурация системы (параметры `.env`).
        *   `services.audit`: Запись системных событий в лог аудита.
*   `context.request_meta`: Метаданные HTTP/SSE-запроса (IP-адрес клиента, User-Agent и т.д.).

---

## 4. Написание функции-обработчика

Асинхронная функция-обработчик имеет строгую сигнатуру:
```python
async def my_handler(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    ...
```

### Правила обработки ошибок
Если при выполнении произошла ошибка, вы должны выбросить `RuntimeError`. Система перехватит её и вернёт клиенту в виде структурированной ошибки MCP Tool.

### Вспомогательные утилиты `server/mcp/plugins/_common.py`
Для валидации аргументов и путей используйте готовые функции:

*   `require_argument(arguments, "key")`: Возвращает значение аргумента или выбрасывает `RuntimeError`, если он отсутствует или пуст.
*   `string_list_argument(arguments, "key")`: Безопасно преобразует аргумент в список строк.
*   `int_argument(arguments, "key", default)`: Извлекает целое число.
*   `bool_argument(arguments, "key", default)`: Извлекает логическое значение.
*   `dict_argument(arguments, "key")`: Извлекает словарь.
*   `managed_path(context, raw_path, use_logs_root=False)`: Проверяет и резолвит путь в рамках ограничений безопасности `HOST_OPS__MANAGED_FILE_ROOTS` или `HOST_OPS__MANAGED_LOG_ROOTS`. Предотвращает уязвимости типа Path Traversal.
*   `command_result_payload(result)`: Преобразует результат выполнения CLI-команды (`CommandResult`) в словарь.

---

## 5. Система доступности (Availability System)

Плагины и инструменты могут автоматически отключаться, если в операционной системе отсутствует необходимая утилита (например, `docker` или `nginx`) или библиотека (`psutil`).

### Проверка через `static_availability`
Самый простой способ — использовать фабрику `static_availability`:

```python
from server.mcp.plugins._common import static_availability

# Инструмент требует утилиту docker в PATH
availability = static_availability(backend="docker")

# Требуется библиотека psutil
availability = static_availability(require_psutil=True)
```

### Кастомная проверка доступности
Если проверка сложнее, напишите асинхронную функцию:

```python
from server.models import RuntimeAvailability

async def custom_availability(services: ApplicationServices) -> RuntimeAvailability:
    if services.host_ops.is_windows:
        return RuntimeAvailability(available=True)
    return RuntimeAvailability(
        available=False, 
        reason="Этот плагин поддерживается только в операционной системе Windows."
    )
```

---

## 6. Хуки жизненного цикла (Startup / Shutdown)

Если плагину требуется инициализация фоновых задач, соединений или периодический опрос, используйте асинхронные хуки:

```python
import asyncio
from server.services import ApplicationServices

async def my_startup(services: ApplicationServices) -> None:
    # Запуск фонового процесса или инициализация ресурсов
    pass

async def my_shutdown(services: ApplicationServices) -> None:
    # Безопасное освобождение ресурсов при завершении приложения
    pass
```

Эти функции передаются в `PluginDefinition`:
```python
PLUGIN = PluginDefinition(
    manifest=...,
    tools=...,
    startup=my_startup,
    shutdown=my_shutdown,
)
```

---

## 7. Работа с `host_ops`

Ядро приложения предоставляет безопасный интерфейс для взаимодействия с хостом — `HostOpsService`. Доступ к нему осуществляется через `context.services.host_ops`.

Основные методы:
*   `await context.services.host_ops.run_backend(cmd_name, *args, check=True)`: Безопасно запускает внешнюю утилиту (если она разрешена в `HOST_OPS__PROCESS_ALLOWED_EXECUTABLES`).
*   `context.services.host_ops.is_linux`: Проверка работы под Linux.
*   `context.services.host_ops.is_windows`: Проверка работы под Windows.
*   `context.services.host_ops.resolve_managed_path(raw_path)`: Валидация пути.

---

## 8. Примеры плагинов

### Пример 1: Минимальный плагин (`mail.py`)
Простая отправка писем через настроенный в ядре SMTP-клиент.

```python
from __future__ import annotations
from typing import Any
from server.models import (
    MCPTool, MCPToolManifest, PermissionDefinition, 
    PluginDefinition, PluginManifest, ToolExecutionContext
)

async def send_test_email(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    recipient = arguments.get("recipient")
    subject = arguments.get("subject") or "Test email"
    body = arguments.get("body") or "Hello from Multiplex!"
    
    if not recipient:
        raise RuntimeError("Аргумент 'recipient' обязателен.")
        
    sent = await context.services.mailer.send_email(str(recipient), str(subject), str(body))
    if not sent:
        raise RuntimeError("Отправка SMTP недоступна или отключена.")
        
    return {"recipient": recipient, "sent": True}

PLUGIN = PluginDefinition(
    manifest=PluginManifest(
        key="mail",
        name="Почта",
        version="1.0.0",
        description="Отправка системных писем.",
        permissions=[
            PermissionDefinition(key="mail.send", description="Разрешает отправку писем")
        ],
    ),
    tools={
        "mail.send_test_email": MCPTool(
            manifest=MCPToolManifest(
                key="mail.send_test_email",
                name="Отправить тестовое письмо",
                description="Отправляет тестовое email-сообщение.",
                input_schema={
                    "type": "object",
                    "required": ["recipient"],
                    "properties": {
                        "recipient": {"type": "string"},
                        "subject": {"type": "string"},
                        "body": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
                permissions=["mail.send"],
                read_only=False,
                default_global_enabled=False,
            ),
            handler=send_test_email,
        )
    },
)
```

### Пример 2: Плагин с `psutil` (`system_stats.py`)
Использование сторонней библиотеки с автоматической проверкой её импорта.

```python
from __future__ import annotations
from typing import Any
from server.host_ops import _psutil
from server.mcp.plugins._common import static_availability
from server.models import (
    MCPTool, MCPToolManifest, PermissionDefinition, 
    PluginDefinition, PluginManifest, ToolExecutionContext
)

async def get_snapshot(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    if _psutil is None:
        raise RuntimeError("Библиотека psutil не установлена.")
        
    return {
        "cpu_percent": _psutil.cpu_percent(interval=0.1),
        "memory": _psutil.virtual_memory()._asdict(),
    }

PLUGIN = PluginDefinition(
    manifest=PluginManifest(
        key="system_stats",
        name="Системная статистика",
        version="1.0.0",
        description="Снятие метрик процессора и памяти хоста.",
        permissions=[
            PermissionDefinition(key="system.stats.read", description="Разрешает чтение метрик хоста")
        ],
        required_backends=["psutil"],
    ),
    tools={
        "system_stats.get_snapshot": MCPTool(
            manifest=MCPToolManifest(
                key="system_stats.get_snapshot",
                name="Получить снимок метрик",
                description="Возвращает показатели CPU и RAM.",
                input_schema={"type": "object", "properties": {}, "additionalProperties": False},
                permissions=["system.stats.read"],
                read_only=True,
                required_backends=["psutil"],
            ),
            handler=get_snapshot,
            availability=static_availability(require_psutil=True),
        )
    },
)
```

### Пример 3: Фоновая служба (`alerts.py`)
Плагин, управляющий фоновым циклом проверки правил.

```python
from __future__ import annotations
from server.models import PluginDefinition, PluginManifest

async def alerts_startup(services) -> None:
    await services.alerts.start()

async def alerts_shutdown(services) -> None:
    await services.alerts.stop()

PLUGIN = PluginDefinition(
    manifest=PluginManifest(
        key="alerts",
        name="Оповещения",
        version="1.0.0",
        description="Фоновый контроль триггеров хоста.",
        permissions=[],
    ),
    tools={},
    startup=alerts_startup,
    shutdown=alerts_shutdown,
)
```

---

## 9. Пошаговая инструкция: Создание плагина за 5 шагов

1.  **Создайте файл плагина**: Добавьте файл `server/mcp/plugins/my_plugin.py`. Имя файла определяет ключ плагина.
2.  **Импортируйте модели**: Добавьте необходимые типы из `server.models`.
3.  **Напишите асинхронный обработчик**: Создайте функцию `async def` для выполнения полезного действия.
4.  **Определите объект `PLUGIN`**: Создайте экземпляр `PluginDefinition` со всеми манифестами.
5.  **Запустите/перезапустите сервер**: Изменения подхватятся автоматически при перезапуске сервера (или используйте горячую перезагрузку).

---

## 10. Горячая перезагрузка плагинов (Hot Reload)

Вы можете обновить плагины в реальном времени без полной остановки сервера.
Для этого отправьте POST-запрос на:
`/api/admin/plugins/reload` с телом `{"plugin_keys": ["my_plugin"]}`.

Процесс перезагрузки:
1.  Вызывается хук `shutdown` текущего плагина (если он есть).
2.  Модуль Python принудительно перезагружается в памяти (`importlib.reload`).
3.  Вызывается хук `startup` новой версии плагина.
4.  Обновляются зарегистрированные инструменты в шлюзе FastMCP.

---

## 11. Управление через API и фильтрация прав

### Эндпоинты управления (требуют прав администратора)
*   `GET /api/admin/plugins` — Список всех установленных плагинов.
*   `GET /api/admin/tools` — Список инструментов и их глобальный статус включения.
*   `POST /api/admin/plugins/{key}/enable` / `disable` — Глобальное включение/выключение плагина.
*   `POST /api/admin/tools/{key}/enable` / `disable` — Глобальное включение/выключение конкретного инструмента.
*   `POST /api/admin/users/{user_id}/tools/{key}/enable` / `disable` — Включение/выключение инструмента для конкретного пользователя.

### Логика авторизации при вызове инструмента
Перед тем как запустить обработчик, FastMCP проверяет условия в следующем порядке:
1.  Включен ли MCP глобально в настройках сервера.
2.  Включен ли плагин в базе данных.
3.  Доступен ли плагин в среде выполнения (проверка ОС и утилит).
4.  Включена ли глобальная политика доступа к инструменту (`TOOL_POLICIES` scope="global").
5.  Если пользователь — `root`, доступ разрешается.
6.  Если обычный пользователь:
    *   Проверяется наличие всех прав из `tool.manifest.permissions` в профиле пользователя.
    *   Проверяется включение персональной политики доступа (`TOOL_POLICIES` scope="user" для `user_id`).

---

## 12. Соглашения об именах и правила безопасности

*   **Имена файлов и ключи**: Имя файла должно строго соответствовать ключу плагина в `PluginManifest` (например, `docker.py` -> `docker`).
*   **Именование прав**: Формат `<plugin_key>.<entity>.<action>`. Например, `docker.containers.stop`.
*   **Именование инструментов**: Формат `<plugin_key>.<action_name>`. Например, `docker.stop_container`.
*   **Безопасность путей**: Никогда не склеивайте пути вручную с помощью оператора `+` или f-строк. Всегда используйте утилиту `managed_path()` для защиты от Path Traversal.
*   **Секреты**: Не логгируйте аргументы, содержащие пароли или токены. Обязательно вносите их в `audit_redact_fields`.

---

## 13. Тестирование плагинов

В соответствии с правилами проекта, избегайте избыточного мокирования низкоуровневых операций. Предпочитайте легкие тесты интеграции и проверку структуры.

### Пример Smoke-теста структуры плагина (`tests/test_my_plugin.py`)
```python
from server.mcp.plugins.my_plugin import PLUGIN

def test_plugin_manifest():
    assert PLUGIN.manifest.key == "my_plugin"
    assert len(PLUGIN.tools) > 0
    
    # Проверка прав
    permissions = [p.key for p in PLUGIN.manifest.permissions]
    assert "my_plugin.read" in permissions
```

### Пример Unit-теста обработчика
```python
import pytest
from unittest.mock import AsyncMock, MagicMock
from server.mcp.plugins.my_plugin import get_status
from server.models import ToolExecutionContext, UserPrincipal

@pytest.mark.asyncio
async def test_get_status_handler():
    # Mocking context
    user = UserPrincipal(user_id="user_1", username="test_user", is_root=False)
    services = MagicMock()
    context = ToolExecutionContext(user=user, services=services, request_meta={})
    
    arguments = {"name": "test_entity"}
    
    result = await get_status(context, arguments)
    
    assert result["status"] == "ok"
    assert result["entity_name"] == "test_entity"
```

---

## 14. Чеклист разработчика плагина

- [ ] Файл плагина размещен в `server/mcp/plugins/` и не имеет префикса `_`.
- [ ] Ключ плагина (`key`) совпадает с именем файла Python.
- [ ] Все ключи инструментов начинаются с префикса `<plugin_key>.`.
- [ ] Инструменты, изменяющие состояние хоста (`read_only=False`), имеют `default_global_enabled=False`.
- [ ] Все права доступа описаны в манифесте плагина (`manifest.permissions`).
- [ ] Вводные аргументы валидируются через хелперы из `_common.py`.
- [ ] Взаимодействие с путями происходит строго через хелпер `managed_path`.
- [ ] Все чувствительные аргументы скрыты с помощью `audit_redact_fields`.
- [ ] Написаны smoke-тесты на проверку структуры манифеста.
- [ ] Запущены линтеры: `.venv/Scripts/python.exe -m ruff check . --fix`.
- [ ] Все pytest-тесты проходят успешно: `.venv/Scripts/python.exe -m pytest tests`.
