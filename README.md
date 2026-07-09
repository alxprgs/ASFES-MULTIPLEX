<div align="center">

# 🚀 ASFES Multiplex

**Домашний FastAPI + MCP control plane с React-админкой**

[![Python](https://img.shields.io/badge/Python-3.11%2B-blue?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115%2B-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![React](https://img.shields.io/badge/React-TypeScript-61DAFB?logo=react&logoColor=white)](https://react.dev/)
[![MongoDB](https://img.shields.io/badge/MongoDB-Required-47A248?logo=mongodb&logoColor=white)](https://www.mongodb.com/)
[![Redis](https://img.shields.io/badge/Redis-Optional-DC382D?logo=redis&logoColor=white)](https://redis.io/)
[![FastMCP](https://img.shields.io/badge/FastMCP-2.x-purple?logo=anthropic&logoColor=white)](https://github.com/jlowin/fastmcp)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Version](https://img.shields.io/badge/version-0.1.4-informational)](pyproject.toml)
[![Coverage](https://img.shields.io/badge/coverage-%E2%89%A580%25-brightgreen)](pyproject.toml)
[![Code Style: Ruff](https://img.shields.io/badge/code%20style-ruff-orange?logo=ruff)](https://github.com/astral-sh/ruff)

</div>

---

ASFES Multiplex — централизованный control plane для управления локальным сервером. В production backend, MCP-шлюз и React SPA работают **на одном HTTP-порту** (по умолчанию `8000`). Предоставляет REST API, MCP-шлюз для AI-агентов, pip-совместимое PyPI-зеркало, зеркало дистрибутивов Python и прокси-менеджер — всё через единственный порт.

---

## 📋 Содержание

- [Архитектура](#-архитектура)
- [Функциональность](#-функциональность)
  - [Аутентификация и безопасность](#-аутентификация-и-безопасность)
  - [MCP Gateway (AI-агенты)](#-mcp-gateway-ai-агенты)
  - [Host Operations (управление сервером)](#-host-operations-управление-сервером)
  - [PyPI-зеркало](#-pypi-зеркало-pep-503)
  - [Python Distribution Mirror](#-python-distribution-mirror)
  - [Proxy Manager](#-proxy-manager)
  - [Alerting (система алертов)](#-alerting-система-алертов)
  - [Audit Log (журнал аудита)](#-audit-log-журнал-аудита)
  - [Update Manager (обновления)](#-update-manager-обновления)
  - [React Admin UI](#-react-admin-ui)
  - [Home Assistant Integration](#-home-assistant-integration)
  - [Observability (мониторинг и логи)](#-observability-мониторинг-и-логи)
- [Первый запуск](#-первый-запуск)
  - [Windows / PowerShell](#windows--powershell)
  - [Debian / Ubuntu](#debianubuntu-production)
- [Конфигурация](#-конфигурация-переменные-окружения)
- [Безопасность](#-безопасность)
- [Тесты и линтинг](#-тесты-и-линтинг)
- [Маршрутизация](#-таблица-маршрутизации)
- [Технологический стек](#-технологический-стек)

---

## 🏗 Архитектура

```
┌─────────────────────────────────────────────────────────┐
│                  0.0.0.0:8000 (single port)             │
├──────────────┬──────────────┬───────────┬───────────────┤
│   /api/*     │    /mcp/*    │  /pypi/*  │  /* (SPA)     │
│  FastAPI     │  FastMCP     │  PEP 503  │  React UI     │
│  REST API    │  Gateway     │  Mirror   │               │
├──────────────┴──────────────┴───────────┴───────────────┤
│              MongoDB (required) + Redis (optional)       │
└─────────────────────────────────────────────────────────┘
```

| Префикс          | Назначение                                                |
|------------------|-----------------------------------------------------------|
| `/api/*`         | FastAPI REST API (auth, admin, health, oauth, pypi, proxy)|
| `/pypi/*`        | pip-совместимый Simple API (`/pypi/simple/`)              |
| `/mcp/*`         | FastMCP gateway (MCP-протокол, SSE, OAuth/PKCE)           |
| `/.well-known/*` | OAuth discovery, JWKS endpoint                            |
| `/assets/*`      | Статические файлы React-сборки                            |
| `/*`             | React SPA (catch-all, SPA routing)                        |

> **Важно:** Попытки обратиться к `/api`, `/mcp`, `/pypi` или `/.well-known` через catch-all маршрут намеренно возвращают **404** — это не баг, а механизм защиты маршрутов.

---

## ⚡ Функциональность

### 🔐 Аутентификация и безопасность

Многоуровневая система аутентификации, поддерживающая несколько режимов одновременно:

| Метод | Описание |
|-------|----------|
| **JWT + HttpOnly Cookie** | Браузерный flow: токены хранятся в `httponly`-куках, недоступных JS |
| **CSRF Protection** | Double Submit Cookie pattern: `X-CSRF-Token` заголовок для всех write-запросов |
| **Bearer Token (API)** | `Authorization: Bearer <token>` для скриптов и MCP-клиентов — без CSRF |
| **OAuth 2.0 / PKCE** | Полный flow для MCP-клиентов: authorize → code → token → refresh |
| **Refresh Token** | Автоматическое обновление сессии без повторного логина |
| **WebAuthn / Passkeys** | Аппаратные ключи безопасности (FIDO2) — беспарольная аутентификация |
| **TOTP (2FA)** | Двухфакторная аутентификация через Google Authenticator, Authy и т.д. |
| **API Keys** | Долгоживущие ключи для автоматизации и CI/CD |
| **Rate Limiting** | Ограничение частоты запросов: отдельные лимиты для login, register, OAuth, REST, MCP |

**Управление пользователями:**
- Root-пользователь создаётся автоматически через переменные `ROOT__*`
- Регистрация новых пользователей выключена по умолчанию (включается в UI)
- Гранулярные permissions: назначение прав каждому пользователю точечно
- Смена пароля, обновление профиля, управление passkeys через UI

---

### 🤖 MCP Gateway (AI-агенты)

Полноценный **Model Context Protocol** шлюз на базе [FastMCP](https://github.com/jlowin/fastmcp), позволяющий AI-ассистентам (Claude, Cursor, Windsurf, Copilot и другим MCP-совместимым клиентам) напрямую управлять сервером.

**Возможности:**
- SSE (Server-Sent Events) транспорт — стандартный MCP-протокол
- OAuth 2.0 / PKCE авторизация для MCP-клиентов с `/.well-known/oauth-authorization-server`
- Динамическая регистрация OAuth-клиентов (RFC 7591)
- JWKS endpoint для верификации токенов внешними сервисами
- Тонкая настройка прав: каждый MCP-инструмент управляется через систему permissions
- **Write-tools глобально выключены** при первом запуске — включаются точечно
- Rate limiting на MCP-запросы отдельно от REST
- Middleware аудита: все вызовы tools логируются

**Подключение MCP-клиента:**
```json
{
  "mcpServers": {
    "asfes-multiplex": {
      "url": "http://localhost:8000/mcp",
      "transport": "http"
    }
  }
}
```

---

### 🖥 Host Operations (управление сервером)

Набор MCP-плагинов и REST API для управления локальным сервером. Все операции ограничены конфигурируемыми путями и whitelist-ом исполняемых файлов.

#### 🐳 Docker (`docker.py`, `docker_compose.py`)
- Список контейнеров, образов, сетей, томов
- Запуск, остановка, перезапуск, удаление контейнеров
- Получение логов контейнера в реальном времени
- Управление Docker Compose проектами (up, down, pull, logs)
- Просмотр статистики ресурсов контейнеров

#### 📁 File Manager (`file_manager.py`)
- Чтение, запись, создание, удаление файлов и директорий
- Строгое ограничение: операции только внутри `HOST_OPS__MANAGED_FILE_ROOTS`
- Защита от path traversal атак
- Листинг директорий с метаданными

#### 📊 Logs Viewer (`logs_viewer.py`)
- Чтение лог-файлов из разрешённых директорий (`HOST_OPS__MANAGED_LOG_ROOTS`)
- Tail (последние N строк), поиск по паттерну
- Поддержка больших файлов без загрузки в память целиком

#### ⚙️ Process Manager (`process_manager.py`)
- Запуск разрешённых исполняемых файлов (`HOST_OPS__PROCESS_ALLOWED_EXECUTABLES`)
- Получение списка активных процессов через `psutil`
- Завершение процессов по PID
- Системная статистика CPU/RAM/Disk

#### 🔥 Firewall (`firewall.py`)
- Управление правилами UFW / iptables
- Добавление, удаление, просмотр правил
- Переключение между провайдерами через `HOST_OPS__PROVIDER_OVERRIDES`

#### 🕒 Scheduler (`scheduler.py`)
- Создание и управление cron-задачами
- Просмотр запланированных задач
- Ручной запуск задач

#### 🔒 SSL (`ssl.py`)
- Управление SSL-профилями и сертификатами
- Просмотр сроков действия сертификатов
- Хранение профилей в `HOST_OPS__SSL_PROFILES_DIRECTORY`

#### 🌐 VPN (`vpn.py`)
- Управление WireGuard / OpenVPN профилями
- Просмотр статуса VPN-подключений
- Профили хранятся в `HOST_OPS__VPN_PROFILES_DIRECTORY`

#### 🔀 Nginx (`nginx.py`)
- Управление конфигурациями Nginx
- Проверка конфигурации (`nginx -t`)
- Перезагрузка/перезапуск Nginx
- Пути к конфигам задаются в `HOST_OPS__NGINX_CONFIG_PATHS`

#### 📈 System Stats (`system_stats.py`)
- CPU, RAM, Disk, сеть — через `psutil`
- Аптайм системы, load average

#### 🔌 Port Scanner (`ports_scanner.py`)
- Сканирование открытых портов на разрешённых хостах
- Whitelist хостов: `HOST_OPS__PORT_PROBE_ALLOWED_HOSTS`

#### 🗄 Database Manager (`database_manager.py`)
- Управление профилями подключения к базам данных
- Хранение профилей в `HOST_OPS__DATABASE_PROFILES_DIRECTORY`

#### 📧 Mail (`mail.py`)
- Отправка email-уведомлений через настроенный SMTP

#### 🔔 Alerts (`alerts.py` plugin)
- Управление правилами алертов через MCP

---

### 📦 PyPI-зеркало (PEP 503)

Встроенный pip-совместимый прокси-репозиторий пакетов Python. Полностью реализует [Simple Repository API (PEP 503)](https://peps.python.org/pep-0503/).

**Использование:**
```bash
pip install flask --index-url http://<ваш-хост>:8000/pypi/simple/
```

**Ключевые возможности:**

| Функция | Описание |
|---------|----------|
| **On-Demand Proxy** | Пакет отсутствует локально → автоматически скачивается с PyPI.org, верифицируется хэш и сохраняется в кэш |
| **Управление пакетами** | Просмотр размера, количества версий; ручное скачивание конкретной или всех версий |
| **Массовый импорт** | Загрузка списка зависимостей из файла (`requirements.txt`-формат) |
| **Blacklist** | Блокировка нежелательных пакетов/версий — запросы через Simple API возвращают `HTTP 404` |
| **Аудит** | Все операции (скачивание, удаление, блокировка) фиксируются в журнале |
| **Rate limiting** | Ограничение скорости скачивания (`PYPI__RATE_LIMIT_MB`) |
| **Режимы сети** | `direct` / `proxy` / `mix` — поддержка SOCKS5 и HTTP прокси |
| **Проверка хэша** | Верификация SHA-256 при скачивании с upstream |
| **Контроль места** | Минимальный порог свободного диска (`PYPI__MIN_SAFE_SPACE_GB`) |
| **Публичный доступ** | Опциональный доступ к Simple API без авторизации (`PYPI__PUBLIC_ACCESS`) |

**Конфигурация:**
```env
PYPI__ENABLED=true
PYPI__DATA_DIR=data/pypi_storage
PYPI__ON_DEMAND_PROXY=true
PYPI__PUBLIC_ACCESS=true
PYPI__PARALLEL=5
PYPI__RATE_LIMIT_MB=         # пусто = без ограничений
PYPI__NETWORK_MODE=direct    # direct | proxy | mix
```

---

### 🐍 Python Distribution Mirror

Зеркало официальных дистрибутивов **Python** (`.tar.gz`, `.pkg`, `.exe`, `.msi` и другие форматы) с python.org. Отдельный сервис от PyPI-зеркала.

**Возможности:**
- Просмотр и скачивание официальных релизов Python
- Фоновые задачи скачивания с отслеживанием прогресса в UI
- Фильтрация и поиск версий/платформ
- Persistent хранение в MongoDB (job tracking)
- Cache-файл `.mirror_cache.json` для быстрого листинга

---

### 🔀 Proxy Manager

Централизованное управление HTTP/SOCKS5 прокси-серверами с шифрованием учётных данных.

**Возможности:**
- Добавление прокси вручную или из URL (`protocol://user:pass@host:port`)
- Массовый импорт из файла в формате Proxifier
- Экспорт в форматы: Proxifier, URL-строки, plain text, Telegram-форматирование
- Автоматическая проверка работоспособности прокси
- Шифрование паролей прокси в базе данных (`SECURITY__PROXY_ENCRYPTION_KEY`)
- **SSRF-защита**: блокировка приватных и зарезервированных IP-адресов (RFC 1918, link-local, CGNAT)
- Хранение истории проверок с метриками latency

---

### 🚨 Alerting (система алертов)

Фоновый сервис мониторинга с настраиваемыми правилами и уведомлениями.

**Возможности:**
- Создание правил алертов на основе системных метрик (CPU, RAM, диск, сеть и т.д.)
- Настраиваемый интервал опроса (`HOST_OPS__ALERT_POLL_INTERVAL_SECONDS`)
- Уведомления по email через настроенный SMTP
- История сработавших событий алертов (alert events)
- Управление через Admin UI и MCP-инструменты
- Статус `active` / `resolved` для каждого события

---

### 📋 Audit Log (журнал аудита)

Полный журнал всех действий пользователей и системы с гарантией целостности.

**Возможности:**
- Логирование всех API-запросов (actor, action, target, result, metadata)
- Запись в MongoDB + дублирование в файлы и SQLite
- **Integrity verification**: фоновая проверка целостности записей (HMAC-цепочка)
- Интервал проверки: `LOGGING__VERIFIER_INTERVAL_SECONDS` (по умолчанию 10 минут)
- Архивация старых записей
- Просмотр и фильтрация через Admin UI (вкладка Audit)
- Email-уведомления при обнаружении нарушений целостности

---

### 🔄 Update Manager (обновления)

Встроенный менеджер обновлений для production-инсталляций на Debian/Ubuntu.

**Стадии обновления:**
1. **Код приложения** — git pull из репозитория
2. **Python-зависимости** — обновление pip-пакетов
3. **Frontend** — `npm install && npm run build`
4. **Перезапуск сервиса** — systemd restart

**Возможности:**
- Streaming прогресса через SSE (видно в реальном времени в UI)
- Сессии обновления с историей
- Аудит каждого обновления
- Отдельные кнопки: полное обновление или только перезапуск

---

### 🖼 React Admin UI

Полнофункциональная React + TypeScript админ-панель, работающая как SPA на том же порту.

| Вкладка | Функциональность |
|---------|-----------------|
| **Dashboard** | Системные метрики, статус сервисов, последние события аудита |
| **Runtime Settings** | Включение/выключение регистрации, MCP, Redis в реальном времени |
| **Users** | Управление пользователями: создание, редактирование, блокировка |
| **Permissions** | Назначение прав доступа каждому пользователю |
| **Plugins** | Список MCP-плагинов, их статус и перезагрузка |
| **Tools** | MCP-инструменты: просмотр, включение/выключение, политики |
| **OAuth Clients** | Управление OAuth-клиентами, ротация секретов |
| **PyPI** | Управление кэшем пакетов, blacklist, массовый импорт |
| **Python Mirror** | Загрузка дистрибутивов Python, очередь задач |
| **Proxy** | Управление прокси-серверами, проверка работоспособности |
| **Alerts** | Правила алертов, история событий |
| **Audit** | Журнал событий с фильтрацией |
| **Profile** | Профиль, смена пароля, 2FA, Passkeys, API-ключи, подключения Home Assistant |
| **System Update** | Обновление системы, перезапуск сервиса |

---

### 🏡 Home Assistant Integration

Полноценная интеграция для мониторинга и управления ASFES Multiplex прямо из Home Assistant:
- **Состояние и сенсоры**: 11 сенсоров (загрузка CPU, RAM, диска, uptime, температура, сеть, запущенные контейнеры Docker и процессы, активные подключения Redis/MongoDB) и 6 бинарных сенсоров состояния внутренних сервисов.
- **Управление (Switches)**: включение/выключение регистрации, MCP и Redis (управляется флагом `HA__SWITCHES_ENABLED` на сервере).
- **Действия (Buttons)**: перезагрузка плагинов, обновление кэша зеркал Python/PyPI, а также системный перезапуск Multiplex и Docker (управляется флагом `HA__DESTRUCTIVE_BUTTONS_ENABLED` на сервере).
- **Повышенная безопасность**: полная изоляция JWT-токенов интеграции от стандартных токенов API, ротация refresh-токенов и защита от атак повторного воспроизведения (replay attacks). Управление активными сессиями интеграции доступно прямо из профиля пользователя.

---

### 📊 Observability (мониторинг и логи)

Встроенный стек для сбора логов, метрик и трассировок, полностью отключаемый и настраиваемый через `.env`.

- **Prometheus метрики**: Экспорт более чем 20 метрик через единый эндпоинт `/api/metrics` (активные http-запросы, CPU/RAM/Disk, скачивания PyPI, события алертов, нарушения целостности логов и т.д.). Доступ к эндпоинту может быть защищен пермишеном `system.metrics.read` или сделан публичным для скрейпинга Prometheus.
- **Loki Log Forwarding**: Асинхронный, буферизированный форвардер логов в Grafana Loki на основе `asyncio.Queue`. Работает без блокировки основного потока исполнения и поддерживает batch-отправку, экспоненциальный retry и защиту от переполнения памяти при падении Loki.
- **OpenTelemetry Tracing**: Опциональная трассировка HTTP-запросов и операций баз данных. Настраивается через conditional import и не создает оверхеда, если OTel-пакеты не установлены в системе.

---

---

## 🚀 Первый запуск

### Требования перед стартом

> ⚠️ **MongoDB обязательна.** Если MongoDB недоступна — backend не стартует. Это намеренная защита.

1. Python **3.11+** и Node.js **18+**
2. Доступная MongoDB (локально или удалённо)
3. Файл `.env` в корне проекта (создать из `.env.example`)

---

### Windows / PowerShell

#### 1. Создание виртуального окружения

```powershell
python -m venv .venv
```

#### 2. Установка Python-зависимостей

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

#### 3. Сборка frontend

```powershell
cd frontend
npm install
npm run build
cd ..
```

#### 4. Конфигурация

```powershell
Copy-Item .env.example .env
# Откройте .env и заполните обязательные секреты (MONGO__URI, SECURITY__*, ROOT__*)
notepad .env
```

#### 5. Запуск

```powershell
.\.venv\Scripts\python.exe run.py
```

Приложение доступно по адресу: **http://localhost:8000**

При первом запуске автоматически создаётся root-пользователь из переменных `ROOT__USERNAME` / `ROOT__PASSWORD` / `ROOT__EMAIL`.

---

### Debian/Ubuntu (production)

Установочный скрипт автоматически:
- Ставит системные зависимости через `apt` (Python, Node.js, git)
- Копирует проект в `/opt/asfes-multiplex`
- Создаёт Python venv и собирает frontend
- Интерактивно запрашивает MongoDB URI и root-пароль
- Генерирует криптографически стойкие секреты
- Записывает конфигурацию в `/etc/asfes-multiplex/multiplex.env`
- Создаёт и активирует systemd-сервис `asfes-multiplex`

```bash
sudo bash scripts/install.sh
```

**Управление сервисом:**
```bash
sudo systemctl status asfes-multiplex
sudo systemctl restart asfes-multiplex
sudo systemctl stop asfes-multiplex

# Просмотр логов в реальном времени
sudo journalctl -u asfes-multiplex -f
```

**Обновление:**
```bash
sudo bash scripts/update.sh   # полное обновление (git pull + pip + npm build)
sudo bash scripts/restart.sh  # только перезапуск
```

> **Совет:** Для LAN/домашнего сервера рекомендуется держать сервис за VPN или закрыть порт фаерволом. Для публичного домена — настроить Nginx reverse proxy с HTTPS, задать `APP__PUBLIC_BASE_URL`, `SECURITY__COOKIE_SECURE=true`.

---

## ⚙️ Конфигурация: переменные окружения

Все настройки читаются из файла `.env` в корне проекта. Полный пример с комментариями — в [`.env.example`](.env.example).

### Обязательные настройки

| Переменная | Описание |
|------------|----------|
| `MONGO__URI` | URI подключения к MongoDB (без MongoDB backend не стартует) |
| `SECURITY__API_JWT_SECRET` | Секрет подписи API JWT-токенов (`openssl rand -hex 32`) |
| `SECURITY__OAUTH_JWT_SECRET` | Секрет подписи OAuth JWT-токенов (`openssl rand -hex 32`) |
| `SECURITY__PASSWORD_PEPPER` | Pepper для хеширования паролей |
| `ROOT__PASSWORD` | Пароль root-пользователя (≥12 символов) |
| `ROOT__EMAIL` | Email root-пользователя |

> ⚠️ Запуск с дефолтными секретами (строки `change-this-*`, `ChangeMeRootPassword123!`) в **production-режиме заблокирован** самим приложением.

---

### Основные настройки приложения (`APP__*`)

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `APP__HOST` | `0.0.0.0` | Сетевой интерфейс для прослушивания |
| `APP__PORT` | `8000` | Порт веб-сервера |
| `APP__ENV` | `production` | Режим работы (`development` / `production`) |
| `APP__PUBLIC_BASE_URL` | — | Внешний URL (для OAuth callbacks, напр. `https://multiplex.example.com`) |
| `APP__API_PREFIX` | `/api` | URL-префикс REST API |
| `APP__MCP_PATH` | `/mcp` | URL-путь MCP gateway |
| `APP__FRONTEND_DIST` | `frontend/dist` | Путь к собранному React bundle |
| `APP__TRUSTED_PROXY_IPS` | `["127.0.0.1","::1"]` | IP доверенных прокси (для X-Forwarded-For) |

---

### База данных и кэш

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `MONGO__DATABASE` | `asfes_multiplex` | Имя базы данных |
| `MONGO__CONNECT_TIMEOUT_MS` | `5000` | Таймаут подключения к MongoDB (мс) |
| `MONGO__MAX_POOL_SIZE` | `50` | Максимум соединений в пуле |
| `REDIS__MODE` | `runtime` | `disabled` / `runtime` / `required` |
| `REDIS__URL` | `redis://127.0.0.1:6379/0` | URL подключения к Redis |
| `REDIS__ENABLED_ON_STARTUP` | `false` | Подключаться ли к Redis при старте |

---

### Безопасность и токены (`SECURITY__*`)

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `SECURITY__ACCESS_TOKEN_TTL_MINUTES` | `15` | TTL access token (минуты) |
| `SECURITY__REFRESH_TOKEN_TTL_DAYS` | `30` | TTL refresh token (дни) |
| `SECURITY__OAUTH_ACCESS_TOKEN_TTL_MINUTES` | `30` | TTL OAuth access token |
| `SECURITY__COOKIE_SECURE` | `true` | Передавать куки только по HTTPS |
| `SECURITY__COOKIE_SAMESITE` | `lax` | SameSite policy (`lax` / `strict` / `none`) |
| `SECURITY__PROXY_ENCRYPTION_KEY` | — | Ключ шифрования паролей прокси |

---

### Host Operations (`HOST_OPS__*`)

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `HOST_OPS__MANAGED_FILE_ROOTS` | `["data","runtime"]` | Разрешённые корни для файловых операций |
| `HOST_OPS__MANAGED_LOG_ROOTS` | `["runtime/logs"]` | Разрешённые корни для чтения логов |
| `HOST_OPS__PROCESS_ALLOWED_EXECUTABLES` | `[]` | Whitelist разрешённых исполняемых файлов |
| `HOST_OPS__NGINX_CONFIG_PATHS` | `["data/nginx"]` | Пути к конфигам Nginx |
| `HOST_OPS__COMMAND_TIMEOUT_SECONDS` | `30` | Таймаут выполнения команд (сек) |
| `HOST_OPS__MAX_OUTPUT_BYTES` | `65536` | Лимит вывода команды (байты) |
| `HOST_OPS__ALERT_POLL_INTERVAL_SECONDS` | `60` | Интервал опроса алертов (сек) |
| `HOST_OPS__PORT_PROBE_ALLOWED_HOSTS` | `["127.0.0.1","::1","localhost"]` | Разрешённые хосты для сканирования портов |
| `HOST_OPS__VPN_PROFILES_DIRECTORY` | `data/profiles/vpn` | Хранилище VPN-профилей |
| `HOST_OPS__SSL_PROFILES_DIRECTORY` | `data/profiles/ssl` | Хранилище SSL-профилей |
| `HOST_OPS__DATABASE_PROFILES_DIRECTORY` | `data/profiles/databases` | Хранилище профилей БД |

---

### PyPI-зеркало (`PYPI__*`)

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `PYPI__ENABLED` | `true` | Включить PyPI-зеркало |
| `PYPI__DATA_DIR` | `data/pypi_storage` | Директория хранилища пакетов |
| `PYPI__ON_DEMAND_PROXY` | `true` | Прозрачное кэширование с PyPI.org |
| `PYPI__PUBLIC_ACCESS` | `true` | Simple API без авторизации |
| `PYPI__PARALLEL` | `5` | Параллельных соединений при скачивании |
| `PYPI__MIN_SAFE_SPACE_GB` | `3.0` | Минимум свободного места (ГБ) |
| `PYPI__NETWORK_MODE` | `direct` | `direct` / `proxy` / `mix` |
| `PYPI__RATE_LIMIT_MB` | — | Лимит скорости скачивания (МБ/с) |

---

### OAuth / PKCE (`OAUTH__*`)

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `OAUTH__REQUIRE_PKCE` | `true` | Обязательный PKCE для всех OAuth-запросов |
| `OAUTH__ALLOW_PLAIN_PKCE` | `false` | Разрешить plain-метод в PKCE (не рекомендуется) |
| `OAUTH__SUPPORTED_SCOPES` | `["mcp","profile"]` | Поддерживаемые scope |

---

### Rate Limiting (`RATE_LIMITS__*`)

| Лимит | Default | Окно |
|-------|---------|------|
| Login | 5 попыток | 60 сек |
| Register | 3 попытки | 3600 сек |
| OAuth Token | 20 запросов | 60 сек |
| REST Read | 60 запросов | 60 сек |
| REST Write | 20 запросов | 60 сек |
| MCP Read | 30 запросов | 60 сек |
| MCP Write | 10 запросов | 300 сек |

---

### SMTP (`SMTP__*`) — опционально

```env
SMTP__ENABLED=true
SMTP__HOST=smtp.example.com
SMTP__PORT=587
SMTP__USERNAME=mailer@example.com
SMTP__PASSWORD=your-password
SMTP__FROM_EMAIL=no-reply@example.com
SMTP__STARTTLS=true
```

---

## 🛡 Безопасность

### Модель безопасности первого запуска

1. **Регистрация** пользователей **выключена** по умолчанию — включается через Admin UI → Runtime Settings
2. **Write-tools** MCP **глобально выключены** при первом создании политик — включаются точечно в UI (Tools → Permissions)
3. Root-пользователь создаётся только через переменные `ROOT__*` (не через UI)
4. В **production-режиме** (`APP__ENV=production`) приложение **блокирует старт** при обнаружении дефолтных секретов

### Рекомендации

- Для домашнего сервера: держите за **VPN или закройте порт фаерволом**
- Для публичного домена: **Nginx reverse proxy + HTTPS**, задайте `APP__PUBLIC_BASE_URL`, `SECURITY__COOKIE_SECURE=true`
- Runtime-файлы (`runtime/logs/`, `runtime/multiplex_logs.db`) исключены из git
- Никогда не коммитьте `.env` с реальными секретами

---

## 🧪 Тесты и линтинг

### Backend

```powershell
# Линтер и автофикс (Ruff)
.\.venv\Scripts\python.exe -m ruff check . --fix

# Тесты
.\.venv\Scripts\python.exe -m pytest tests

# Тесты с покрытием (порог ≥ 80%)
.\.venv\Scripts\python.exe -m pytest --cov=server --cov-report=term-missing
```

### Frontend

```powershell
cd frontend

# Линтер (ESLint)
npm run lint

# Сборка (проверяет TypeScript и bundling)
npm run build

cd ..
```

> **Правило:** После любых изменений Python-кода — `ruff check --fix`, затем `pytest`. После изменений frontend — `npm run lint`, затем `npm run build`.

---

## 🗺 Таблица маршрутизации

| Метод | Путь | Описание | Auth |
|-------|------|----------|------|
| `GET` | `/api/health` | Health check | — |
| `POST` | `/api/auth/login` | Вход (cookie + Bearer) | — |
| `POST` | `/api/auth/logout` | Выход | ✅ |
| `POST` | `/api/auth/refresh` | Обновление токена | ✅ |
| `POST` | `/api/auth/register` | Регистрация (если включена) | — |
| `GET/POST` | `/api/auth/2fa/*` | Управление TOTP | ✅ |
| `GET/POST` | `/api/auth/passkeys/*` | WebAuthn / Passkeys | ✅ |
| `GET/POST` | `/api/admin/*` | Управление системой | ✅ root |
| `GET/POST` | `/api/oauth/*` | OAuth 2.0 / PKCE | — |
| `GET` | `/.well-known/oauth-authorization-server` | OAuth discovery | — |
| `GET` | `/api/oauth/jwks` | JWKS public keys | — |
| `GET/POST` | `/api/pypi/*` | Управление PyPI-зеркалом | ✅ |
| `GET` | `/pypi/simple/<pkg>/` | pip Simple API | optional |
| `GET/POST` | `/api/proxy/*` | Управление прокси | ✅ |
| `GET/POST` | `/api/python-mirror/*` | Управление Python-зеркалом | ✅ |
| `GET/POST/DELETE` | `/api/ha/*` | Интеграция Home Assistant | ✅ HA Bearer / API JWT |
| `GET` | `/api/metrics` | Метрики Prometheus | optional (system.metrics.read) |
| `*` | `/mcp/*` | MCP Gateway (FastMCP) | OAuth |
| `GET` | `/*` | React SPA (catch-all) | — |


---

## 🔧 Технологический стек

| Слой | Технология |
|------|-----------|
| **Backend** | Python 3.11+, FastAPI 0.115+, Uvicorn |
| **MCP Gateway** | FastMCP 2.x |
| **Frontend** | React, TypeScript, Vite |
| **База данных** | MongoDB (Motor — async driver) |
| **Кэш / Rate limit** | Redis (опционально) |
| **Аутентификация** | PyJWT, python-webauthn |
| **Линтер** | Ruff |
| **Тесты** | pytest, pytest-asyncio, pytest-mock |
| **Логирование** | Rich, SQLite (integrity logs) |
| **HTTP-клиент** | httpx, aiohttp |
| **Процессы** | psutil |
| **Шифрование** | cryptography |
| **Production** | Debian/Ubuntu, systemd |
| **Dev** | Windows 11, PowerShell |

---

## 📄 Лицензия

Проект распространяется под лицензией [MIT](LICENSE).

---

<div align="center">

Сделано с ❤️ для домашнего сервера

</div>
