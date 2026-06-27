# AGENTS.md — ASFES Multiplex Context

Этот файл является корневым системным контекстом (System Prompt / Context Guardrail) для всех AI-агентов и ассистентов, работающих в репозитории проекта **ASFES Multiplex**: Antigravity CLI, Claude Code, Cursor, Codex и любых других инструментов, поддерживающих загрузку `AGENTS.md` / `CLAUDE.md`.

> Файл находится в **корне репозитория** (`/AGENTS.md`) — это единственное канонические расположение. Большинство AI-инструментов сканируют только корень проекта.

Требования этого файла **критичны для корректного выполнения задач**. Их нарушение может привести к повреждению данных, утечке секретов, неработоспособному состоянию приложения или проблемам безопасности. Агент обязан следовать всем правилам ниже без исключений.

---

## Project & Architecture Overview

### Суть проекта

**ASFES Multiplex** — домашний FastAPI + MCP (Model Context Protocol) control plane с React-админкой. Предоставляет централизованный API и MCP-шлюз для управления локальным сервером: Docker, файлы, логи, процессы, firewall, scheduler, SSL, VPN, Nginx и системная статистика.

### Архитектурный паттерн: однопортовое проксирование

В production backend, MCP-шлюз и React SPA работают **на одном HTTP-порту** (по умолчанию `0.0.0.0:8000`). Маршрутизация в `server/app.py`:

| Префикс              | Назначение                                              |
|----------------------|---------------------------------------------------------|
| `/api/*`             | FastAPI REST API (auth, admin, health, oauth)           |
| `/mcp/*`             | FastMCP gateway (MCP-протокол, SSE, OAuth/PKCE)         |
| `/.well-known/*`     | OAuth discovery, JWKS                                   |
| `/assets/*`          | Статические файлы React-сборки (`frontend/dist/assets`) |
| `/*` (всё остальное) | React SPA (`frontend/dist/index.html`) — catch-all      |

Попытки обратиться к `/api`, `/mcp` или `/.well-known` через catch-all маршрут возвращают **404** — это намеренное поведение.

### Технологический стек

- **Backend**: Python, FastAPI, FastMCP, Pydantic Settings, Motor (async MongoDB)
- **Аутентификация**: JWT (HttpOnly cookie + Bearer), CSRF-заголовок для браузерных write-запросов, OAuth 2.0 / PKCE для MCP-клиентов
- **Frontend**: React, TypeScript, Vite (сборка в `frontend/dist/`)
- **Хранилища**: MongoDB (обязательна — без неё backend не стартует), Redis (опционально, runtime rate limit), SQLite + файлы (integrity-логи)
- **Окружение разработки**: Windows 11, PowerShell, виртуальное окружение `.venv`
- **Production**: Debian/Ubuntu, `/opt/asfes-multiplex`, systemd service `asfes-multiplex`

### Ключевые модули

- `server/app.py` — фабрика FastAPI приложения, монтирование маршрутов, lifespan
- `server/core/config.py` — настройки через Pydantic Settings (читает `.env`)
- `server/services.py` — инициализация MongoDB, плагинов, политик, сервисов
- `server/mcp/` — FastMCP gateway, регистрация tools, OAuth scope
- `server/routes/` — маршруты: `auth.py`, `admin.py`, `oauth.py`, `health.py`
- `server/host_ops.py` — host operations tools (файлы, процессы, Docker, Nginx и т.д.)
- `frontend/src/` — React TypeScript админ-панель
- `scripts/install.sh` — установочный скрипт для Debian/Ubuntu
- `tests/` — pytest-тесты (unit + integration)

---

## Local Development & Operations

### Требования перед запуском

1. Доступная MongoDB по адресу из `MONGO__URI`. **Если MongoDB недоступна — backend не стартует.** Это не баг, а намеренная защита.
2. Файл `.env` в корне проекта (скопировать из `.env.example` и заполнить реальными секретами).
3. Python-окружение `.venv` с установленными зависимостями.
4. Собранный React-бандл (`frontend/dist/`), иначе все не-API пути вернут 404.

### Локальная установка зависимостей (Windows / PowerShell)

```powershell
# 1. Python-зависимости
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

# 2. Frontend-зависимости и сборка
cd frontend
npm install
npm run build
cd ..
```

### Запуск приложения (Windows / PowerShell)

```powershell
.\.venv\Scripts\python.exe run.py
```

Приложение поднимается на `http://0.0.0.0:8000`. Порт и хост задаются в `.env` (`APP__HOST`, `APP__PORT`).

### Команды валидации (обязательны после правок)

```powershell
# Backend: линтер и автофикс стиля (Ruff)
.\.venv\Scripts\python.exe -m ruff check . --fix

# Backend: запуск тестов
.\.venv\Scripts\python.exe -m pytest tests

# Frontend: линтер (ESLint)
cd frontend
npm run lint

# Frontend: сборка (проверяет TypeScript и bundling)
npm run build
cd ..
```

**Порядок выполнения при изменении Python-кода:** сначала `ruff check --fix`, затем `pytest`.
**Порядок выполнения при изменении frontend-кода:** сначала `npm run lint`, затем `npm run build`.

### Production (Debian/Ubuntu)

```bash
# Первичная установка
sudo bash scripts/install.sh

# Управление сервисом
sudo systemctl status asfes-multiplex
sudo systemctl restart asfes-multiplex
sudo systemctl stop asfes-multiplex

# Логи в реальном времени
sudo journalctl -u asfes-multiplex -f

# Обновление
sudo bash scripts/update.sh
sudo bash scripts/restart.sh
```

Конфигурация production-окружения: `/etc/asfes-multiplex/multiplex.env`
Рабочая директория: `/opt/asfes-multiplex`

---

## Core Agent Behavior Rules

> **Это обязательные правила. Агент обязан соблюдать их в полном объёме при каждой задаче.**

### Язык общения

- **Все** ответы пользователю, планы, промежуточные отчёты, объяснения и итоговые сообщения писать **исключительно на русском языке**.
- Исключения: команды терминала, примеры кода, commit message, конфигурационные файлы — на английском.

### Проверка после правок

- После **любых** изменений кода запускать релевантную проверку:
  - Изменён Python-код → `ruff check . --fix`, затем `pytest tests`.
  - Изменён TypeScript/React-код → `npm run lint`, затем `npm run build`.
  - Изменены оба слоя → все четыре команды.
- Если выполнить проверку **невозможно** (нет MongoDB, нет Node.js и т.д.) — **явно сообщить об этом** пользователю с указанием причины и ожидаемого результата.

### Frontend-сборка

- При завершении **любых** работ, затрагивающих `frontend/` (`.tsx`, `.ts`, `.css`, `package.json`, `vite.config.ts`), **обязательно** запускать `npm run lint && npm run build` из директории `frontend/`.
- Без актуального бандла в `frontend/dist/` React SPA не работает — сервер отдаёт 404 на все не-API маршруты.

### Документация

- При изменениях в: установке, запуске, API-структуре, env-переменных, сценариях проверки, архитектуре — **проверять и обновлять `README.md`**.
- Не оставлять README устаревшим относительно реального поведения проекта.

### Файловая система — строгий запрет удаления

- **Запрещено** удалять файлы и папки **вне** директории проекта при любых обстоятельствах.
- **Запрещено** окончательно удалять файлы внутри проекта **без явного разрешения пользователя**.
- Если файл нужно убрать — **переместить в архивную папку** (например, `archive/` или `_deprecated/`), если это безопасно и не нарушает работу проекта.

### Деструктивные команды — абсолютный запрет без разрешения

Следующие команды **запрещено выполнять без явного разрешения** пользователя в каждом конкретном случае:

```
rm -rf <anything>
git clean -fd
git clean -fxd
git reset --hard
git push --force
git push --force-with-lease
git rebase --onto (без обсуждения)
```

Также запрещены: массовые удаления файлов через скрипты, сброс истории коммитов, затирание незакоммиченных изменений пользователя.

### Секреты и переменные окружения

- **Никогда** не хардкодить значения `SECURITY__*` (`API_JWT_SECRET`, `OAUTH_JWT_SECRET`, `PASSWORD_PEPPER`), пароли, токены и ключи в коде или конфигурационных файлах.
- **Не коммитить** файл `.env`, runtime-файлы, integrity-логи (`runtime/logs/`, `runtime/multiplex_logs.db`), локальные данные из `runtime/`.
- В примерах и тестах использовать только заглушки (placeholder values), явно обозначенные как тестовые.

### Commit message — только английский, GPG

- Commit messages писать **строго на английском языке**.
- Формат: подробный, содержательный, предпочтительно **Conventional Commits** (`feat:`, `fix:`, `refactor:`, `chore:`, `docs:`, `test:`, `security:` и т.д.).
- Каждый коммит **должен быть подписан GPG-ключом** (`git commit -S ...`).
- **⚠️ Зависание GPG в неинтерактивном режиме:** Если команда `git commit -S` зависает или падает из-за ожидания ввода passphrase (отсутствие TTY / pinentry в среде CLI-агента) — **немедленно прервать процесс** (`Ctrl+C` / kill), не пытаться обойти подпись флагом `--no-gpg-sign` или другими способами, и **попросить пользователя сделать коммит вручную**.
- **Запрещено** выполнять `git push` без явного разрешения пользователя.

### Инфраструктурные файлы

- **Запрещено** изменять или удалять без явного обсуждения: `scripts/install.sh`, `scripts/update.sh`, `scripts/restart.sh`, конфигурации systemd.
- Эти файлы управляют production-развёртыванием — их некорректное изменение может сломать установленную систему.

---

## Git & Commit Conventions

### Формат коммита

```
<type>(<scope>): <short summary in imperative mood>

<body: detailed description, what changed and why>

<footer: breaking changes, closes issues>
```

**Типы (type):**
- `feat` — новая функциональность
- `fix` — исправление бага
- `refactor` — рефакторинг без изменения поведения
- `chore` — обновление зависимостей, конфигурации, tooling
- `docs` — изменения только в документации
- `test` — добавление или исправление тестов
- `security` — исправления безопасности
- `perf` — улучшение производительности

**Области (scope):** `backend`, `frontend`, `mcp`, `auth`, `host-ops`, `config`, `tests`, `scripts`, `docker`, `nginx`

### Пример корректного коммита

```
feat(auth): add CSRF validation middleware for write endpoints

Introduces CSRF token validation for all state-mutating API routes
(POST, PUT, PATCH, DELETE) using the Double Submit Cookie pattern.

Token is set via HttpOnly cookie and must be echoed in the
X-CSRF-Token request header. Exempt endpoints: /api/oauth/token,
/api/auth/login (pre-auth).

Closes #42
```

### Правила

- **Первая строка** (summary): не более 72 символов, глагол в инфинитиве (`add`, `fix`, `remove`), без точки в конце.
- **Тело** (body): объяснить «что» и «почему», разделить от summary пустой строкой.
- GPG-подпись обязательна: `git commit -S -m "..."` или настроить `commit.gpgSign = true` в git config.
- **⚠️ Если `git commit -S` зависает** (нет TTY, pinentry не запускается в среде агента): немедленно прервать команду, **не снимать флаг `-S`** и попросить пользователя выполнить коммит вручную в своём терминале.
- `git push` **только с явного разрешения** пользователя.

---

## Security, Secrets & Production Environment

### Переменные окружения — что обязательно и что запрещено

**Обязательные production-секреты (никогда не хардкодить, не коммитить):**

| Переменная                    | Назначение                                  |
|-------------------------------|---------------------------------------------|
| `MONGO__URI`                  | URI подключения к MongoDB                   |
| `SECURITY__API_JWT_SECRET`    | Секрет подписи API JWT-токенов              |
| `SECURITY__OAUTH_JWT_SECRET`  | Секрет подписи OAuth JWT-токенов            |
| `SECURITY__PASSWORD_PEPPER`   | Pepper для хеширования паролей              |
| `ROOT__PASSWORD`              | Пароль root-пользователя                    |
| `ROOT__EMAIL`                 | Email root-пользователя                     |

**Запрещено:**
- Коммитить `.env` (он в `.gitignore` — не обходить это).
- Коммитить файлы из `runtime/` (логи, SQLite-база, бэкапы).
- Использовать дефолтные значения из `.env.example` (`change-this-api-secret`, `ChangeMeRootPassword123!`) в production — приложение само блокирует старт с ними.
- Логировать значения секретов в консоль или в audit-лог.

### Безопасность первого запуска

- **Регистрация новых пользователей выключена** по умолчанию. Первый root создаётся через `ROOT__*` переменные.
- **Write-tools глобально выключены** при первом создании политик. Включать точечно через UI (MCP Tools → Permissions).
- Browser UI использует HttpOnly cookies + CSRF-заголовок `X-CSRF-Token` для всех write-запросов.
- Bearer API (Authorization: Bearer) совместим для скриптов и MCP/OAuth flow.
- Для LAN/домашнего использования: держать за VPN или закрыть порт firewall. Для публичного домена: включить HTTPS reverse proxy, задать `APP__PUBLIC_BASE_URL`, `SECURITY__COOKIE_SECURE=true`.

### Работа с host_ops tools

- `HOST_OPS__MANAGED_FILE_ROOTS` и `HOST_OPS__MANAGED_LOG_ROOTS` — **строгие границы** для file/log операций. Агент не должен пытаться изменить или обойти эти пути.
- `HOST_OPS__PROCESS_ALLOWED_EXECUTABLES` — список разрешённых исполняемых файлов для process tools. Расширять только с явного согласования.
- Операции с Docker, Nginx, firewall, VPN через MCP-tools затрагивают production-инфраструктуру — перед выполнением всегда уточнять намерение у пользователя.

---

## Common Pitfalls & Anti-Patterns for AI

Список типичных ошибок AI-агентов в этом проекте. Агент обязан их избегать.

### Забыть собрать фронтенд

**Проблема:** Агент правит `*.tsx` / `*.ts` / `*.css` в `frontend/src/`, считает задачу выполненной, не запускает `npm run build`.
**Результат:** Изменения в браузере не появляются. Сервер раздаёт старый `index.html` из `frontend/dist/`.
**Правило:** `cd frontend && npm run lint && npm run build` — **обязательный финальный шаг** после любых правок фронтенда.

### Запуск без MongoDB

**Проблема:** Агент пытается стартовать приложение (`python run.py`) в окружении без доступной MongoDB.
**Результат:** Backend падает при старте в `build_application_services()` — `ServerSelectionTimeoutError`. Приложение **не стартует частично**, оно не стартует вообще.
**Правило:** Перед запуском убедиться, что MongoDB доступна по `MONGO__URI` из `.env`. Если нет — явно сообщить пользователю.

### Коммит дефолтных секретов

**Проблема:** Агент копирует `.env.example` в `.env`, не меняет значения, коммитит файл или использует в тестах строки `change-this-api-secret`, `ChangeMeRootPassword123!`.
**Результат:** Утечка секретов, блокировка старта в production (приложение само отказывает при дефолтных значениях).
**Правило:** `.env` в `.gitignore`. Дефолтные значения из `.env.example` — **только для примера**, не для использования.

### Удаление инфраструктурных скриптов

**Проблема:** Агент решает «почистить» репозиторий и удаляет `scripts/install.sh`, `scripts/update.sh`, `scripts/restart.sh`.
**Результат:** Невозможность установки или обновления production-окружения на Debian/Ubuntu.
**Правило:** Файлы в `scripts/` — production-критичные. Изменять только с явного обсуждения. Не удалять никогда.

### Прямое изменение конфигурации systemd без обсуждения

**Проблема:** Агент через MCP host_ops tools редактирует systemd unit-файл, перезапускает сервис без предупреждения.
**Результат:** Возможная недоступность сервиса, потеря данных, прерывание работы.
**Правило:** Любые операции, затрагивающие systemd, производственные конфиги и живой сервис — **только с явного разрешения пользователя**.

### Смешивание языков в ответах

**Проблема:** Агент пишет объяснения или планы частично на английском, частично на русском.
**Правило:** Ответы пользователю — **только русский**. Код, команды, commit messages — английский.

### Игнорирование CSRF при тестировании API

**Проблема:** Агент пишет тесты для браузерных write-эндпоинтов без передачи CSRF-заголовка `X-CSRF-Token`.
**Результат:** Тесты падают с 403 Forbidden, хотя код правильный.
**Правило:** Браузерные write-запросы (`POST`, `PUT`, `PATCH`, `DELETE` на `/api/*`) требуют: HttpOnly session cookie + заголовок `X-CSRF-Token`. Bearer-токен через `Authorization: Bearer` CSRF не требует.

### Предположение о наличии Redis

**Проблема:** Агент пишет код, явно зависящий от Redis, считая его всегда доступным.
**Результат:** Падение при `REDIS__ENABLED_ON_STARTUP=false` (режим по умолчанию).
**Правило:** Redis **опционален**. При написании или правке бизнес-логики агент обязан:
- Проверять `settings.redis.mode` или `REDIS__ENABLED_ON_STARTUP` перед любым обращением к Redis.
- **Не вызывать** методы кэширования (`cache.set`, `cache.get`) или rate limit напрямую, если Redis отключён в конфигурации — такой вызов вызовет исключение в runtime.
- Оборачивать Redis-зависимый код в проверку доступности или использовать существующие абстракции проекта, которые учитывают опциональность Redis.

### Зависание GPG при коммите

**Проблема:** Агент выполняет `git commit -S`, команда зависает на ожидании passphrase — pinentry не может открыться без TTY в неинтерактивной среде CLI-агента.
**Результат:** Процесс коммита навсегда подвисает или падает с ошибкой `gpg: signing failed`.
**Правило:** При любом зависании `git commit -S` — **немедленно прервать процесс** и передать управление пользователю. Не пытаться обойти GPG-подпись через `--no-gpg-sign`, `--no-verify` или изменение git config. Попросить пользователя выполнить коммит вручную в своём терминале, где pinentry работает корректно.

### Push без разрешения

**Проблема:** Агент выполняет `git push` после коммита, считая это логическим продолжением задачи.
**Правило:** `git push` — **строго запрещён** без явного разрешения пользователя в текущей сессии.

### Требования к покрытию тестами и CI

- **Запуск тестов с покрытием:** После изменений в Python-коде запускать `pytest --cov=server --cov-report=term-missing`.
- **Минимальный порог покрытия:** Запрещено коммитить изменения, снижающие покрытие ниже лимита `fail_under`, настроенного в `pyproject.toml`.
- **Принцип постепенного повышения:** При выполнении задач на покрытие лимит `fail_under` повышается поэтапно (50% -> 60% -> 70% -> 80%). Не повышать лимит авансом, чтобы не блокировать горячие исправления.
- **Стратегия тестирования:**
  - Предпочитать чистые unit-тесты (без MongoDB) для изолированной логики.
  - REST-маршруты и планировщик (scheduler) тестировать интеграционно через `integration_env` (реальная тестовая база MongoDB авто-удаляется после теста).
  - Для плагинов избегать "over-mocking" системных утилит; делать легкие smoke-тесты или использовать реальную файловую систему / сокеты, проверяя graceful error handling при отсутствии утилит.

---

*Этот файл находится в корне репозитория (`/AGENTS.md`) и автоматически загружается Antigravity CLI, Claude Code, Cursor и другими AI-инструментами, поддерживающими `AGENTS.md`. Обновлять при изменении архитектуры, стека или правил безопасности проекта.*
