# Аудит безопасности ASFES Multiplex

Дата аудита: 2026-04-30  
Область проверки: backend FastAPI, MCP gateway и плагины, frontend React/Vite, install/runtime конфигурация, зависимости Node/Python.

## Краткое резюме

Проект уже содержит важные защитные механизмы: HttpOnly cookie-сессии, CSRF для cookie-auth write-запросов, PKCE для OAuth, ротацию refresh-токенов, scrypt-хеширование паролей с pepper, TOTP/passkey, разграничение MCP tools по правам, managed roots для файловых операций и блокировку production-запуска с дефолтными секретами.

Основные риски связаны не с классическими SQL/XSS, а с природой приложения как control plane: OAuth dynamic registration, локальные MCP-инструменты управления процессами/Docker/HTTP probe, доверие к forwarded-заголовкам, небезопасные production-дефолты cookie/HTTP и недостаточная проверка Python-зависимостей.

## Методика

- Ручной просмотр маршрутов, сервисов, моделей, MCP gateway и плагинов.
- Поиск опасных паттернов: `subprocess`, `exec/eval`, `shell=True`, `read_text/write_text`, `redirect_uri`, cookies, headers, secrets.
- Проверка наличия секретов в git: `.env` не отслеживается, `.env.example` отслеживается.
- Проверка frontend supply chain: `npm audit --audit-level=low --json`.
- Проверка Python supply chain: `pip-audit` в окружении не установлен, поэтому CVE-аудит Python-зависимостей не завершен.

## Итог по уровням

| Уровень | Количество |
|---|---:|
| Критический | 0 |
| Высокий | 4 |
| Средний | 7 |
| Низкий | 5 |
| Информационный | 0 |

## Найденные уязвимости и риски

### SEC-01. OAuth Dynamic Client Registration открыт без аутентификации

Уровень: высокий  
Файлы: `server/routes/oauth.py:260-304`, `server/services.py:891-909`

`POST /api/oauth/register` позволяет любому внешнему клиенту создать public OAuth client с произвольными `redirect_uris` и разрешенными scope из поддерживаемого набора. Сейчас это не дает доступ без логина пользователя, но расширяет поверхность phishing/consent attacks: злоумышленник может зарегистрировать легитимно выглядящий клиент, завести пользователя на authorize flow и получить OAuth token после ввода учетных данных.

Доказательство:
- Регистрация клиента не требует `Depends(require_permission(...))`: `server/routes/oauth.py:260`.
- Созданный клиент сохраняет переданные `redirect_uris` без нормализации и ограничений доверенных доменов: `server/services.py:891-900`.

Рекомендации:
- Отключить dynamic registration по умолчанию.
- Либо требовать админское право `oauth.clients.manage`.
- Либо добавить allowlist redirect-доменов/схем, запрет wildcard/localhost в production и отдельный runtime toggle.
- Добавить rate limit именно на `/oauth/register`.

### SEC-02. Публичные OAuth-клиенты не ограничены client secret, confidential-флаг фактически не используется при token exchange

Уровень: высокий  
Файлы: `server/services.py:891-909`, `server/services.py:954-1021`, `server/routes/oauth.py:183-220`

Сервис умеет создавать `confidential=True` клиентов и хранит `client_secret_hash`, но `/token` не принимает и не проверяет `client_secret`. Любой, кто получил authorization code и PKCE verifier public/confidential клиента, может обменять код без дополнительной проверки секрета. Для public clients это ожидаемо, для confidential clients - нарушение модели OAuth.

Рекомендации:
- В `/oauth/token` принимать `client_secret` или HTTP Basic auth для confidential clients.
- В `exchange_code` проверять `client["confidential"]` и `client_secret_hash`.
- В metadata показывать разные `token_endpoint_auth_methods_supported`.
- Для dynamic registration оставлять только public clients, а админские confidential clients валидировать строго.

### SEC-03. `OAUTH__ALLOW_PLAIN_PKCE=false` не запрещает `plain` PKCE фактически

Уровень: высокий  
Файлы: `server/core/security.py:100-106`, `server/routes/oauth.py:78-82`, `server/services.py:927-940`, `server/services.py:971`

Конфиг объявляет `allow_plain_pkce: false`, а metadata показывает `plain` только при включенном флаге. Но сервер принимает `code_challenge_method` из запроса и затем `verify_pkce` всегда поддерживает `PLAIN`. В итоге клиент может отправить `code_challenge_method=plain`, и обмен кода пройдет, если verifier равен challenge.

Рекомендации:
- На `/authorize` отклонять все методы кроме `S256`, если `allow_plain_pkce=false`.
- Нормализовать значение метода к `S256`/`plain` и хранить только разрешенные значения.
- Добавить тест на отказ `plain` при дефолтной конфигурации.

### SEC-04. MCP `process_manager.start_process` дает удаленный запуск произвольной команды

Уровень: высокий  
Файлы: `server/mcp/plugins/process_manager.py:45-53`

Инструмент принимает массив `command` и вызывает `asyncio.create_subprocess_exec(*command)`. Shell injection нет, но это намеренный arbitrary command execution от имени процесса сервиса. При компрометации OAuth-токена пользователя с `process.write` или ошибочной выдаче права атакующий получает выполнение команд на хосте.

Рекомендации:
- Оставить инструмент выключенным глобально по умолчанию.
- Ввести allowlist команд или профили команд вместо произвольного массива.
- Разделить права на `process.start.allowed` и `process.kill`.
- Запускать сервис под максимально ограниченным пользователем, без Docker/sudo-групп.
- Логировать хеш и полный argv, но редактировать секретные аргументы.

### SEC-05. Доверие к `X-Forwarded-For` позволяет обходить IP-based rate limit и портить аудит

Уровень: средний  
Файлы: `server/services.py:79-87`, `run.py:68-82`

`request_meta_from_request` берет первый IP из `x-forwarded-for` без проверки доверенного proxy. Uvicorn настроен с `forwarded_allow_ips=127.0.0.1`, но функция вручную читает заголовок из запроса. Если приложение доступно напрямую, клиент может подставить любой IP и обходить login/register/oauth rate limit, а также искажать audit trail.

Рекомендации:
- Использовать `request.client.host` после обработки trusted proxy uvicorn/starlette.
- Либо принимать `X-Forwarded-For` только если `request.client.host` входит в allowlist reverse proxy.
- Добавить тест: прямой запрос с поддельным `X-Forwarded-For` не должен менять rate-limit key.

### SEC-06. Production install оставляет cookie без `Secure`

Уровень: средний  
Файлы: `server/core/config.py:144-145`, `server/routes/auth.py:31-56`, `scripts/install.sh:221-232`

Cookie `multiplex_session`, refresh cookie и CSRF cookie выставляются с `secure=False` по умолчанию. Установщик для production также пишет `SECURITY__COOKIE_SECURE=false`. При использовании публичного домена или случайном HTTP-доступе session/refresh cookies могут утечь по незашифрованному каналу.

Рекомендации:
- В production по умолчанию ставить `SECURITY__COOKIE_SECURE=true`.
- Если выбран `PUBLIC_BASE_URL=https://...`, установщик должен автоматически включать Secure.
- Для LAN-only сценария явно документировать исключение.

### SEC-07. Публичный health endpoint раскрывает внутреннее состояние MongoDB/Redis/MCP

Уровень: низкий  
Файл: `server/routes/health.py:10-22`

`/api/health` без аутентификации сообщает состояние MongoDB, Redis и MCP. Это удобно для мониторинга, но помогает внешнему атакующему fingerprint-ить сервис и выбирать время атаки.

Рекомендации:
- Оставить публично только `status`.
- Подробности (`mongodb`, `redis`, `mcp_enabled`) отдавать только аутентифицированным администраторам или внутреннему monitor token.

### SEC-08. MCP HTTP probe может использоваться как SSRF/внутренний сканер

Уровень: средний  
Файл: `server/mcp/plugins/ports_scanner.py:33-61`

`probe_tcp` и `probe_http` принимают произвольный host/url. Для пользователя с `ports.read` это превращает сервер в SSRF-прокси для доступа к внутренним адресам, cloud metadata endpoints, localhost-сервисам и административным панелям.

Рекомендации:
- Добавить allowlist сетей или запрет link-local/private ranges в production.
- Блокировать `169.254.169.254`, loopback, Unix socket-like схемы, нестандартные схемы.
- Возвращать минимальные сведения о headers, так как они могут содержать внутренние данные.

### SEC-09. Docker read tools глобально включаются по умолчанию и могут раскрывать секреты контейнеров

Уровень: средний  
Файл: `server/mcp/plugins/docker.py:141-203`

`docker.inspect_container`, `docker.container_logs`, `docker.container_stats` имеют `default_global_enabled=True`. Даже read-only Docker inspect/logs часто раскрывают env-переменные, mounted paths, image names, command line и токены из логов. Для root-пользователя доступ всегда разрешен, для остальных нужен набор прав, но глобальное включение повышает риск ошибочной выдачи.

Рекомендации:
- Снять `default_global_enabled=True` с `inspect_container` и `container_logs`.
- Редактировать env/secrets в output Docker inspect.
- Ограничить `tail_lines` жестким максимумом.
- Разделить `docker.containers.read` на `list`, `logs`, `inspect`.

### SEC-10. Docker write tools также имеют `default_global_enabled=True`

Уровень: средний  
Файл: `server/mcp/plugins/docker.py:204-260`

Start/stop/restart контейнеров - операционные действия с impact на доступность. Сейчас эти tools помечены `default_global_enabled=True`, что увеличивает шанс случайной доступности после назначения пользователю соответствующих прав.

Рекомендации:
- Для всех Docker write tools поставить `default_global_enabled=False`.
- В UI показывать отдельное подтверждение для destructive actions.
- Добавить audit metadata с container id/name и результатом.

### SEC-11. Установщик включает `NoNewPrivileges=false` и sudoers для update/restart

Уровень: средний  
Файлы: `scripts/install.sh:267-285`, `scripts/install.sh:247-252`

Сервис запускается с `NoNewPrivileges=false`, а пользователь сервиса получает `NOPASSWD` на `update.sh` и `restart.sh`. Это может быть оправдано для self-update, но увеличивает последствия RCE в приложении: атакующий сможет искать путь к root через разрешенные скрипты и окружение.

Рекомендации:
- По возможности включить `NoNewPrivileges=true`.
- Перенести update/restart в отдельный root-owned helper с жесткой проверкой argv и checksum.
- Убедиться, что `scripts/update.sh` и `scripts/restart.sh` root-owned и не writable для app user.
- Рассмотреть отдельный systemd policykit/sudoers rule только на `systemctl restart asfes-multiplex`.

### SEC-12. Atomic write не защищен от symlink race внутри managed roots

Уровень: средний  
Файл: `server/host_ops.py:226-245`, `server/host_ops.py:338-370`

`resolve_managed_path` проверяет путь через `resolve(strict=False)`, затем операции записи выполняются позже. Если app user имеет возможность создавать symlink внутри managed root, возможны TOCTOU-сценарии, особенно если managed root указывает на writable каталог. На Windows/Junctions и Linux symlinks это требует отдельной проверки.

Рекомендации:
- Для write/delete/move отклонять symlink components или использовать `open` с `O_NOFOLLOW` там, где доступно.
- Перед заменой файла повторно проверять parent и target.
- Разделить managed roots для чтения и записи.

### SEC-13. `/oauth/revoke` не требует client authentication

Уровень: низкий  
Файл: `server/routes/oauth.py:225-228`

Endpoint удаляет refresh token по значению токена без проверки client_id/client_secret. Если токен утек, его и так можно использовать, но отсутствие client binding на revoke упрощает неавторизованную инвалидацию токенов.

Рекомендации:
- Принимать `client_id` и проверять, что token принадлежит client.
- Для confidential clients проверять secret.
- Сохранять совместимость с public clients только при корректном `client_id`.

### SEC-14. 2FA disable разрешает recovery code без consume

Уровень: низкий  
Файл: `server/services.py:618-625`

Для отключения 2FA `verify_second_factor(..., consume_recovery=False)`. Если recovery code скомпрометирован, его можно многократно использовать для отключения 2FA, пока пользователь не сбросит набор кодов.

Рекомендации:
- Для любых операций с recovery code использовать consume-on-success.
- Логировать тип второго фактора: TOTP или recovery.
- Требовать текущий пароль для отключения 2FA.

### SEC-15. Парольная политика проверяет только длину

Уровень: низкий  
Файлы: `server/models.py:47-53`, `server/services.py:299-322`, `server/core/config.py:103-107`

Для пользовательской регистрации и root-пароля есть только минимальная длина root-пароля, а для обычных пользователей модель не задает минимальную длину/сложность. При включении регистрации пользователи могут выбрать слабые пароли.

Рекомендации:
- Ввести минимальную длину 12+ для всех пользователей.
- Проверять denylist распространенных паролей.
- Добавить rate limit по username + IP уже есть, но слабые пароли остаются проблемой.

### SEC-16. JWT реализован вручную и поддерживает только HS256 без проверки `alg` из header

Уровень: низкий  
Файл: `server/core/security.py:117-152`

Текущая реализация фиксированно подписывает/проверяет HMAC SHA-256 и не доверяет `alg`, поэтому классическая `alg=none` атака не проходит. Но ручная JWT-реализация повышает риск ошибок расширения: нет `kid`, нет leeway, нет явной проверки `nbf`, нет валидации структуры header.

Рекомендации:
- Перейти на зрелую библиотеку JWT или добавить тесты на malformed JSON/header/nbf/aud list.
- Явно проверять `header.alg == HS256` и `typ == JWT`.

## Положительные наблюдения

- `.env` присутствует локально, но не отслеживается git; `.gitignore` содержит `.env`.
- Production mode блокируется при дефолтных security secrets и root password.
- Password hashing использует `hashlib.scrypt` с солью и pepper.
- Refresh tokens хранятся в базе как SHA-256 hash и ротируются при refresh.
- Cookie auth защищен CSRF-header проверкой для write-запросов.
- MCP tools проверяются через `is_tool_enabled_for_user` перед выполнением.
- File manager ограничен managed roots и не использует shell.
- Frontend не использует `dangerouslySetInnerHTML`, `innerHTML`, localStorage/sessionStorage для токенов.
- `npm audit` не нашел уязвимостей в frontend-зависимостях.

## Проверка зависимостей

### Frontend

Команда:

```powershell
npm.cmd audit --audit-level=low --json
```

Результат: 0 уязвимостей, 123 зависимости всего.

### Python

`pip-audit` в `.venv` не установлен. Полная CVE-проверка Python-зависимостей не выполнена.

Рекомендации:

```powershell
.\.venv\Scripts\python.exe -m pip install pip-audit
.\.venv\Scripts\pip-audit.exe -r requirements.txt
```

Также стоит рассмотреть pinning с hashes через `pip-tools`/`requirements.lock`, потому что текущие диапазоны (`>=,<`) дают воспроизводимость только частично.

## Приоритетный план исправлений

1. Закрыть или ограничить OAuth dynamic registration.
2. Исправить PKCE: запретить `plain`, если `allow_plain_pkce=false`.
3. Реализовать client authentication для confidential OAuth clients.
4. Убрать доверие к `X-Forwarded-For` без trusted proxy.
5. Включить `SECURITY__COOKIE_SECURE=true` для HTTPS/production install.
6. Перевести опасные MCP tools (`process_manager`, Docker write/logs/inspect, ports probe) в deny-by-default и добавить allowlists.
7. Усилить systemd/sudoers hardening.
8. Запустить `pip-audit` и зафиксировать Python lockfile.

## Ограничения аудита

- Не проводился динамический pentest запущенного production-инстанса.
- Не проверялись реальные права Linux-пользователя, Docker socket, ownership installed files и sudoers на сервере.
- Не проверялась история git на ранее утекшие секреты.
- Python CVE-аудит не выполнен из-за отсутствия `pip-audit`.
- Код содержит mojibake в русских строках в некоторых файлах; это не уязвимость само по себе, но усложняет review и может скрывать ошибки в текстах UI/permissions.
