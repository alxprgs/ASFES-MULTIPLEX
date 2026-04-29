# ASFES Multiplex

ASFES Multiplex - домашний FastAPI + MCP control plane с React-админкой. В production-сценарии backend, MCP и UI работают на одном HTTP-порту: `/api`, `/mcp`, `/.well-known/*` остаются служебными маршрутами, а остальные пути отдаёт React SPA.

## Что внутри

- FastAPI backend с JWT, HttpOnly cookie-login, CSRF для браузерных write-запросов и OAuth/PKCE для MCP-клиентов.
- React + TypeScript админ-панель: dashboard, runtime settings, пользователи, permissions, плагины, tools, audit и профиль.
- MCP gateway на FastMCP и набор локальных plugins для Docker, файлов, логов, процессов, firewall, scheduler, SSL, VPN, Nginx и системной статистики.
- MongoDB как основное хранилище, Redis опционально для runtime rate limit.
- Integrity-логи в файлы и SQLite, runtime-файлы исключены из git.

## Локальный запуск

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
cd frontend
npm install
npm run build
cd ..
.\.venv\Scripts\python.exe run.py
```

По умолчанию приложение слушает `0.0.0.0:8000`. Нужна доступная MongoDB из `MONGO__URI`; если её нет, backend не стартует полностью.

Полезные проверки:

```powershell
.\.venv\Scripts\python.exe -m pytest tests
cd frontend
npm run build
```

## Установка на Debian/Ubuntu

Скрипт ставит зависимости через `apt`, копирует проект в `/opt/asfes-multiplex`, создаёт venv, собирает frontend, спрашивает MongoDB URI и root-пароль, генерирует секреты, пишет `/etc/asfes-multiplex/multiplex.env` и создаёт systemd service.

```bash
sudo bash scripts/install.sh
```

После установки:

```bash
sudo systemctl status asfes-multiplex
sudo systemctl restart asfes-multiplex
sudo journalctl -u asfes-multiplex -f
```

Дефолтный режим рассчитан на LAN/VPN: `APP__HOST=0.0.0.0`, `APP__PORT=8000`, cookie `Secure=false`. Для публичного домена включите reverse proxy с HTTPS, задайте `APP__PUBLIC_BASE_URL`, `SECURITY__COOKIE_SECURE=true` и ограничьте доступ firewall.

## Важные env-настройки

- `MONGO__URI` - внешний URI MongoDB; установщик не поднимает MongoDB сам.
- `ROOT__USERNAME`, `ROOT__PASSWORD`, `ROOT__EMAIL` - первый root-доступ.
- `SECURITY__API_JWT_SECRET`, `SECURITY__OAUTH_JWT_SECRET`, `SECURITY__PASSWORD_PEPPER` - обязательные production-секреты.
- `APP__FRONTEND_DIST` - путь к собранному React UI, обычно `/opt/asfes-multiplex/frontend/dist`.
- `HOST_OPS__MANAGED_FILE_ROOTS` и `HOST_OPS__MANAGED_LOG_ROOTS` - разрешённые области для file/log tools.

В production запуск с дефолтными секретами и дефолтным root-паролем блокируется.

## Безопасность первого запуска

- Регистрация выключена по умолчанию.
- Write-tools глобально выключены при первом создании политик; включайте их точечно в UI.
- Browser UI использует HttpOnly cookies и CSRF header для write-запросов.
- Bearer API остаётся совместимым для скриптов и MCP/OAuth flow.
- Для домашнего сервера держите сервис за LAN/VPN или закрывайте порт firewall.
