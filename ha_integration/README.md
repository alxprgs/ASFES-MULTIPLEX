# ASFES Multiplex — Home Assistant Integration

This custom integration allows Home Assistant to monitor and control your ASFES Multiplex instance.

## Installation

1. Copy the `custom_components/asfes_multiplex` directory to your Home Assistant `config/custom_components/` folder.
2. Restart Home Assistant.
3. Go to **Settings → Devices & Services → Add Integration**.
4. Search for **ASFES Multiplex** and follow the setup wizard.

## Configuration

During setup you will be asked for:
- **Server URL** — e.g. `http://192.168.1.100:8000`
- **Username** and **Password** — your ASFES Multiplex account credentials
- **Connection Label** — a friendly name for this connection (optional)

If your account has 2FA enabled, you will be prompted for your TOTP code.

## Entities

### Sensors
- CPU Usage (%)
- RAM Usage (%)
- Disk Usage (%)
- Uptime (seconds)
- Network RX / TX (bytes, cumulative)
- Temperature (°C, if available)
- Docker Containers Running
- Running Processes
- Redis Connected Clients
- MongoDB Connected Clients

### Binary Sensors
- MongoDB Online
- Redis Online
- API Healthy
- MCP Healthy
- Python Mirror Running
- PyPI Mirror Running

### Switches (requires `HA__SWITCHES_ENABLED=true` on server)
- Enable Registration
- Enable MCP
- Enable Redis

### Buttons
- Reload Plugins
- Refresh Python Mirror
- Refresh PyPI
- Restart Multiplex *(requires `HA__DESTRUCTIVE_BUTTONS_ENABLED=true`)*
- Restart Docker *(requires `HA__DESTRUCTIVE_BUTTONS_ENABLED=true`)*

## Security

HA tokens are completely isolated from regular API tokens:
- Separate JWT secrets (`HA__JWT_SECRET` and `HA__REFRESH_JWT_SECRET`)
- Separate audience claims (`home-assistant`)
- Tokens only work on `/api/ha/*` endpoints
- Short-lived access tokens (30 min) + long-lived refresh tokens (365 days)
- Automatic token refresh before expiry

## Options

After installation, go to the integration options to configure:
- **Update Interval** (10–300 seconds, default: 30)

---

# ASFES Multiplex — Интеграция с Home Assistant

Эта кастомная интеграция позволяет Home Assistant отслеживать состояние и управлять вашим инстансом ASFES Multiplex.

## Установка

1. Скопируйте папку `custom_components/asfes_multiplex` в директорию `config/custom_components/` вашего Home Assistant.
2. Перезагрузите Home Assistant.
3. Перейдите в **Настройки → Устройства и службы → Добавить интеграцию**.
4. Найдите **ASFES Multiplex** и следуйте подсказкам мастера настройки.

## Настройка

При первоначальной настройке вам потребуется указать:
- **Server URL** — например, `http://192.168.1.100:8000`
- **Username** и **Password** — учетные данные вашего пользователя ASFES Multiplex
- **Connection Label** — понятное название для этого подключения (необязательно)

Если для вашей учетной записи включена двухфакторная аутентификация (2FA), система попросит ввести ваш одноразовый TOTP-код.

## Сущности

### Сенсоры (Sensors)
- Загрузка CPU (%)
- Загрузка RAM (%)
- Загрузка диска (%)
- Время работы / Uptime (в секундах)
- Получено / Отправлено по сети (в байтах, накопительно)
- Температура (°C, если поддерживается платформой)
- Запущенные Docker-контейнеры
- Запущенные процессы
- Активные клиенты Redis
- Активные клиенты MongoDB

### Бинарные сенсоры (Binary Sensors)
- MongoDB онлайн (MongoDB Online)
- Redis онлайн (Redis Online)
- Здоровье API (API Healthy)
- Здоровье MCP (MCP Healthy)
- Зеркало Python активно (Python Mirror Running)
- Зеркало PyPI активно (PyPI Mirror Running)

### Выключатели (Switches, требует `HA__SWITCHES_ENABLED=true` на сервере)
- Включение регистрации (Enable Registration)
- Включение MCP (Enable MCP)
- Включение Redis (Enable Redis)

### Кнопки (Buttons)
- Перезагрузить плагины (Reload Plugins)
- Обновить зеркало Python (Refresh Python Mirror)
- Обновить PyPI (Refresh PyPI)
- Перезапустить Multiplex *(требует `HA__DESTRUCTIVE_BUTTONS_ENABLED=true`)*
- Перезапустить Docker *(требует `HA__DESTRUCTIVE_BUTTONS_ENABLED=true`)*

## Безопасность

Токены для Home Assistant полностью изолированы от основных токенов API:
- Отдельные секретные ключи JWT (`HA__JWT_SECRET` и `HA__REFRESH_JWT_SECRET`)
- Отдельный идентификатор получателя (`home-assistant` в поле aud)
- Токены дают доступ исключительно к эндпоинтам `/api/ha/*`
- Короткоживущие access-токены (30 минут) + долгоживущие refresh-токены (365 дней)
- Автоматическая ротация и обновление токенов до истечения срока их действия

## Параметры (Options)

После настройки интеграции в меню «Параметры» можно настроить:
- **Интервал опроса** (от 10 до 300 секунд, по умолчанию: 30)
