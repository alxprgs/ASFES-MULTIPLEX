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
