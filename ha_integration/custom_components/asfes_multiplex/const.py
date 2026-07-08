"""Constants for the ASFES Multiplex integration."""

from __future__ import annotations

DOMAIN = "asfes_multiplex"
VERSION = "1.0.0"
MANUFACTURER = "ASFES"

# Config entry data keys
CONF_HOST = "host"
CONF_ACCESS_TOKEN = "access_token"
CONF_REFRESH_TOKEN = "refresh_token"
CONF_ACCOUNT_LABEL = "account_label"
CONF_SERIAL = "serial"

# Options keys
OPT_POLL_INTERVAL = "poll_interval"
OPT_POLL_INTERVAL_DEFAULT = 30
OPT_POLL_INTERVAL_MIN = 10
OPT_POLL_INTERVAL_MAX = 300

# API paths
API_AUTH = "/api/ha/auth/token"
API_AUTH_2FA = "/api/ha/auth/token/2fa"
API_REFRESH = "/api/ha/auth/token/refresh"
API_REVOKE = "/api/ha/auth/token"
API_STATE = "/api/ha/state"
API_DIAGNOSTICS = "/api/ha/diagnostics"
API_SWITCH = "/api/ha/switches/{name}"
API_BUTTON = "/api/ha/buttons/{name}"
API_HEALTH = "/api/health"
