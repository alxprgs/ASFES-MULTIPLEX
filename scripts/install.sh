#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="asfes-multiplex"
APP_USER="asfes-multiplex"
INSTALL_DIR="/opt/asfes-multiplex"
CONFIG_DIR="/etc/asfes-multiplex"
DATA_DIR="/var/lib/asfes-multiplex"
LOG_DIR="/var/log/asfes-multiplex"
ENV_FILE="${CONFIG_DIR}/multiplex.env"
UNIT_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
SUDOERS_FILE="/etc/sudoers.d/${SERVICE_NAME}-update"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Запустите установщик от root: sudo bash scripts/install.sh"
  exit 1
fi

if ! command -v apt-get >/dev/null 2>&1; then
  echo "Поддерживаются Debian/Ubuntu-системы с apt."
  exit 1
fi

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
SOURCE_DIR="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd)"

node_runtime_supported() {
  command -v node >/dev/null 2>&1 || return 1
  command -v npm >/dev/null 2>&1 || return 1
  [[ "$(node -e "const [major, minor] = process.versions.node.split('.').map(Number); process.stdout.write((major === 20 && minor >= 19) || major >= 22 ? 'yes' : 'no')")" == "yes" ]]
}

ensure_node_runtime() {
  local node_root="${DATA_DIR}/runtime/nodejs"
  if [[ -x "${node_root}/bin/node" ]]; then
    export PATH="${node_root}/bin:${PATH}"
  fi
  if node_runtime_supported; then
    echo "Node.js подходит: $(node --version), npm $(npm --version)"
    return
  fi
  local platform
  case "$(uname -m)" in
    x86_64 | amd64) platform="linux-x64" ;;
    aarch64 | arm64) platform="linux-arm64" ;;
    *) echo "Неподдерживаемая архитектура для портативного Node.js: $(uname -m)"; exit 1 ;;
  esac
  echo "Устанавливаю портативный Node.js 22 для сборки frontend..."
  local tmp_dir archive
  tmp_dir="$(mktemp -d)"
  archive="$(curl -fsSL "https://nodejs.org/dist/latest-v22.x/SHASUMS256.txt" | awk -v platform="${platform}" '$2 ~ ("node-v.*-" platform ".tar.xz$") {print $2; exit}')"
  if [[ -z "${archive}" ]]; then
    echo "Не удалось найти архив Node.js 22 для ${platform}."
    exit 1
  fi
  curl -fSL "https://nodejs.org/dist/latest-v22.x/${archive}" -o "${tmp_dir}/${archive}"
  rm -rf "${node_root}"
  mkdir -p "${node_root}"
  tar -xJf "${tmp_dir}/${archive}" -C "${node_root}" --strip-components=1
  rm -rf "${tmp_dir}"
  export PATH="${node_root}/bin:${PATH}"
  echo "Node.js установлен: $(node --version), npm $(npm --version)"
}

configure_build_environment() {
  mkdir -p "${DATA_DIR}/runtime/npm-cache" "${DATA_DIR}/runtime/build-home"
  export PIP_DISABLE_PIP_VERSION_CHECK=1
  export PIP_NO_CACHE_DIR=1
  export NPM_CONFIG_CACHE="${DATA_DIR}/runtime/npm-cache"
  export npm_config_cache="${DATA_DIR}/runtime/npm-cache"
  export HOME="${DATA_DIR}/runtime/build-home"
}

read -rp "MongoDB URI [mongodb://127.0.0.1:27017]: " MONGO_URI
MONGO_URI="${MONGO_URI:-mongodb://127.0.0.1:27017}"

read -rp "Root username [root]: " ROOT_USERNAME
ROOT_USERNAME="${ROOT_USERNAME:-root}"

while true; do
  read -rp "Root email [root@multiplex.asfes.ru]: " ROOT_EMAIL
  ROOT_EMAIL="${ROOT_EMAIL:-root@multiplex.asfes.ru}"
  if [[ ! "${ROOT_EMAIL}" =~ ^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$ ]]; then
    echo "Введите корректный email для root-пользователя."
    continue
  fi
  if [[ "${ROOT_EMAIL}" =~ \.(local|localhost|test|invalid|example)$ ]]; then
    echo "EmailStr не принимает reserved/local домены. Укажите обычный домен, например root@multiplex.asfes.ru."
    continue
  fi
  break
done

while true; do
  read -rsp "Root password (минимум 12 символов): " ROOT_PASSWORD
  echo
  read -rsp "Повторите root password: " ROOT_PASSWORD_CONFIRM
  echo
  if [[ "${ROOT_PASSWORD}" != "${ROOT_PASSWORD_CONFIRM}" ]]; then
    echo "Пароли не совпали."
    continue
  fi
  if [[ "${#ROOT_PASSWORD}" -lt 12 ]]; then
    echo "Пароль должен быть не короче 12 символов."
    continue
  fi
  break
done

read -rp "Listen host [0.0.0.0]: " APP_HOST
APP_HOST="${APP_HOST:-0.0.0.0}"

read -rp "Listen port [8000]: " APP_PORT
APP_PORT="${APP_PORT:-8000}"

DEFAULT_PUBLIC_URL="http://$(hostname -f 2>/dev/null || hostname):${APP_PORT}"
read -rp "Public base URL [${DEFAULT_PUBLIC_URL}]: " PUBLIC_BASE_URL
PUBLIC_BASE_URL="${PUBLIC_BASE_URL:-${DEFAULT_PUBLIC_URL}}"

echo "Устанавливаю системные зависимости..."
apt-get update
apt-get install -y python3 python3-venv python3-pip rsync sudo git ca-certificates curl gnupg

if ! id -u "${APP_USER}" >/dev/null 2>&1; then
  useradd --system --home "${DATA_DIR}" --shell /usr/sbin/nologin "${APP_USER}"
fi

mkdir -p "${INSTALL_DIR}" "${CONFIG_DIR}" "${DATA_DIR}" "${DATA_DIR}/runtime" "${DATA_DIR}/data" "${LOG_DIR}"
rsync -a --delete \
  --exclude ".venv" \
  --exclude ".env" \
  --exclude "frontend/node_modules" \
  --exclude "runtime" \
  --exclude "data" \
  --exclude ".test_runtime" \
  --exclude "pytest-cache-files-*" \
  "${SOURCE_DIR}/" "${INSTALL_DIR}/"

configure_build_environment
ensure_node_runtime
python3 -m venv "${INSTALL_DIR}/.venv"
"${INSTALL_DIR}/.venv/bin/python" -m pip install --upgrade pip
"${INSTALL_DIR}/.venv/bin/python" -m pip install -r "${INSTALL_DIR}/requirements.txt"

ROOT_EMAIL="${ROOT_EMAIL}" "${INSTALL_DIR}/.venv/bin/python" - <<'PY'
import os
from pydantic import BaseModel, EmailStr

class RootEmailCheck(BaseModel):
    email: EmailStr

RootEmailCheck(email=os.environ["ROOT_EMAIL"])
PY

if [[ -f "${INSTALL_DIR}/frontend/package-lock.json" ]]; then
  npm --prefix "${INSTALL_DIR}/frontend" ci
else
  npm --prefix "${INSTALL_DIR}/frontend" install
fi
npm --prefix "${INSTALL_DIR}/frontend" run build

SECRET_VALUES="$("${INSTALL_DIR}/.venv/bin/python" - <<'PY'
import secrets
print(secrets.token_urlsafe(48))
print(secrets.token_urlsafe(48))
print(secrets.token_urlsafe(48))
PY
)"
API_SECRET="$(sed -n '1p' <<<"${SECRET_VALUES}")"
OAUTH_SECRET="$(sed -n '2p' <<<"${SECRET_VALUES}")"
PASSWORD_PEPPER="$(sed -n '3p' <<<"${SECRET_VALUES}")"

cat >"${ENV_FILE}" <<EOF
APP__NAME=ASFES Multiplex
APP__ENV=production
APP__DEV=false
APP__HOST=${APP_HOST}
APP__PORT=${APP_PORT}
APP__PUBLIC_BASE_URL=${PUBLIC_BASE_URL}
APP__API_PREFIX=/api
APP__MCP_PATH=/mcp
APP__FRONTEND_DIST=${INSTALL_DIR}/frontend/dist
APP__STARTUP_PROGRESS=false
APP__TRUSTED_PROXY_IPS=["127.0.0.1","::1"]

MONGO__URI=${MONGO_URI}
MONGO__DATABASE=asfes_multiplex
MONGO__CONNECT_TIMEOUT_MS=5000
MONGO__SERVER_SELECTION_TIMEOUT_MS=5000
MONGO__MAX_POOL_SIZE=50

REDIS__MODE=runtime
REDIS__URL=redis://127.0.0.1:6379/0
REDIS__ENABLED_ON_STARTUP=false

SMTP__ENABLED=false

ROOT__USERNAME=${ROOT_USERNAME}
ROOT__PASSWORD=${ROOT_PASSWORD}
ROOT__EMAIL=${ROOT_EMAIL}
ROOT__DISPLAY_NAME=Root

LOGGING__LEVEL=INFO
LOGGING__DIRECTORY=${LOG_DIR}
LOGGING__SQLITE_PATH=${DATA_DIR}/runtime/multiplex_logs.db
LOGGING__VERIFIER_INTERVAL_SECONDS=600
LOGGING__CONSOLE_RICH_TRACEBACKS=true

HOST_OPS__MANAGED_FILE_ROOTS=["${DATA_DIR}/data","${DATA_DIR}/runtime"]
HOST_OPS__MANAGED_LOG_ROOTS=["${LOG_DIR}"]
HOST_OPS__BACKUP_DIRECTORY=${DATA_DIR}/backups
HOST_OPS__COMMAND_TIMEOUT_SECONDS=30
HOST_OPS__MAX_OUTPUT_BYTES=65536
HOST_OPS__ALERT_POLL_INTERVAL_SECONDS=60
HOST_OPS__EXECUTABLE_OVERRIDES={}
HOST_OPS__PROVIDER_OVERRIDES={}
HOST_OPS__DATABASE_PROFILES_DIRECTORY=${DATA_DIR}/profiles/databases
HOST_OPS__VPN_PROFILES_DIRECTORY=${DATA_DIR}/profiles/vpn
HOST_OPS__SSL_PROFILES_DIRECTORY=${DATA_DIR}/profiles/ssl
HOST_OPS__NGINX_CONFIG_PATHS=["${DATA_DIR}/nginx"]
HOST_OPS__PROCESS_ALLOWED_EXECUTABLES=[]
HOST_OPS__PORT_PROBE_ALLOWED_HOSTS=["127.0.0.1","::1","localhost"]

SECURITY__API_JWT_SECRET=${API_SECRET}
SECURITY__OAUTH_JWT_SECRET=${OAUTH_SECRET}
SECURITY__PASSWORD_PEPPER=${PASSWORD_PEPPER}
SECURITY__ACCESS_TOKEN_TTL_MINUTES=15
SECURITY__REFRESH_TOKEN_TTL_DAYS=30
SECURITY__OAUTH_ACCESS_TOKEN_TTL_MINUTES=30
SECURITY__OAUTH_REFRESH_TOKEN_TTL_DAYS=30
SECURITY__OAUTH_AUTHORIZATION_CODE_TTL_MINUTES=10
SECURITY__SESSION_COOKIE_NAME=multiplex_session
SECURITY__CSRF_COOKIE_NAME=multiplex_csrf
SECURITY__COOKIE_SECURE=true
SECURITY__COOKIE_SAMESITE=lax
SECURITY__ALLOW_INSECURE_COOKIES=false
SECURITY__API_AUDIENCE=multiplex-api
SECURITY__MCP_AUDIENCE=multiplex-mcp

PASSWORD_POLICY__MIN_LENGTH=12
PASSWORD_POLICY__FORBIDDEN_PASSWORDS=["password","password123","qwerty123","admin123","changeme","changemerootpassword123!"]

OAUTH__ISSUER_PATH=/api/oauth
OAUTH__AUTHORIZATION_PATH=/api/oauth/authorize
OAUTH__TOKEN_PATH=/api/oauth/token
OAUTH__REVOCATION_PATH=/api/oauth/revoke
OAUTH__CLIENTS_PATH=/api/oauth/clients
OAUTH__JWKS_PATH=/api/oauth/jwks
OAUTH__SUPPORTED_SCOPES=["mcp","profile"]
OAUTH__REQUIRE_PKCE=true
OAUTH__ALLOW_PLAIN_PKCE=false
EOF

chmod 600 "${ENV_FILE}"
chown -R "${APP_USER}:${APP_USER}" "${INSTALL_DIR}" "${DATA_DIR}" "${LOG_DIR}"
chown root:"${APP_USER}" "${ENV_FILE}"

rm -f "${SUDOERS_FILE}"

echo "Проверяю подключение к MongoDB..."
MONGO__URI="${MONGO_URI}" "${INSTALL_DIR}/.venv/bin/python" - <<'PY'
import os
from pymongo import MongoClient
uri = os.environ["MONGO__URI"]
client = MongoClient(uri, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000)
client.admin.command("ping")
client.close()
PY

cat >"${UNIT_FILE}" <<EOF
[Unit]
Description=ASFES Multiplex
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${APP_USER}
Group=${APP_USER}
WorkingDirectory=${INSTALL_DIR}
EnvironmentFile=${ENV_FILE}
ExecStart=${INSTALL_DIR}/.venv/bin/python ${INSTALL_DIR}/run.py
Restart=on-failure
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true
ProtectSystem=full
ReadWritePaths=${DATA_DIR} ${LOG_DIR}

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "${SERVICE_NAME}"
systemctl restart "${SERVICE_NAME}"

echo "Готово. Статус: systemctl status ${SERVICE_NAME}"
echo "URL: ${PUBLIC_BASE_URL}"
