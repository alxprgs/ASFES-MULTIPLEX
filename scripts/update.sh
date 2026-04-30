#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="asfes-multiplex"
APP_USER="asfes-multiplex"
INSTALL_DIR="/opt/asfes-multiplex"
CONFIG_DIR="/etc/asfes-multiplex"
DATA_DIR="/var/lib/asfes-multiplex"
LOG_DIR="/var/log/asfes-multiplex"
ENV_FILE="${CONFIG_DIR}/multiplex.env"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Запустите обновление от root: sudo bash scripts/update.sh"
  exit 1
fi

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
SOURCE_DIR="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd)"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Не найден ${ENV_FILE}. Сначала выполните sudo bash scripts/install.sh"
  exit 1
fi

if [[ -d "${SOURCE_DIR}/.git" ]]; then
  echo "Обновляю исходный репозиторий: git fetch && git pull --ff-only"
  git -c "safe.directory=${SOURCE_DIR}" -C "${SOURCE_DIR}" fetch --all --prune
  git -c "safe.directory=${SOURCE_DIR}" -C "${SOURCE_DIR}" pull --ff-only
else
  echo "Git-репозиторий в ${SOURCE_DIR} не найден, пропускаю git fetch/pull"
fi

mkdir -p "${INSTALL_DIR}"
rsync -a --delete \
  --exclude ".venv" \
  --exclude ".env" \
  --exclude "frontend/node_modules" \
  --exclude "frontend/dist" \
  --exclude "runtime" \
  --exclude "data" \
  --exclude ".test_runtime" \
  --exclude ".pytest_cache" \
  --exclude "pytest-cache-files-*" \
  "${SOURCE_DIR}/" "${INSTALL_DIR}/"

if [[ ! -x "${INSTALL_DIR}/.venv/bin/python" ]]; then
  python3 -m venv "${INSTALL_DIR}/.venv"
fi

"${INSTALL_DIR}/.venv/bin/python" -m pip install --upgrade pip
"${INSTALL_DIR}/.venv/bin/python" -m pip install -r "${INSTALL_DIR}/requirements.txt"

if [[ -f "${INSTALL_DIR}/frontend/package-lock.json" ]]; then
  npm --prefix "${INSTALL_DIR}/frontend" ci
else
  npm --prefix "${INSTALL_DIR}/frontend" install
fi
npm --prefix "${INSTALL_DIR}/frontend" run build

sed -i '/^APP__VERSION=/d' "${ENV_FILE}"

chown -R "${APP_USER}:${APP_USER}" "${INSTALL_DIR}" "${DATA_DIR}" "${LOG_DIR}"
nohup bash -c "sleep 1; systemctl restart '${SERVICE_NAME}'" >/dev/null 2>&1 &

echo "Готово. Проверка:"
echo "  systemctl status ${SERVICE_NAME}"
echo "  curl -i http://127.0.0.1:5976/mcp/"
