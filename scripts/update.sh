#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="asfes-multiplex"
APP_USER="asfes-multiplex"
INSTALL_DIR="/opt/asfes-multiplex"
CONFIG_DIR="/etc/asfes-multiplex"
DATA_DIR="/var/lib/asfes-multiplex"
LOG_DIR="/var/log/asfes-multiplex"
ENV_FILE="${CONFIG_DIR}/multiplex.env"

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

configure_build_environment
"${INSTALL_DIR}/.venv/bin/python" -m pip install --upgrade pip
"${INSTALL_DIR}/.venv/bin/python" -m pip install -r "${INSTALL_DIR}/requirements.txt"

ensure_node_runtime
if [[ -f "${INSTALL_DIR}/frontend/package-lock.json" ]]; then
  npm --prefix "${INSTALL_DIR}/frontend" ci
else
  npm --prefix "${INSTALL_DIR}/frontend" install
fi
npm --prefix "${INSTALL_DIR}/frontend" run build

chown -R "${APP_USER}:${APP_USER}" "${INSTALL_DIR}" "${DATA_DIR}" "${LOG_DIR}"
nohup bash -c "sleep 1; systemctl restart '${SERVICE_NAME}'" >/dev/null 2>&1 &

echo "Готово. Проверка:"
echo "  systemctl status ${SERVICE_NAME}"
echo "  curl -i http://127.0.0.1:5976/mcp/"
