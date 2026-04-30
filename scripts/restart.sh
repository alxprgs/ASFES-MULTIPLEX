#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="asfes-multiplex"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Запустите перезапуск от root: sudo bash scripts/restart.sh"
  exit 1
fi

if ! command -v systemctl >/dev/null 2>&1; then
  echo "systemctl не найден. Перезапуск через этот скрипт доступен только на systemd-системах."
  exit 1
fi

nohup bash -c "sleep 1; systemctl restart '${SERVICE_NAME}'" >/dev/null 2>&1 &

echo "Перезапуск ${SERVICE_NAME} запланирован."
