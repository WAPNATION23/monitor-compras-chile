#!/usr/bin/env bash
# Entrypoint unificado: web | cron | update | comando libre
set -euo pipefail

DATA_DIR="${OJO_DATA_DIR:-/data}"
DB_FILE="${DATA_DIR}/auditoria_estado.db"
# Railway inyecta PORT; Docker local usa 8501
PORT="${PORT:-8501}"
APP_USER="${OJO_APP_USER:-ojo}"

mkdir -p "${DATA_DIR}"

# Volúmenes Railway suelen ser root:root → ajustar para el usuario de la app
if [[ "$(id -u)" -eq 0 ]]; then
  chown -R "${APP_USER}:${APP_USER}" "${DATA_DIR}" || true
  chmod 775 "${DATA_DIR}" || true
fi

_run_as_app() {
  if [[ "$(id -u)" -eq 0 ]] && id -u "${APP_USER}" >/dev/null 2>&1; then
    exec runuser -u "${APP_USER}" -- "$@"
  fi
  exec "$@"
}

_bootstrap_db() {
  if [[ -f "${DB_FILE}" ]] && [[ -s "${DB_FILE}" ]]; then
    return 0
  fi
  if [[ -f /app/seed.db.gz ]]; then
    echo "[entrypoint] BD vacía — restaurando seed.db.gz en ${DB_FILE}"
    gunzip -c /app/seed.db.gz > "${DB_FILE}"
  else
    echo "[entrypoint] BD vacía — creando esquema mínimo"
    python -c "from processor import DataProcessor; DataProcessor()"
  fi
  if [[ "$(id -u)" -eq 0 ]]; then
    chown "${APP_USER}:${APP_USER}" "${DB_FILE}" 2>/dev/null || true
  fi
}

_bootstrap_db

case "${1:-web}" in
  web)
    _run_as_app streamlit run dashboard.py \
      --server.port="${PORT}" \
      --server.address=0.0.0.0 \
      --server.headless=true \
      --browser.gatherUsageStats=false
    ;;
  cron)
    echo "[entrypoint] Cron activo — ver /app/docker/crontab"
    _run_as_app supercronic /app/docker/crontab
    ;;
  update)
    _run_as_app python daily_update.py --full --force
    ;;
  shell)
    _run_as_app bash
    ;;
  *)
    _run_as_app "$@"
    ;;
esac
