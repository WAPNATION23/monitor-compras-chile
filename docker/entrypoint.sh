#!/usr/bin/env bash
# Entrypoint unificado: web | cron | update | comando libre
set -euo pipefail

DATA_DIR="${OJO_DATA_DIR:-/data}"
DB_FILE="${DATA_DIR}/auditoria_estado.db"
# Railway inyecta PORT; Docker local usa 8501
PORT="${PORT:-8501}"

mkdir -p "${DATA_DIR}"

_bootstrap_db() {
  if [[ -f "${DB_FILE}" ]] && [[ -s "${DB_FILE}" ]]; then
    return 0
  fi
  if [[ -f /app/seed.db.gz ]]; then
    echo "[entrypoint] BD vacía — restaurando seed.db.gz en ${DB_FILE}"
    gunzip -c /app/seed.db.gz > "${DB_FILE}"
    return 0
  fi
  echo "[entrypoint] BD vacía — creando esquema mínimo"
  python -c "from processor import DataProcessor; DataProcessor()"
}

_bootstrap_db

case "${1:-web}" in
  web)
    exec streamlit run dashboard.py \
      --server.port="${PORT}" \
      --server.address=0.0.0.0 \
      --server.headless=true \
      --browser.gatherUsageStats=false
    ;;
  cron)
    echo "[entrypoint] Cron activo — ver /app/docker/crontab"
    exec supercronic /app/docker/crontab
    ;;
  update)
    exec python daily_update.py --full --force
    ;;
  shell)
    exec bash
    ;;
  *)
    exec "$@"
    ;;
esac
