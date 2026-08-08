#!/usr/bin/env bash
# Pipeline diario para Railway Cron o GitHub Actions.
# Ejecutar: bash scripts/cron_update.sh
set -euo pipefail
cd "$(dirname "$0")/.."
echo "[cron] Ojo del Pueblo — $(date -u +%Y-%m-%dT%H:%M:%SZ)"
python daily_update.py --full --force
echo "[cron] Completado."
