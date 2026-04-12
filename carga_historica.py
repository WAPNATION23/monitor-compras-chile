"""
Carga masiva histórica — recorre meses desde 2024 hasta hoy.
Usa el pipeline normal (main.run_pipeline) pero tolera errores por día.
Ejecutar: py carga_historica.py
"""
import logging
import sys
from datetime import date, timedelta

from main import run_pipeline, _setup_logging

_setup_logging(verbose=False, json_fmt=False)
logger = logging.getLogger(__name__)

# Configuración: desde cuándo hasta cuándo
FECHA_INICIO = date(2024, 1, 1)
FECHA_FIN = date(2026, 4, 9)
MAX_OC_POR_DIA = 500  # suficiente para la mayoría de los días

# Solo días hábiles (lun-vie) — fines de semana casi no hay OC
dias_habiles = []
d = FECHA_INICIO
while d <= FECHA_FIN:
    if d.weekday() < 5:  # 0=lunes ... 4=viernes
        dias_habiles.append(d)
    d += timedelta(days=1)

logger.info("Carga histórica: %d días hábiles desde %s hasta %s", len(dias_habiles), FECHA_INICIO, FECHA_FIN)

exitos = 0
fallos = 0
for i, fecha in enumerate(dias_habiles, 1):
    logger.info("═══ Día %d/%d: %s ═══", i, len(dias_habiles), fecha.strftime("%d/%m/%Y"))
    try:
        run_pipeline(
            fecha=fecha,
            solo_analisis=False,
            metodo="serenata",
            notificar_telegram=False,
            max_oc=MAX_OC_POR_DIA,
        )
        exitos += 1
    except Exception as exc:
        logger.error("Falló día %s: %s", fecha, exc)
        fallos += 1
        continue

logger.info("═══ CARGA HISTÓRICA COMPLETA ═══")
logger.info("Días exitosos: %d | Días fallidos: %d | Total: %d", exitos, fallos, exitos + fallos)
