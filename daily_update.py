"""
daily_update.py
═══════════════
Pipeline de actualización diaria para "Ojo del Pueblo".

Mantiene los datos frescos sin intervención manual:
  • Extrae órdenes de compra y licitaciones del día anterior desde Mercado Público.
  • Procesa y guarda en SQLite (con dedupe).
  • Detecta anomalías sobre lo nuevo.
  • Actualiza un marker JSON con timestamp + estadísticas de la última corrida.

Modos de uso:
  - CLI:  `python daily_update.py`                 → corre ahora una sola vez
          `python daily_update.py --fecha 12052026` → para fecha específica
  - Programático:
        from daily_update import update_all, start_background_scheduler
        update_all()                       # corre ahora (sync, ~5 min)
        start_background_scheduler()       # daemon que corre cada 6h si vencida

El scheduler se llama una sola vez al arrancar el dashboard. Usa un lock global
para evitar duplicación si Streamlit re-importa el módulo.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import threading
import time as _time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from config import DB_NAME

logger = logging.getLogger(__name__)

# Marker JSON con la última corrida. Se persiste en el volumen de Railway
# (misma carpeta que la DB) para sobrevivir a redeploys.
_BASE = Path(DB_NAME).resolve().parent
_MARKER_PATH = _BASE / "last_update.json"

# Configuración del scheduler
SCHEDULE_INTERVAL_HOURS = 24   # cada cuánto se ejecuta cuando todo está al día
CHECK_INTERVAL_SECONDS = 3600  # cada cuánto despierta el daemon para chequear
STALE_THRESHOLD_HOURS = 18     # si pasó más que esto desde la última corrida, corre
EXTRACT_OC_MAX = int(os.getenv("DAILY_UPDATE_MAX_OC", "1000"))
EXTRACT_LIC_LIMIT = int(os.getenv("DAILY_UPDATE_MAX_LIC", "500"))

# Lock global anti-reentrancia
_scheduler_thread: threading.Thread | None = None
_run_lock = threading.Lock()


# ─────────────────────────── Marker I/O ───────────────────────────

def read_marker() -> dict[str, Any]:
    """Lee el marker JSON. Retorna dict vacío si no existe o está corrupto."""
    try:
        with open(_MARKER_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def _write_marker(payload: dict[str, Any]) -> None:
    """Escribe atómicamente el marker. No falla si el FS es read-only."""
    try:
        tmp = _MARKER_PATH.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False, default=str)
        tmp.replace(_MARKER_PATH)
    except OSError as exc:
        logger.warning("No se pudo escribir marker en %s: %s", _MARKER_PATH, exc)


def last_update_dt() -> datetime | None:
    """Retorna el datetime UTC de la última actualización exitosa o None."""
    raw = read_marker().get("ts_utc")
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def hours_since_last_update() -> float | None:
    """Retorna las horas transcurridas desde la última corrida, o None si nunca."""
    ts = last_update_dt()
    if ts is None:
        return None
    now = datetime.now(timezone.utc)
    return (now - ts).total_seconds() / 3600.0


def is_update_due() -> bool:
    """True si la última corrida fue hace > STALE_THRESHOLD_HOURS o nunca."""
    h = hours_since_last_update()
    return h is None or h > STALE_THRESHOLD_HOURS


# ─────────────────── Storage: tabla licitaciones ───────────────────

def _ensure_licitaciones_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS licitaciones (
            codigo_externo TEXT PRIMARY KEY,
            nombre TEXT,
            estado TEXT,
            fecha_publicacion TEXT,
            fecha_cierre TEXT,
            nombre_organismo TEXT,
            tipo TEXT,
            monto_estimado REAL,
            raw_json TEXT,
            fecha_extraccion TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def _store_licitaciones(licitaciones: list[dict[str, Any]]) -> int:
    """Inserta licitaciones nuevas en SQLite. Retorna cantidad insertada."""
    if not licitaciones:
        return 0
    inserted = 0
    with sqlite3.connect(DB_NAME) as conn:
        _ensure_licitaciones_table(conn)
        for lic in licitaciones:
            codigo = lic.get("CodigoExterno") or lic.get("codigo_externo") or ""
            if not codigo:
                continue
            try:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO licitaciones
                    (codigo_externo, nombre, estado, fecha_publicacion,
                     fecha_cierre, nombre_organismo, tipo, monto_estimado, raw_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        codigo,
                        str(lic.get("Nombre", ""))[:500],
                        str(lic.get("Estado", "")),
                        str(lic.get("FechaPublicacion", "")),
                        str(lic.get("FechaCierre", "")),
                        str(lic.get("NombreOrganismo", ""))[:300],
                        str(lic.get("Tipo", "")),
                        float(lic.get("MontoEstimado") or 0),
                        json.dumps(lic, ensure_ascii=False, default=str)[:5000],
                    ),
                )
                if conn.total_changes:
                    inserted += 1
            except (sqlite3.Error, ValueError) as exc:
                logger.warning("Error guardando licitación %s: %s", codigo, exc)
        conn.commit()
    return inserted


# ─────────────────────── Updaters por fuente ───────────────────────

def update_orders(fecha: date, max_oc: int | None = None) -> dict[str, Any]:
    """Extrae + procesa OCs de la fecha indicada. Retorna stats."""
    from extractor import MercadoPublicoExtractor
    from processor import DataProcessor

    t0 = _time.perf_counter()
    extractor = MercadoPublicoExtractor()
    kwargs = {"max_oc": max_oc} if max_oc else {}
    ordenes = extractor.extract(fecha, **kwargs)
    n_oc = len(ordenes) if ordenes else 0

    inserted = 0
    if ordenes:
        processor = DataProcessor()
        _, inserted = processor.process_and_store(ordenes)
    return {
        "fecha": fecha.isoformat(),
        "ocs_extraidas": n_oc,
        "items_insertados": inserted,
        "duration_s": round(_time.perf_counter() - t0, 1),
    }


def update_licitaciones(fecha: date) -> dict[str, Any]:
    """Extrae + guarda licitaciones de la fecha indicada."""
    from licitaciones_extractor import LicitacionesExtractor

    t0 = _time.perf_counter()
    try:
        extractor = LicitacionesExtractor()
        listado = extractor.extract_by_date(fecha)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Error extrayendo licitaciones: %s", exc)
        return {"error": str(exc), "duration_s": 0.0}
    inserted = _store_licitaciones(listado[:EXTRACT_LIC_LIMIT])
    return {
        "fecha": fecha.isoformat(),
        "licitaciones_descargadas": len(listado),
        "licitaciones_insertadas": inserted,
        "duration_s": round(_time.perf_counter() - t0, 1),
    }


# ─────────────────────────── update_all ────────────────────────────

def update_all(fecha: date | None = None, *, force: bool = False) -> dict[str, Any]:
    """
    Pipeline completo de actualización. Idempotente vía dedupe en SQLite.

    Args:
        fecha: fecha objetivo. Default = ayer.
        force: si False y is_update_due()==False, no hace nada.

    Retorna dict con stats. Si el ticket no está configurado o no se pudo
    conectar, retorna {"error": "..."} sin abortar.
    """
    if not force and not is_update_due():
        return {"skipped": True, "reason": "datos al día"}

    if not _run_lock.acquire(blocking=False):
        return {"skipped": True, "reason": "ya hay una actualización corriendo"}

    try:
        if fecha is None:
            fecha = date.today() - timedelta(days=1)

        ticket = os.getenv("MERCADO_PUBLICO_TICKET", "").strip()
        if not ticket:
            payload = {
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "error": "MERCADO_PUBLICO_TICKET no configurado",
            }
            _write_marker(payload)
            return payload

        logger.info("[daily_update] inicio - fecha=%s", fecha)
        stats: dict[str, Any] = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "fecha_objetivo": fecha.isoformat(),
        }

        try:
            stats["ordenes"] = update_orders(fecha, max_oc=EXTRACT_OC_MAX)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Error update_orders")
            stats["ordenes"] = {"error": str(exc)}

        try:
            stats["licitaciones"] = update_licitaciones(fecha)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Error update_licitaciones")
            stats["licitaciones"] = {"error": str(exc)}

        stats["completed_utc"] = datetime.now(timezone.utc).isoformat()
        _write_marker(stats)
        logger.info("[daily_update] fin: %s", stats)
        return stats
    finally:
        _run_lock.release()


# ─────────────────────── Scheduler embebido ───────────────────────

def _scheduler_loop() -> None:
    """Bucle daemon: cada hora chequea si toca actualizar."""
    # Pequeño delay inicial para que el dashboard termine de cargar primero
    _time.sleep(60)
    while True:
        try:
            if is_update_due():
                logger.info("[scheduler] actualización vencida, ejecutando")
                update_all()
            else:
                hrs = hours_since_last_update()
                logger.debug("[scheduler] datos al día (%.1f h)", hrs or -1)
        except Exception as exc:  # noqa: BLE001
            logger.exception("[scheduler] error inesperado: %s", exc)
        _time.sleep(CHECK_INTERVAL_SECONDS)


def start_background_scheduler() -> bool:
    """
    Arranca el daemon de actualización si aún no está corriendo.

    Idempotente: múltiples llamadas no crean threads duplicados. Útil porque
    Streamlit puede re-importar el módulo al recargar la app.

    Retorna True si se arrancó (o ya estaba corriendo), False si está deshabilitado
    por env var DISABLE_AUTO_UPDATE=1.
    """
    if os.getenv("DISABLE_AUTO_UPDATE", "").strip() in ("1", "true", "yes"):
        logger.info("[scheduler] deshabilitado por DISABLE_AUTO_UPDATE")
        return False

    global _scheduler_thread
    if _scheduler_thread is not None and _scheduler_thread.is_alive():
        return True

    t = threading.Thread(
        target=_scheduler_loop,
        name="daily-update-scheduler",
        daemon=True,
    )
    t.start()
    _scheduler_thread = t
    logger.info("[scheduler] arrancado")
    return True


# ───────────────────────────── CLI ─────────────────────────────────

def _parse_cli() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Actualización diaria de Ojo del Pueblo")
    p.add_argument("--fecha", help="Fecha objetivo ddmmaaaa (default: ayer)")
    p.add_argument("--force", action="store_true", help="Forzar aunque esté al día")
    return p.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )
    args = _parse_cli()
    fecha = None
    if args.fecha:
        fecha = datetime.strptime(args.fecha, "%d%m%Y").date()
    stats = update_all(fecha=fecha, force=args.force)
    print(json.dumps(stats, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
