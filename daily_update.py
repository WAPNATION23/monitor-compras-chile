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

# Configuración del scheduler (SCHEDULE_EVERY_HOURS se define abajo con el resto de env)
CHECK_INTERVAL_SECONDS = 1800  # despierta cada 30 min; decide si toca correr
STALE_THRESHOLD_HOURS = 11     # con lote cada 12h, disparar un poco antes
EXTRACT_OC_MAX = int(os.getenv("DAILY_UPDATE_MAX_OC", "5000"))
EXTRACT_LIC_LIMIT = int(os.getenv("DAILY_UPDATE_MAX_LIC", "500"))
CATCHUP_MAX_DAYS = int(os.getenv("CATCHUP_MAX_DAYS", "14"))
RESYNC_RECENT_DAYS = int(os.getenv("RESYNC_RECENT_DAYS", "30"))
RESYNC_MAX_OCS = int(os.getenv("RESYNC_MAX_OCS", "200"))
SERVEL_REFRESH_DAYS = int(os.getenv("SERVEL_REFRESH_DAYS", "7"))
# Relleno lento: evita 429 y construye BD grande a lo largo de semanas
BACKFILL_ENABLED = os.getenv("BACKFILL_ENABLED", "1").strip().lower() in ("1", "true", "yes")
BACKFILL_OC_BUDGET = int(os.getenv("BACKFILL_OC_BUDGET", "500"))
BACKFILL_HORIZON_DAYS = int(os.getenv("BACKFILL_HORIZON_DAYS", "180"))
# Scheduler embebido: cada cuántas horas intenta (12 = 2 lotes/día)
SCHEDULE_EVERY_HOURS = float(os.getenv("SCHEDULE_EVERY_HOURS", "12"))
_BACKFILL_STATE_PATH = _BASE / "backfill_state.json"

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
    """True si la última corrida fue hace > umbral o nunca."""
    h = hours_since_last_update()
    threshold = max(STALE_THRESHOLD_HOURS, SCHEDULE_EVERY_HOURS - 1.0)
    return h is None or h > threshold


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

def _existing_oc_codes_for_date(fecha: date) -> set[str]:
    """Códigos OC ya persistidos para una fecha (evita gastar presupuesto en dupes)."""
    try:
        with sqlite3.connect(DB_NAME) as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT codigo_oc FROM ordenes_items
                WHERE substr(fecha_creacion, 1, 10) = ?
                """,
                (fecha.isoformat(),),
            ).fetchall()
        return {r[0] for r in rows if r[0]}
    except sqlite3.Error:
        return set()


def update_orders(fecha: date, max_oc: int | None = None) -> dict[str, Any]:
    """Extrae + procesa OCs de la fecha indicada. Retorna stats."""
    from extractor import MercadoPublicoExtractor
    from processor import DataProcessor

    t0 = _time.perf_counter()
    extractor = MercadoPublicoExtractor()
    skip = _existing_oc_codes_for_date(fecha)
    kwargs: dict[str, Any] = {"skip_codes": skip}
    if max_oc is not None:
        kwargs["max_oc"] = max_oc
    ordenes = extractor.extract(fecha, **kwargs)
    n_oc = len(ordenes) if ordenes else 0

    inserted = 0
    if ordenes:
        processor = DataProcessor()
        _, inserted = processor.process_and_store(ordenes)
    return {
        "fecha": fecha.isoformat(),
        "ocs_extraidas": n_oc,
        "ocs_ya_en_bd": len(skip),
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


def get_latest_oc_date() -> date | None:
    """Última fecha de OC en la BD (aprox.)."""
    try:
        with sqlite3.connect(DB_NAME) as conn:
            row = conn.execute(
                "SELECT MAX(substr(fecha_creacion, 1, 10)) FROM ordenes_items"
            ).fetchone()
        if not row or not row[0]:
            return None
        return datetime.strptime(str(row[0])[:10], "%Y-%m-%d").date()
    except (sqlite3.Error, ValueError):
        return None


def _first_gap_day(since: date, until: date) -> date | None:
    """Primer día hábil sin filas entre since y until (inclusive)."""
    d = since
    with sqlite3.connect(DB_NAME) as conn:
        while d <= until:
            cnt = conn.execute(
                "SELECT COUNT(*) FROM ordenes_items WHERE substr(fecha_creacion, 1, 10) = ?",
                (d.isoformat(),),
            ).fetchone()[0]
            if cnt == 0:
                return d
            d += timedelta(days=1)
    return None


def catch_up_orders(max_days: int | None = None) -> dict[str, Any]:
    """Recupera días faltantes (incluso si hay huecos en el medio)."""
    max_days = max_days if max_days is not None else CATCHUP_MAX_DAYS
    ayer = date.today() - timedelta(days=1)
    latest = get_latest_oc_date()

    if latest is None:
        # BD vacía: respetar CATCHUP_MAX_DAYS (antes hardcodeaba 3 y perdía historial)
        start = ayer - timedelta(days=max(0, max_days - 1))
    else:
        # Buscar huecos desde el día siguiente al mínimo razonable
        scan_from = min(latest, ayer) - timedelta(days=max_days)
        if scan_from < date(2018, 1, 1):
            scan_from = date(2018, 1, 1)
        gap = _first_gap_day(scan_from, ayer)
        if gap is None:
            return {"skipped": True, "reason": "sin días pendientes", "latest": latest.isoformat() if latest else None}
        start = gap

    dias: list[date] = []
    d = start
    while d <= ayer and len(dias) < max_days:
        dias.append(d)
        d += timedelta(days=1)

    results = []
    total_inserted = 0
    for dia in dias:
        try:
            r = update_orders(dia, max_oc=EXTRACT_OC_MAX)
            total_inserted += r.get("items_insertados", 0)
            results.append(r)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Catch-up falló %s: %s", dia, exc)
            results.append({"fecha": dia.isoformat(), "error": str(exc)})

    return {
        "dias_procesados": len(dias),
        "desde": dias[0].isoformat() if dias else None,
        "hasta": dias[-1].isoformat() if dias else None,
        "items_insertados_total": total_inserted,
        "detalle": results,
    }


def _read_backfill_state() -> dict[str, Any]:
    try:
        with open(_BACKFILL_STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def _write_backfill_state(payload: dict[str, Any]) -> None:
    try:
        _BACKFILL_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(_BACKFILL_STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except OSError as exc:
        logger.warning("No se pudo escribir backfill_state: %s", exc)


def slow_backfill(
    budget: int | None = None,
    horizon_days: int | None = None,
) -> dict[str, Any]:
    """
    Rellena historial de a poco: máximo `budget` OCs por corrida (~500 / 12h).

    Camina hacia atrás desde ayer dentro del horizonte, salta OCs ya en BD,
    y guarda cursor para continuar en la siguiente ventana.
    """
    from extractor import MercadoPublicoExtractor

    budget = budget if budget is not None else BACKFILL_OC_BUDGET
    horizon_days = horizon_days if horizon_days is not None else BACKFILL_HORIZON_DAYS
    if budget <= 0:
        return {"skipped": True, "reason": "budget<=0"}

    ayer = date.today() - timedelta(days=1)
    scan_from = ayer - timedelta(days=max(0, horizon_days - 1))
    if scan_from < date(2018, 1, 1):
        scan_from = date(2018, 1, 1)

    state = _read_backfill_state()
    cursor_s = state.get("cursor_date")
    try:
        cursor = datetime.strptime(str(cursor_s)[:10], "%Y-%m-%d").date() if cursor_s else ayer
    except ValueError:
        cursor = ayer
    if cursor < scan_from or cursor > ayer:
        cursor = ayer

    remaining = budget
    detalle: list[dict[str, Any]] = []
    total_oc = 0
    total_items = 0
    extractor = MercadoPublicoExtractor()

    d = cursor
    while remaining > 0 and d >= scan_from:
        try:
            listado = extractor.extract_fast(d)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Backfill listado falló %s: %s", d, exc)
            detalle.append({"fecha": d.isoformat(), "error": str(exc)})
            d -= timedelta(days=1)
            continue

        if not listado:
            d -= timedelta(days=1)
            continue

        existentes = _existing_oc_codes_for_date(d)
        pendientes = sum(1 for oc in listado if oc.get("Codigo") and oc["Codigo"] not in existentes)
        if pendientes <= 0:
            d -= timedelta(days=1)
            continue

        tomar = min(remaining, pendientes)
        try:
            r = update_orders(d, max_oc=tomar)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Backfill update falló %s: %s", d, exc)
            detalle.append({"fecha": d.isoformat(), "error": str(exc)})
            break

        ocs = int(r.get("ocs_extraidas") or 0)
        remaining -= ocs
        total_oc += ocs
        total_items += int(r.get("items_insertados") or 0)
        r["pendientes_antes"] = pendientes
        detalle.append(r)

        # Si el día aún tiene pendientes, el cursor se queda ahí
        if ocs < pendientes and remaining <= 0:
            break
        d -= timedelta(days=1)

    next_cursor = max(d, scan_from)
    _write_backfill_state({
        "cursor_date": next_cursor.isoformat(),
        "horizon_days": horizon_days,
        "last_budget": budget,
        "last_ocs": total_oc,
        "updated_utc": datetime.now(timezone.utc).isoformat(),
        "scan_from": scan_from.isoformat(),
        "scan_to": ayer.isoformat(),
    })

    return {
        "mode": "slow_backfill",
        "budget": budget,
        "ocs_extraidas": total_oc,
        "items_insertados": total_items,
        "budget_restante": max(0, remaining),
        "cursor_next": next_cursor.isoformat(),
        "horizon_days": horizon_days,
        "detalle": detalle,
        "done": total_oc == 0 and remaining == budget,
    }


def resync_recent_orders(days: int | None = None, max_ocs: int | None = None) -> dict[str, Any]:
    """Re-descarga OCs recientes para corregir montos, organismos y moneda."""
    from extractor import MercadoPublicoExtractor
    from processor import DataProcessor

    days = days if days is not None else RESYNC_RECENT_DAYS
    max_ocs = max_ocs if max_ocs is not None else RESYNC_MAX_OCS
    cutoff = (date.today() - timedelta(days=days)).isoformat()

    with sqlite3.connect(DB_NAME) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT codigo_oc FROM ordenes_items
            WHERE substr(fecha_creacion, 1, 10) >= ?
            ORDER BY fecha_creacion DESC
            LIMIT ?
            """,
            (cutoff, max_ocs),
        ).fetchall()
    codigos = [r[0] for r in rows if r[0]]
    if not codigos:
        return {"skipped": True, "reason": "sin OCs recientes"}

    extractor = MercadoPublicoExtractor()
    processor = DataProcessor()
    ordenes: list[dict[str, Any]] = []
    errores = 0
    for codigo in codigos:
        try:
            detail = extractor.fetch_oc_detail(codigo)
            if detail:
                ordenes.append(detail)
        except Exception as exc:  # noqa: BLE001
            errores += 1
            logger.debug("Resync OC %s: %s", codigo, exc)

    ocs_ok, items = processor.refresh_orders(ordenes)
    return {
        "ocs_solicitadas": len(codigos),
        "ocs_refrescadas": ocs_ok,
        "items_actualizados": items,
        "errores_api": errores,
    }


def update_secondary_sources(force: bool = False) -> dict[str, Any]:
    """SERVEL, CGR e InfoProbidad — fuentes que no corrían en el pipeline diario."""
    stats: dict[str, Any] = {}
    marker = read_marker()
    last_servel = marker.get("servel_utc", "")

    # SERVEL — semanal
    run_servel = force
    if not run_servel and last_servel:
        try:
            ts = datetime.fromisoformat(last_servel)
            run_servel = (datetime.now(timezone.utc) - ts).days >= SERVEL_REFRESH_DAYS
        except ValueError:
            run_servel = True
    else:
        run_servel = True

    if run_servel:
        try:
            from cargar_servel_auto import main as servel_main
            from cargar_gastos_servel import main as gastos_main

            servel_main()
            gastos_main()
            stats["servel"] = {"ok": True}
            marker["servel_utc"] = datetime.now(timezone.utc).isoformat()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Error cargando SERVEL: %s", exc)
            stats["servel"] = {"error": str(exc)}
    else:
        stats["servel"] = {"skipped": True}

    try:
        from contraloria_connector import ContraloriaConnector

        cgr = ContraloriaConnector(DB_NAME)
        fisc = cgr.obtener_fiscalizaciones()
        n_f = cgr.guardar_fiscalizaciones(fisc) if fisc else 0
        informes = cgr.obtener_informes_destacados()
        n_i = cgr.guardar_informes(informes) if informes else 0
        stats["cgr"] = {"fiscalizaciones": n_f, "informes": n_i}
    except Exception as exc:  # noqa: BLE001
        logger.warning("Error CGR: %s", exc)
        stats["cgr"] = {"error": str(exc)}

    try:
        from infoprobidad_connector import InfoProbidadConnector

        ip = InfoProbidadConnector(DB_NAME)
        stats["infoprobidad"] = {"mode": "live_on_demand", "endpoint": "datos.cplt.cl/sparql"}
    except Exception as exc:  # noqa: BLE001
        logger.warning("Error InfoProbidad: %s", exc)
        stats["infoprobidad"] = {"error": str(exc)}

    try:
        from case_review import init_review_tables

        init_review_tables(DB_NAME)
        stats["case_review"] = {"tables": "ok"}
    except Exception as exc:  # noqa: BLE001
        stats["case_review"] = {"error": str(exc)}

    _write_marker({**marker, **stats})
    return stats


# ─────────────────────────── update_all ────────────────────────────

def update_all(
    fecha: date | None = None,
    *,
    force: bool = False,
    full: bool = False,
) -> dict[str, Any]:
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

        logger.info("[daily_update] inicio - fecha=%s backfill=%s", fecha, BACKFILL_ENABLED)
        stats: dict[str, Any] = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "fecha_objetivo": fecha.isoformat(),
            "backfill_enabled": BACKFILL_ENABLED,
        }

        # Modo agresivo (solo si backfill desactivado): catch-up multi-día
        if full and not BACKFILL_ENABLED:
            try:
                stats["catch_up"] = catch_up_orders()
            except Exception as exc:  # noqa: BLE001
                stats["catch_up"] = {"error": str(exc)}

        budget_left = BACKFILL_OC_BUDGET if BACKFILL_ENABLED else EXTRACT_OC_MAX
        # Reservar parte del presupuesto para el día objetivo (datos frescos)
        fresh_cap = min(200, budget_left) if BACKFILL_ENABLED else budget_left
        try:
            stats["ordenes"] = update_orders(fecha, max_oc=fresh_cap)
            used = int(stats["ordenes"].get("ocs_extraidas") or 0)
            if BACKFILL_ENABLED:
                budget_left = max(0, budget_left - used)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Error update_orders")
            stats["ordenes"] = {"error": str(exc)}

        # Relleno lento del historial (500 OCs / ventana) → BD grande sin 429
        if BACKFILL_ENABLED and budget_left > 0:
            try:
                stats["backfill"] = slow_backfill(budget=budget_left)
            except Exception as exc:  # noqa: BLE001
                stats["backfill"] = {"error": str(exc)}

        try:
            stats["licitaciones"] = update_licitaciones(fecha)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Error update_licitaciones")
            stats["licitaciones"] = {"error": str(exc)}

        if full and not BACKFILL_ENABLED:
            try:
                stats["resync"] = resync_recent_orders()
            except Exception as exc:  # noqa: BLE001
                stats["resync"] = {"error": str(exc)}
            try:
                stats["secondary"] = update_secondary_sources(force=force)
            except Exception as exc:  # noqa: BLE001
                stats["secondary"] = {"error": str(exc)}
        elif full and BACKFILL_ENABLED:
            # En modo lento, fuentes secundarias ocasionales (no cada lote)
            try:
                stats["secondary"] = update_secondary_sources(force=False)
            except Exception as exc:  # noqa: BLE001
                stats["secondary"] = {"error": str(exc)}

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

    # En producción (Railway cron / GitHub Actions) desactivar con DISABLE_STREAMLIT_SCHEDULER=1
    if os.getenv("DISABLE_STREAMLIT_SCHEDULER", "").strip() in ("1", "true", "yes"):
        logger.info("[scheduler] deshabilitado — usar Railway cron o GitHub Actions")
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
    p.add_argument(
        "--full",
        action="store_true",
        help="Pipeline completo: catch-up/backfill + fuentes secundarias",
    )
    p.add_argument(
        "--backfill-only",
        action="store_true",
        help="Solo relleno lento (presupuesto BACKFILL_OC_BUDGET)",
    )
    p.add_argument(
        "--budget",
        type=int,
        default=None,
        help="OCs máximas en esta corrida (override BACKFILL_OC_BUDGET)",
    )
    return p.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )
    args = _parse_cli()
    if args.backfill_only:
        stats = slow_backfill(budget=args.budget)
        print(json.dumps(stats, indent=2, ensure_ascii=False, default=str))
        return
    fecha = None
    if args.fecha:
        fecha = datetime.strptime(args.fecha, "%d%m%Y").date()
    stats = update_all(fecha=fecha, force=args.force, full=args.full)
    print(json.dumps(stats, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
