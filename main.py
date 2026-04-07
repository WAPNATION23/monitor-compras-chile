"""
Monitor de Compras Públicas de Chile — MVP
═══════════════════════════════════════════
Clon chileno de "Operação Serenata de Amor".
Descarga órdenes de compra desde la API de Mercado Público, las almacena en
SQLite y detecta sobreprecios usando métodos estadísticos (IQR + Z-Score)
y modelos forenses inspirados en Rosie (Serenata).

Uso:
    python main.py                   # Procesa OC de ayer
    python main.py --fecha 15032026  # Fecha específica (ddmmaaaa)
    python main.py --solo-analisis   # Solo ejecuta la detección de anomalías
    python main.py --metodo serenata # Ejecuta todos los algoritmos forenses

Requisitos:
    pip install -r requirements.txt
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import date, timedelta

from extractor import MercadoPublicoExtractor
from processor import DataProcessor
from detector import AnomalyDetector
from notifier import TelegramNotifier
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)


# ──────────────────── Configuración de Logging ──────────────────── #

class _JsonFormatter(logging.Formatter):
    """Emite cada registro como una línea JSON con pares clave-valor."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0]:
            entry["exc"] = self.formatException(record.exc_info)
        if hasattr(record, "event"):
            entry["event"] = record.event
        for key, value in record.__dict__.items():
            if key.startswith("_") or key in {
                "args", "asctime", "created", "exc_info", "exc_text", "filename",
                "funcName", "levelname", "levelno", "lineno", "module", "msecs",
                "message", "msg", "name", "pathname", "process", "processName",
                "relativeCreated", "stack_info", "thread", "threadName", "taskName",
                "event",
            }:
                continue
            entry[key] = value
        return json.dumps(entry, ensure_ascii=False)


def _setup_logging(verbose: bool = False, json_fmt: bool = True) -> None:
    """Configura el logging. json_fmt=True da salida JSON estructurada."""
    level: int = logging.DEBUG if verbose else logging.INFO
    handler = logging.StreamHandler()
    if json_fmt:
        handler.setFormatter(_JsonFormatter())
    else:
        fmt = "%(asctime)s │ %(levelname)-8s │ %(name)-25s │ %(message)s"
        handler.setFormatter(logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S"))
    logging.basicConfig(level=level, handlers=[handler])


# ──────────────────── Parsing de argumentos ──────────────────── #

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Monitor Ciudadano de Compras Publicas - Chile",
        epilog="Inspirado en Operação Serenata de Amor (Brasil).",
    )
    parser.add_argument(
        "--fecha",
        type=str,
        default=None,
        help="Fecha de consulta en formato ddmmaaaa. Por defecto: ayer.",
    )
    parser.add_argument(
        "--solo-analisis",
        action="store_true",
        help="Solo ejecutar la detección de anomalías (sin descargar datos nuevos).",
    )
    parser.add_argument(
        "--metodo",
        type=str,
        choices=["iqr", "zscore", "estadistico", "serenata", "all"],
        default="serenata",
        help=(
            "Método de detección: "
            "'iqr' (solo IQR), "
            "'zscore' (solo Z-Score), "
            "'estadistico' (IQR + Z-Score), "
            "'serenata' (todos los algoritmos forenses), "
            "'all' (alias de serenata). "
            "Default: 'serenata'."
        ),
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Activar logging detallado (DEBUG).",
    )
    parser.add_argument(
        "--telegram",
        action="store_true",
        help="Enviar alertas de anomalías a Telegram (requiere config).",
    )
    parser.add_argument(
        "--no-json",
        action="store_true",
        help="Usar formato de log legible en vez de JSON.",
    )
    return parser.parse_args()


# ──────────────────── Pipeline principal ──────────────────── #

def run_pipeline(
    fecha: date,
    solo_analisis: bool = False,
    metodo: str = "serenata",
    notificar_telegram: bool = False,
) -> None:
    """
    Ejecuta el pipeline completo:
        1. Extracción  → API de Mercado Público
        2. Procesamiento → Flatten + SQLite (con deduplicación)
        3. Detección     → Anomalías estadísticas y forenses
        4. Notificación  → Telegram (opcional)
    """

    print("\n" + "═" * 70)
    print("  🇨🇱  MONITOR CIUDADANO DE COMPRAS PÚBLICAS — CHILE")
    print("  Inspirado en Operação Serenata de Amor 🇧🇷")
    print("═" * 70)

    total_oc: int = 0
    total_items: int = 0
    t_start = time.perf_counter()

    if not solo_analisis:
        # ── ETAPA 1: Extracción ──────────────────────────────── #
        print(f"\n📥 Etapa 1: Extrayendo OC de {fecha.strftime('%d/%m/%Y')}...")
        t_extract = time.perf_counter()
        extractor = MercadoPublicoExtractor()
        ordenes_raw = extractor.extract(fecha)
        logger.info(
            "Extracción completada.",
            extra={
                "event": "extraction_complete",
                "duration_s": round(time.perf_counter() - t_extract, 1),
                "ocs": len(ordenes_raw) if ordenes_raw else 0,
            },
        )

        if not ordenes_raw:
            print("⚠  No se encontraron órdenes de compra para esa fecha.")
            print("   Esto puede ocurrir si la API no tiene datos o el ticket expiró.")
            print("   Continuando con la detección de anomalías sobre datos existentes...\n")
        else:
            # ── ETAPA 2: Procesamiento ───────────────────────── #
            print(f"\n🔄 Etapa 2: Procesando {len(ordenes_raw)} órdenes de compra...")
            t_process = time.perf_counter()
            processor = DataProcessor()
            df, inserted = processor.process_and_store(ordenes_raw)
            total_oc = len(ordenes_raw)
            total_items = len(df)
            logger.info(
                "Procesamiento completado.",
                extra={
                    "event": "processing_complete",
                    "duration_s": round(time.perf_counter() - t_process, 1),
                    "items": total_items,
                },
            )

            # Métricas de calidad de datos
            if total_items > 0:
                vacios_producto = df["nombre_producto"].isna().sum() + (df["nombre_producto"] == "").sum()
                vacios_fecha = df["fecha_creacion"].isna().sum() + (df["fecha_creacion"] == "").sum()
                vacios_rut = df["rut_proveedor"].isna().sum() + (df["rut_proveedor"] == "").sum()
                logger.info(
                    "Calidad de datos calculada.",
                    extra={
                        "event": "data_quality",
                        "items": total_items,
                        "empty_product": int(vacios_producto),
                        "empty_date": int(vacios_fecha),
                        "empty_rut": int(vacios_rut),
                        "pct_product": round(100 * vacios_producto / total_items, 1),
                        "pct_date": round(100 * vacios_fecha / total_items, 1),
                        "pct_rut": round(100 * vacios_rut / total_items, 1),
                    },
                )

            print(f"   ✓ {inserted} nuevos ítems almacenados ({total_items} procesados, {total_items - inserted} duplicados omitidos).")

    # ── ETAPA 3: Detección de Anomalías ──────────────────── #
    print(f"\n🔍 Etapa 3: Detectando anomalías (método: {metodo})...\n")
    t_detect = time.perf_counter()
    detector = AnomalyDetector()

    # Ejecutar detect() UNA SOLA VEZ y pasar el resultado a report()
    anomalies = detector.detect(method=metodo)
    detector.report_from_dataframe(anomalies)
    logger.info(
        "Detección completada.",
        extra={
            "event": "detection_complete",
            "duration_s": round(time.perf_counter() - t_detect, 1),
            "anomalies": len(anomalies),
        },
    )
    logger.info(
        "Pipeline completado.",
        extra={
            "event": "pipeline_complete",
            "duration_s": round(time.perf_counter() - t_start, 1),
            "ocs": total_oc,
            "items": total_items,
            "anomalies": len(anomalies),
        },
    )

    # ── ETAPA 4: Notificación a Telegram (opcional) ──────── #
    if notificar_telegram:
        print("\n📲 Etapa 4: Enviando alertas a Telegram...")

        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            print("   ⚠ Telegram no configurado.")
            print("   Configura las variables de entorno TELEGRAM_TOKEN y TELEGRAM_CHAT_ID.")
            print("   (O usa: python obtener_chat_id.py para obtener tu chat_id)")
            return

        try:
            tg = TelegramNotifier()
        except ValueError as exc:
            print(f"   ⚠ Telegram no configurado: {exc}")
            return

        # Enviar cada anomalía como alerta individual (con anti-spam)
        enviadas: int = 0
        omitidas: int = 0
        for _, row in anomalies.iterrows():
            try:
                result = tg.enviar_alerta_desfalco(
                    producto=row.get("nombre_producto", "N/A"),
                    comprador=row.get("nombre_comprador", "N/A"),
                    precio_pagado=float(row.get("precio_unitario", 0)),
                    precio_promedio=float(row.get("mediana", row.get("q3", 0))),
                    z_score=float(row.get("z_score", 0)),
                    link_orden=str(row.get("codigo_oc", "")),
                    categoria_riesgo=str(row.get("categoria_riesgo", "GENERAL")),
                )
                if result is not None:
                    enviadas += 1
                else:
                    omitidas += 1
            except Exception as exc:
                logger.warning("Error enviando alerta para %s: %s", row.get("codigo_oc"), exc)

        # Enviar resumen diario
        try:
            tg.enviar_resumen_diario(
                fecha=fecha.strftime("%d/%m/%Y"),
                total_oc=total_oc,
                total_items=total_items,
                total_anomalias=len(anomalies),
                alertas_enviadas=enviadas,
                alertas_omitidas=omitidas,
            )
        except Exception as exc:
            logger.warning("Error enviando resumen diario: %s", exc)

        print(f"   ✓ {enviadas} alertas enviadas a Telegram ({omitidas} omitidas por anti-spam).")


# ──────────────────── Punto de entrada ──────────────────── #

if __name__ == "__main__":
    args: argparse.Namespace = _parse_args()
    _setup_logging(verbose=args.verbose, json_fmt=not args.no_json)

    # Determinar la fecha de consulta
    if args.fecha:
        try:
            # Parsear formato ddmmaaaa
            dia: int = int(args.fecha[:2])
            mes: int = int(args.fecha[2:4])
            anio: int = int(args.fecha[4:])
            target_date: date = date(anio, mes, dia)
        except (ValueError, IndexError) as exc:
            print(f"❌ Formato de fecha inválido: '{args.fecha}'. Use ddmmaaaa.")
            sys.exit(1)
    else:
        # Por defecto: ayer
        target_date = date.today() - timedelta(days=1)

    try:
        run_pipeline(
            fecha=target_date,
            solo_analisis=args.solo_analisis,
            metodo=args.metodo,
            notificar_telegram=args.telegram,
        )
    except KeyboardInterrupt:
        print("\n\n🛑 Ejecución interrumpida por el usuario.")
        sys.exit(130)
    except Exception as exc:
        logging.getLogger(__name__).exception("Error fatal en el pipeline: %s", exc)
        sys.exit(1)
