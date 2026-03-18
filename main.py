"""
Monitor de Compras Públicas de Chile — MVP
═══════════════════════════════════════════
Clon chileno de "Operação Serenata de Amor".
Descarga órdenes de compra desde la API de Mercado Público, las almacena en
SQLite y detecta sobreprecios usando métodos estadísticos (IQR + Z-Score).

Uso:
    python main.py                   # Procesa OC de ayer
    python main.py --fecha 15032026  # Fecha específica (ddmmaaaa)
    python main.py --solo-analisis   # Solo ejecuta la detección de anomalías

Requisitos:
    pip install requests pandas numpy
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta

from extractor import MercadoPublicoExtractor
from processor import DataProcessor
from detector import AnomalyDetector
from notifier import TelegramNotifier
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)


# ──────────────────── Configuración de Logging ──────────────────── #

def _setup_logging(verbose: bool = False) -> None:
    """Configura el logging con formato legible."""
    level: int = logging.DEBUG if verbose else logging.INFO
    fmt: str = "%(asctime)s │ %(levelname)-8s │ %(name)-25s │ %(message)s"
    logging.basicConfig(level=level, format=fmt, datefmt="%Y-%m-%d %H:%M:%S")


# ──────────────────── Parsing de argumentos ──────────────────── #

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="🇨🇱 Monitor Ciudadano de Compras Públicas — Chile",
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
        choices=["iqr", "zscore", "both"],
        default="both",
        help="Método de detección: 'iqr', 'zscore', o 'both'. Default: 'both'.",
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
    return parser.parse_args()


# ──────────────────── Pipeline principal ──────────────────── #

def run_pipeline(
    fecha: date,
    solo_analisis: bool = False,
    metodo: str = "both",
    notificar_telegram: bool = False,
) -> None:
    """
    Ejecuta el pipeline completo:
        1. Extracción  → API de Mercado Público
        2. Procesamiento → Flatten + SQLite
        3. Detección     → Anomalías estadísticas
    """

    print("\n" + "═" * 70)
    print("  🇨🇱  MONITOR CIUDADANO DE COMPRAS PÚBLICAS — CHILE")
    print("  Inspirado en Operação Serenata de Amor 🇧🇷")
    print("═" * 70)

    total_oc: int = 0
    total_items: int = 0

    if not solo_analisis:
        # ── ETAPA 1: Extracción ──────────────────────────────── #
        print(f"\n📥 Etapa 1: Extrayendo OC de {fecha.strftime('%d/%m/%Y')}...")
        extractor = MercadoPublicoExtractor()
        ordenes_raw = extractor.extract(fecha)

        if not ordenes_raw:
            print("⚠  No se encontraron órdenes de compra para esa fecha.")
            print("   Esto puede ocurrir si la API no tiene datos o el ticket expiró.")
            print("   Continuando con la detección de anomalías sobre datos existentes...\n")
        else:
            # ── ETAPA 2: Procesamiento ───────────────────────── #
            print(f"\n🔄 Etapa 2: Procesando {len(ordenes_raw)} órdenes de compra...")
            processor = DataProcessor()
            df = processor.process_and_store(ordenes_raw)
            total_oc = len(ordenes_raw)
            total_items = len(df)
            print(f"   ✓ {total_items} ítems almacenados en la base de datos.")

    # ── ETAPA 3: Detección de Anomalías ──────────────────── #
    print(f"\n🔍 Etapa 3: Detectando anomalías (método: {metodo})...\n")
    detector = AnomalyDetector()
    anomalies = detector.detect(method=metodo)
    detector.report(method=metodo)

    # ── ETAPA 4: Notificación a Telegram (opcional) ──────── #
    if notificar_telegram:
        print("\n📲 Etapa 4: Enviando alertas a Telegram...")
        try:
            tg = TelegramNotifier()
        except ValueError as exc:
            print(f"   ⚠ Telegram no configurado: {exc}")
            print("   Configura TELEGRAM_BOT_TOKEN y TELEGRAM_CHAT_ID en config.py.")
            return

        # Enviar cada anomalía como alerta individual
        enviadas: int = 0
        for _, row in anomalies.iterrows():
            try:
                tg.enviar_alerta_desfalco(
                    producto=row.get("nombre_producto", "N/A"),
                    precio_pagado=float(row.get("precio_unitario", 0)),
                    precio_promedio=float(row.get("mediana", row.get("q3", 0))),
                    z_score=float(row.get("z_score", 0)),
                    link_orden=str(row.get("codigo_oc", "")),
                )
                enviadas += 1
            except Exception as exc:
                logger.warning("Error enviando alerta para %s: %s", row.get("codigo_oc"), exc)

        # Enviar resumen diario
        try:
            tg.enviar_resumen_diario(
                fecha=fecha.strftime("%d/%m/%Y"),
                total_oc=total_oc,
                total_items=total_items,
                total_anomalias=len(anomalies),
            )
        except Exception as exc:
            logger.warning("Error enviando resumen diario: %s", exc)

        print(f"   ✓ {enviadas} alertas enviadas a Telegram.")


# ──────────────────── Punto de entrada ──────────────────── #

if __name__ == "__main__":
    args: argparse.Namespace = _parse_args()
    _setup_logging(verbose=args.verbose)

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
