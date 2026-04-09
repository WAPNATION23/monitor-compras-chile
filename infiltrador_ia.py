import argparse
import logging
import sqlite3

import requests

from extractor import MercadoPublicoExtractor
from processor import DataProcessor
from config import API_BASE_URL, API_TICKET

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def infiltrar_rut(rut_proveedor: str):
    logger.info("🦸‍♂️ [OPERACION INFILTRACION] Solicitada por I.A para RUT: %s", rut_proveedor)

    extractor = MercadoPublicoExtractor()
    processor = DataProcessor()

    # 1. Consultar a la API por RUT de Proveedor
    params = {
        "rutProveedor": rut_proveedor,
        "ticket": API_TICKET
    }

    try:
        logger.info("📡 Conectando a Base de Datos de Mercado Publico...")
        data = extractor._get_with_retry(API_BASE_URL, params)
        listado = data.get("Listado", [])

        if not listado:
            logger.info("❌ No se encontraron contratos bajo este RUT en los registros accesibles via API rutProveedor.")
            return 0

        logger.info("✅ Se encontraron %d Ordenes de Compra asociadas al RUT. Descargando detalle...", len(listado))

        # 2. Descargar detalle e inyectar
        detalles = []
        for oc in listado[:50]:  # Top 50 max allowed for stealth
            if "Codigo" in oc:
                det = extractor._fetch_oc_detail(oc["Codigo"])
                if det:
                    detalles.append(det)

        logger.info("💾 Inyectando %d expedientes a la base de datos local...", len(detalles))
        df, inserted = processor.process_and_store(detalles)
        logger.info("✓ %d nuevos ítems almacenados (%d procesados, %d duplicados omitidos).", inserted, len(df), len(df) - inserted)

        return inserted

    except (requests.exceptions.RequestException, sqlite3.Error, KeyError, ValueError) as e:
        logger.error("Fallo en la infiltracion: %s", e)
        return -1

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Infiltrador Histórico por RUT")
    parser.add_argument("rut", help="RUT del proveedor a infiltrar (ej. 76.111.222-3)")
    args = parser.parse_args()

    infiltrar_rut(args.rut)
