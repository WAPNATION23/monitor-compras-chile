"""Descarga y carga masiva de aportes SERVEL publicos (XLSX).

Fuentes: archivos XLSX publicados por SERVEL en su WordPress media library.
Los archivos no incluyen RUT del aportante (SERVEL no lo publica), pero
incluyen nombre completo -> suficiente para cruces con proveedores.
"""
from __future__ import annotations

import io
import logging
import sqlite3
import urllib3
from datetime import datetime

import pandas as pd
import requests

from config import DB_NAME

urllib3.disable_warnings()
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# Archivos publicos identificados via WP REST API de servel.cl
SOURCES = [
    {
        "label": "Aportes 2017 (Presidenciales + Parlamentarias)",
        "url": "https://www.servel.cl/wp-content/uploads/2022/12/Estadistica_de_Aportes_Privados_Aceptados_al_2017_11_17.xlsx",
        "header_row": 7,  # row 0-indexed donde están los headers
        "eleccion": "ELECCIONES 2017",
    },
]


def _normalize_name(s) -> str:
    if pd.isna(s) or s is None:
        return ""
    return str(s).strip().upper().replace("\n", " ").replace("  ", " ")


def load_source(src: dict) -> pd.DataFrame:
    logger.info("Descargando %s", src["label"])
    r = requests.get(src["url"], timeout=120, verify=False,
                     headers={"User-Agent": "Mozilla/5.0 AuditoriaChile"})
    r.raise_for_status()
    df_raw = pd.read_excel(io.BytesIO(r.content), header=src["header_row"])
    # Normalizar headers
    df_raw.columns = [str(c).strip().upper().replace("\n", " ") for c in df_raw.columns]
    logger.info("  columnas: %s", list(df_raw.columns))
    logger.info("  filas brutas: %d", len(df_raw))

    # Mapear al schema aportes_servel
    col_map = {
        "nombre_aportante": next((c for c in df_raw.columns if "NOMBRE APORTANTE" in c), None),
        "tipo_aportante": next((c for c in df_raw.columns if "TIPO APORTANTE" in c), None),
        "nombre_receptor": next((c for c in df_raw.columns if "CANDIDATO" in c and "NOMBRE" in c), None),
        "tipo_receptor": next((c for c in df_raw.columns if "TIPO DONATARIO" in c or "TIPO RECEPTOR" in c), None),
        "monto": next((c for c in df_raw.columns if c == "MONTO" or "MONTO" in c), None),
        "fecha": next((c for c in df_raw.columns if "FECHA" in c), None),
        "eleccion": next((c for c in df_raw.columns if "ELECCION" in c), None),
    }
    logger.info("  col_map: %s", col_map)
    if not col_map["nombre_aportante"] or not col_map["monto"]:
        logger.error("  Faltan columnas obligatorias en %s", src["label"])
        return pd.DataFrame()

    out = pd.DataFrame({
        "rut_aportante": "",
        "nombre_aportante": df_raw[col_map["nombre_aportante"]].apply(_normalize_name),
        "rut_receptor": "",
        "nombre_receptor": df_raw[col_map["nombre_receptor"]].apply(_normalize_name) if col_map["nombre_receptor"] else "",
        "tipo_receptor": df_raw[col_map["tipo_receptor"]].apply(_normalize_name) if col_map["tipo_receptor"] else "CANDIDATO",
        "monto_aporte": pd.to_numeric(df_raw[col_map["monto"]], errors="coerce").fillna(0),
        "fecha_aporte": pd.to_datetime(df_raw[col_map["fecha"]], errors="coerce") if col_map["fecha"] else pd.NaT,
        "eleccion_campaña": df_raw[col_map["eleccion"]].apply(_normalize_name) if col_map["eleccion"] else src["eleccion"],
    })
    out = out[(out["nombre_aportante"] != "") & (out["monto_aporte"] > 0)]
    logger.info("  filas validas: %d", len(out))
    return out


def save_to_db(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS aportes_servel (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rut_aportante TEXT, nombre_aportante TEXT,
                rut_receptor TEXT, nombre_receptor TEXT,
                tipo_receptor TEXT, monto_aporte REAL,
                fecha_aporte TIMESTAMP, eleccion_campaña TEXT,
                UNIQUE(rut_aportante, nombre_aportante, rut_receptor, nombre_receptor, monto_aporte, fecha_aporte)
            )
        """)
        # Indices para cruces rapidos
        conn.execute("CREATE INDEX IF NOT EXISTS idx_aportante_nombre ON aportes_servel(nombre_aportante)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_receptor_nombre ON aportes_servel(nombre_receptor)")
        conn.commit()

        before = conn.execute("SELECT COUNT(*) FROM aportes_servel").fetchone()[0]
        inserted = 0
        for _, row in df.iterrows():
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO aportes_servel
                    (rut_aportante, nombre_aportante, rut_receptor, nombre_receptor,
                     tipo_receptor, monto_aporte, fecha_aporte, eleccion_campaña)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row["rut_aportante"], row["nombre_aportante"],
                    row["rut_receptor"], row["nombre_receptor"],
                    row["tipo_receptor"], float(row["monto_aporte"]),
                    str(row["fecha_aporte"]) if pd.notna(row["fecha_aporte"]) else None,
                    row["eleccion_campaña"],
                ))
                inserted += 1
            except sqlite3.IntegrityError:
                pass
        conn.commit()
        after = conn.execute("SELECT COUNT(*) FROM aportes_servel").fetchone()[0]
        return after - before


def main():
    total_inserted = 0
    for src in SOURCES:
        try:
            df = load_source(src)
            n = save_to_db(df)
            logger.info("  -> %d nuevos aportes insertados", n)
            total_inserted += n
        except Exception as exc:
            logger.error("Fallo %s: %s", src["label"], exc)
    logger.info("═══ TOTAL: %d aportes cargados en BD ═══", total_inserted)


if __name__ == "__main__":
    main()
