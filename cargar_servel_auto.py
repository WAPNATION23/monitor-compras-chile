"""Descarga y carga masiva de aportes SERVEL publicos (XLSX).

Fuentes: archivos XLSX publicados por SERVEL en su WordPress media library.
Los archivos no incluyen RUT del aportante (SERVEL no lo publica), pero
incluyen nombre completo -> suficiente para cruces con proveedores.

El loader auto-detecta el header row escaneando las primeras 15 filas
en busca de columnas con 'APORTANTE' o 'CANDIDATO'.
"""
from __future__ import annotations

import io
import logging
import sqlite3

import pandas as pd
import requests

from config import DB_NAME

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# Archivos publicos transaccionales (ingresos/aportes) de servel.cl
SOURCES = [
    ("Aportes 2017 Acep. 17-Nov",
     "https://www.servel.cl/wp-content/uploads/2022/12/Estadistica_de_Aportes_Privados_Aceptados_al_2017_11_17.xlsx"),
    ("Ingresos Pres+Parl 2017 (14-sep-2018)",
     "https://www.servel.cl/wp-content/uploads/2022/12/Ingresos_Presidencial_y_Parlamentarias_2017._Actualizado_al_14_de_septiembre_de_2018.xlsx"),
    ("Ingresos Primarias 2013",
     "https://www.servel.cl/wp-content/uploads/2022/12/Ingresos_Primarias_2013.xlsx"),
    ("Ingresos Primarias 2016",
     "https://www.servel.cl/wp-content/uploads/2022/12/Ingresos_Primarias_2016-1.xlsx"),
    ("Ingresos Primarias 2017 (29-sep-2017)",
     "https://www.servel.cl/wp-content/uploads/2022/12/Ingresos_Primarias_2017_al_2017_09_29-1-1-1.xlsx"),
    ("Ingresos Primarias 2020 Candidatos",
     "https://www.servel.cl/wp-content/uploads/2022/12/01_Reporte_Ingresos_Primarias_2020_Candidatos-1.xlsx"),
    ("Ingresos Primarias 2020 Partidos",
     "https://www.servel.cl/wp-content/uploads/2022/12/03_Reporte_Ingresos_Primarias_2020_Partidos.xlsx"),
    ("Ingresos Pres 2005 (2da vuelta)",
     "https://www.servel.cl/wp-content/uploads/2022/12/ingresos_elecciones_2005_2da_votacion_presidencial.xls"),
    ("Ingresos Pres 2009 (2da vuelta)",
     "https://www.servel.cl/wp-content/uploads/2022/12/ingresos_elecciones_2009_2da_votacion_presidencial.xls"),
    ("Ingresos+Gastos Primarias 2024",
     "https://www.servel.cl/wp-content/uploads/2024/08/Reporte_Ingresos_Gastos_Primarias2024.xlsx"),
    ("Ingresos+Gastos Primarias Pres 2025",
     "https://www.servel.cl/wp-content/uploads/2025/09/Reporte_Ingresos_Gastos_Primarias_Presidenciales_2025.xlsx"),
]


def _normalize_name(s) -> str:
    if pd.isna(s) or s is None:
        return ""
    return " ".join(str(s).strip().upper().replace("\n", " ").split())


def _find_header_row(xl: pd.ExcelFile, sheet: str) -> int | None:
    for skip in range(0, 15):
        try:
            df = pd.read_excel(xl, sheet_name=sheet, header=skip, nrows=1)
        except Exception:
            continue
        cols_up = [str(c).upper() for c in df.columns]
        has_apt = any("APORTANTE" in c or "DONANTE" in c for c in cols_up)
        has_monto = any("MONTO" in c for c in cols_up)
        if has_apt and has_monto:
            return skip
    return None


def load_source(label: str, url: str) -> pd.DataFrame:
    logger.info("Descargando %s", label)
    try:
        r = requests.get(url, timeout=120,
                         headers={"User-Agent": "Mozilla/5.0 AuditoriaChile"})
        r.raise_for_status()
    except Exception as exc:
        logger.warning("  descarga fallo: %s", exc)
        return pd.DataFrame()

    frames = []
    try:
        xl = pd.ExcelFile(io.BytesIO(r.content))
    except Exception as exc:
        logger.warning("  no es excel valido: %s", exc)
        return pd.DataFrame()

    for sh in xl.sheet_names:
        hdr = _find_header_row(xl, sh)
        if hdr is None:
            continue
        try:
            df_raw = pd.read_excel(xl, sheet_name=sh, header=hdr)
        except Exception:
            continue
        df_raw.columns = [str(c).strip().upper().replace("\n", " ") for c in df_raw.columns]

        col_apt = next((c for c in df_raw.columns if "APORTANTE" in c and "NOMBRE" in c), None) \
            or next((c for c in df_raw.columns if "APORTANTE" in c), None) \
            or next((c for c in df_raw.columns if "DONANTE" in c), None)
        col_rec = next((c for c in df_raw.columns if ("CANDIDATO" in c or "RECEPTOR" in c or "DONATARIO" in c) and "NOMBRE" in c), None) \
            or next((c for c in df_raw.columns if "CANDIDATO" in c), None) \
            or next((c for c in df_raw.columns if "DONATARIO" in c), None) \
            or next((c for c in df_raw.columns if "RECEPTOR" in c), None)
        col_tipo_rec = next((c for c in df_raw.columns if "TIPO DONATARIO" in c or "TIPO RECEPTOR" in c), None)
        col_monto = next((c for c in df_raw.columns if c == "MONTO"), None) \
            or next((c for c in df_raw.columns if "MONTO" in c), None)
        col_fecha = next((c for c in df_raw.columns if "FECHA" in c), None)
        col_eleccion = next((c for c in df_raw.columns if "ELECCION" in c or "ELECCIÓN" in c), None)

        if not col_apt or not col_monto:
            continue

        df = pd.DataFrame({
            "rut_aportante": "",
            "nombre_aportante": df_raw[col_apt].apply(_normalize_name),
            "rut_receptor": "",
            "nombre_receptor": df_raw[col_rec].apply(_normalize_name) if col_rec else "",
            "tipo_receptor": df_raw[col_tipo_rec].apply(_normalize_name) if col_tipo_rec else "",
            "monto_aporte": pd.to_numeric(df_raw[col_monto], errors="coerce").fillna(0),
            "fecha_aporte": pd.to_datetime(df_raw[col_fecha], errors="coerce") if col_fecha else pd.NaT,
            "eleccion_campaña": df_raw[col_eleccion].apply(_normalize_name) if col_eleccion else label.upper(),
        })
        df = df[(df["nombre_aportante"] != "") & (df["monto_aporte"] > 0)]
        if not df.empty:
            logger.info("  sheet %r hdr=%d -> %d filas validas", sh, hdr, len(df))
            frames.append(df)

    if not frames:
        logger.warning("  sin filas validas en %s", label)
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


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
        conn.execute("CREATE INDEX IF NOT EXISTS idx_aportante_nombre ON aportes_servel(nombre_aportante)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_receptor_nombre ON aportes_servel(nombre_receptor)")
        conn.commit()

        before = conn.execute("SELECT COUNT(*) FROM aportes_servel").fetchone()[0]
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
            except sqlite3.IntegrityError:
                pass
        conn.commit()
        after = conn.execute("SELECT COUNT(*) FROM aportes_servel").fetchone()[0]
        return after - before


def main():
    total_inserted = 0
    for label, url in SOURCES:
        try:
            df = load_source(label, url)
            n = save_to_db(df)
            logger.info("  -> +%d nuevos (%s)", n, label)
            total_inserted += n
        except Exception as exc:
            logger.error("Fallo %s: %s", label, exc)
    with sqlite3.connect(DB_NAME) as conn:
        total = conn.execute("SELECT COUNT(*) FROM aportes_servel").fetchone()[0]
    logger.info("═══ +%d nuevos | TOTAL BD: %d aportes ═══", total_inserted, total)
    build_cruce_table()


def build_cruce_table():
    """Pre-computa cruce aportantes SERVEL × proveedores del Estado.

    El JOIN LIKE en vivo es O(N*M) (13k × 54k) -> demasiado lento para dashboard.
    Aquí lo hacemos una vez en memoria con sets de Python y guardamos resultado.
    Filtra aportantes que suenan a empresa (contienen SA/LTDA/SPA/etc) para
    reducir falsos positivos con personas naturales homonimas.
    """
    import re
    logger.info("Construyendo tabla cruce_aportes_proveedores…")
    suffix_empresa = re.compile(
        r"\b(S\.?A\.?|LTDA|LIMITADA|SPA|S\.?P\.?A\.?|E\.?I\.?R\.?L\.?|"
        r"CIA|COMPAÑIA|COMPAÑÍA|INMOBILIARIA|CONSTRUCTORA|SERVICIOS|"
        r"EMPRESA|SOCIEDAD|HOLDING|CORP|COMERCIAL|INGENIERIA|INGENIERÍA|"
        r"CONSULTORES|CONSULTORA|ASESORIAS|ASESORÍAS|TRANSPORTES|"
        r"DISTRIBUIDORA|IMPORTADORA|AGRICOLA|AGRÍCOLA|CLINICA|CLÍNICA)\b",
        re.IGNORECASE,
    )
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("DROP TABLE IF EXISTS cruce_aportes_proveedores")
        conn.execute("""
            CREATE TABLE cruce_aportes_proveedores (
                nombre_aportante TEXT PRIMARY KEY,
                n_aportes INTEGER,
                total_donado REAL,
                receptores TEXT,
                n_ocs INTEGER,
                total_ocs REAL,
                rut_proveedor TEXT,
                nombre_proveedor_match TEXT
            )
        """)

        aportantes = conn.execute("""
            SELECT nombre_aportante, COUNT(*), SUM(monto_aporte),
                   GROUP_CONCAT(DISTINCT nombre_receptor)
            FROM aportes_servel
            WHERE nombre_aportante != '' AND LENGTH(nombre_aportante) >= 8
            GROUP BY nombre_aportante
        """).fetchall()
        logger.info("  aportantes únicos: %d", len(aportantes))

        # Filtrar: solo aportantes que parecen empresas (los que tienden a ser proveedores)
        candidatos = [a for a in aportantes if suffix_empresa.search(a[0])]
        logger.info("  aportantes tipo empresa: %d", len(candidatos))

        # Cargar proveedores únicos una sola vez
        provs = conn.execute("""
            SELECT DISTINCT rut_proveedor, UPPER(nombre_proveedor), nombre_proveedor
            FROM ordenes_items
            WHERE nombre_proveedor IS NOT NULL AND LENGTH(nombre_proveedor) >= 5
        """).fetchall()
        logger.info("  proveedores únicos: %d", len(provs))

        # Index por primera palabra significativa para acelerar
        insertados = 0
        for nom_apt, n_apt, tot_apt, receptores in candidatos:
            # Buscar proveedor cuyo nombre coincide (substring en cualquier direccion)
            nom_apt_clean = nom_apt.strip()
            match_rut = match_nom = None
            for rut_p, nom_p_up, nom_p in provs:
                if nom_apt_clean in nom_p_up or nom_p_up in nom_apt_clean:
                    match_rut, match_nom = rut_p, nom_p
                    break
            if not match_nom:
                continue
            # Contar OCs del proveedor
            row = conn.execute("""
                SELECT COUNT(DISTINCT id), SUM(monto_total_item)
                FROM ordenes_items
                WHERE UPPER(nombre_proveedor) = ?
            """, (nom_p_up,)).fetchone()
            n_ocs, tot_ocs = row[0] or 0, row[1] or 0
            if n_ocs == 0:
                continue
            conn.execute("""
                INSERT OR REPLACE INTO cruce_aportes_proveedores
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (nom_apt_clean, n_apt, tot_apt, receptores, n_ocs, tot_ocs, match_rut, match_nom))
            insertados += 1
        conn.commit()
        logger.info("  ✅ %d cruces detectados en cruce_aportes_proveedores", insertados)


if __name__ == "__main__":
    main()

