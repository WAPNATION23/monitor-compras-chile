"""Carga gastos de campana SERVEL (proveedores que facturaron a candidatos).

A diferencia de los aportes, los gastos SI incluyen RUT + DV del proveedor,
lo que permite cruces EXACTOS con proveedores del Estado (ordenes_items.rut_proveedor).

Este es el cruce forense mas potente: empresas que facturaron a una campana
y luego recibieron OCs del Estado cuando ese candidato gano.
"""
from __future__ import annotations

import io
import logging
import re
import sqlite3

import pandas as pd
import requests

from config import DB_NAME

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

SOURCES_GASTOS = [
    ("Gastos Pres+Parl 2017",
     "https://www.servel.cl/wp-content/uploads/2022/12/Gastos_Presidencial_y_Parlamentarias_2017._Actualizado_al_14_de_septiembre_de_2018-1.xlsx"),
    ("Gastos Primarias 2013",
     "https://www.servel.cl/wp-content/uploads/2022/12/Gastos_Primarias_2013.xlsx"),
    ("Gastos Primarias 2016",
     "https://www.servel.cl/wp-content/uploads/2022/12/Gastos_Primarias_2016-1.xlsx"),
    ("Gastos Primarias 2017",
     "https://www.servel.cl/wp-content/uploads/2022/12/Gastos_Primarias_2017_al_2017_09_29-1.xlsx"),
    ("Gastos Primarias 2020 Candidatos",
     "https://www.servel.cl/wp-content/uploads/2022/12/02_Reporte_Gastos_Primarias_2020_Candidatos-1.xlsx"),
    ("Gastos Primarias 2020 Partidos",
     "https://www.servel.cl/wp-content/uploads/2022/12/04_Reporte_Gastos_Primarias_2020_Partidos.xlsx"),
    ("Gastos+Ingresos Primarias 2024",
     "https://www.servel.cl/wp-content/uploads/2024/08/Reporte_Ingresos_Gastos_Primarias2024.xlsx"),
    ("Gastos+Ingresos Primarias Pres 2025",
     "https://www.servel.cl/wp-content/uploads/2025/09/Reporte_Ingresos_Gastos_Primarias_Presidenciales_2025.xlsx"),
]


def _normalize(s) -> str:
    if pd.isna(s) or s is None:
        return ""
    return " ".join(str(s).strip().upper().replace("\n", " ").split())


def _format_rut(run, dv) -> str:
    if pd.isna(run) or run is None or run == "":
        return ""
    try:
        num = int(float(run))
    except Exception:
        return ""
    dv_s = str(dv).strip().upper() if not pd.isna(dv) else ""
    if num <= 0:
        return ""
    return f"{num}-{dv_s}" if dv_s else str(num)


def _find_hdr(xl: pd.ExcelFile, sheet: str) -> int | None:
    for skip in range(0, 15):
        try:
            df = pd.read_excel(xl, sheet_name=sheet, header=skip, nrows=1)
        except Exception:
            continue
        cols = [str(c).upper() for c in df.columns]
        has_prov = any("PROVEEDOR" in c for c in cols)
        has_monto = any("MONTO" in c for c in cols)
        if has_prov and has_monto:
            return skip
    return None


def load_gastos(label: str, url: str) -> pd.DataFrame:
    logger.info("Descargando %s", label)
    try:
        r = requests.get(url, timeout=120,
                         headers={"User-Agent": "Mozilla/5.0 AuditoriaChile"})
        r.raise_for_status()
        xl = pd.ExcelFile(io.BytesIO(r.content))
    except Exception as exc:
        logger.warning("  fallo: %s", exc)
        return pd.DataFrame()

    frames = []
    for sh in xl.sheet_names:
        if "gasto" not in sh.lower() and "planilla" not in sh.lower() and sh.lower() != "sheet1":
            # Solo hojas de gastos (o genericas)
            if sh.lower() not in ("h1", "hoja1", "sheet 1"):
                continue
        hdr = _find_hdr(xl, sh)
        if hdr is None:
            continue
        try:
            df_raw = pd.read_excel(xl, sheet_name=sh, header=hdr)
        except Exception:
            continue
        df_raw.columns = [str(c).strip().upper().replace("\n", " ") for c in df_raw.columns]

        col_rut = next((c for c in df_raw.columns if "RUT O RUN DEL PROVEEDOR" in c or "RUT PROVEEDOR" in c or "RUN PROVEEDOR" in c), None)
        col_dv = next((c for c in df_raw.columns if c in ("DV.1", "DV DEL PROVEEDOR", "DV PROVEEDOR")), None)
        col_nom_prov = next((c for c in df_raw.columns if c == "NOMBRE PROVEEDOR"), None) \
            or next((c for c in df_raw.columns if "PROVEEDOR" in c and "NOMBRE" in c), None)
        col_run_cand = next((c for c in df_raw.columns if "RUN CANDIDATO" in c or "RUT CANDIDATO" in c), None)
        col_dv_cand = next((c for c in df_raw.columns if c == "DV"), None)
        col_nom_cand = next((c for c in df_raw.columns if "NOMBRE DEL CANDIDATO" in c or "NOMBRE CANDIDATO" in c), None)
        col_partido = next((c for c in df_raw.columns if "NOMBRE PARTIDO" in c or c == "PARTIDO"), None)
        col_monto = next((c for c in df_raw.columns if c == "MONTO"), None)
        col_fecha = next((c for c in df_raw.columns if "FECHA DOCUMENTO" in c or c == "FECHA"), None)
        col_tipo = next((c for c in df_raw.columns if "DESCRIPCION T/C" in c or "TIPO CUENTA" in c or "DESCRIPCIËN T/C" in c), None)
        col_glosa = next((c for c in df_raw.columns if "GLOSA" in c), None)
        col_eleccion = next((c for c in df_raw.columns if c in ("ELECCION", "ELECCIËN", "ELECCIÓN")), None)

        if not col_nom_prov or not col_monto:
            continue

        df = pd.DataFrame({
            "rut_proveedor": df_raw.apply(lambda r: _format_rut(r[col_rut], r[col_dv]) if col_rut else "", axis=1),
            "nombre_proveedor": df_raw[col_nom_prov].apply(_normalize),
            "rut_candidato": df_raw.apply(lambda r: _format_rut(r[col_run_cand], r[col_dv_cand]) if col_run_cand and col_dv_cand else "", axis=1),
            "nombre_candidato": df_raw[col_nom_cand].apply(_normalize) if col_nom_cand else "",
            "partido": df_raw[col_partido].apply(_normalize) if col_partido else "",
            "monto": pd.to_numeric(df_raw[col_monto], errors="coerce").fillna(0),
            "fecha": pd.to_datetime(df_raw[col_fecha], errors="coerce", dayfirst=True) if col_fecha else pd.NaT,
            "tipo_gasto": df_raw[col_tipo].apply(_normalize) if col_tipo else "",
            "glosa": df_raw[col_glosa].apply(_normalize) if col_glosa else "",
            "eleccion": df_raw[col_eleccion].apply(_normalize) if col_eleccion else label.upper(),
        })
        df = df[(df["nombre_proveedor"] != "") & (df["monto"] > 0)]
        if not df.empty:
            logger.info("  sheet %r hdr=%d -> %d filas", sh, hdr, len(df))
            frames.append(df)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def save_gastos(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gastos_servel (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rut_proveedor TEXT, nombre_proveedor TEXT,
                rut_candidato TEXT, nombre_candidato TEXT,
                partido TEXT, monto REAL,
                fecha TIMESTAMP, tipo_gasto TEXT, glosa TEXT, eleccion TEXT,
                UNIQUE(rut_proveedor, nombre_proveedor, rut_candidato, monto, fecha, glosa)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_gastos_rut_prov ON gastos_servel(rut_proveedor)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_gastos_nom_prov ON gastos_servel(nombre_proveedor)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_gastos_candidato ON gastos_servel(nombre_candidato)")
        conn.commit()
        before = conn.execute("SELECT COUNT(*) FROM gastos_servel").fetchone()[0]
        for _, r in df.iterrows():
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO gastos_servel
                    (rut_proveedor, nombre_proveedor, rut_candidato, nombre_candidato,
                     partido, monto, fecha, tipo_gasto, glosa, eleccion)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (r["rut_proveedor"], r["nombre_proveedor"], r["rut_candidato"],
                      r["nombre_candidato"], r["partido"], float(r["monto"]),
                      str(r["fecha"]) if pd.notna(r["fecha"]) else None,
                      r["tipo_gasto"], r["glosa"], r["eleccion"]))
            except sqlite3.IntegrityError:
                pass
        conn.commit()
        after = conn.execute("SELECT COUNT(*) FROM gastos_servel").fetchone()[0]
        return after - before


def build_cruce_gastos():
    """Cruce por RUT EXACTO entre gastos_servel.rut_proveedor y ordenes_items.rut_proveedor."""
    logger.info("Construyendo cruce_gastos_proveedores (JOIN por RUT exacto)…")
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("DROP TABLE IF EXISTS cruce_gastos_proveedores")
        conn.execute("""
            CREATE TABLE cruce_gastos_proveedores AS
            SELECT
                g.rut_proveedor AS rut,
                MAX(g.nombre_proveedor) AS nombre_proveedor,
                COUNT(DISTINCT g.id) AS n_facturas_campana,
                ROUND(SUM(g.monto)) AS total_facturado_campana,
                GROUP_CONCAT(DISTINCT g.nombre_candidato) AS candidatos_beneficiados,
                GROUP_CONCAT(DISTINCT g.partido) AS partidos,
                (SELECT COUNT(DISTINCT o.id) FROM ordenes_items o WHERE o.rut_proveedor = g.rut_proveedor) AS n_ocs_estado,
                (SELECT ROUND(SUM(o.monto_total_item)) FROM ordenes_items o WHERE o.rut_proveedor = g.rut_proveedor) AS total_ocs_estado
            FROM gastos_servel g
            WHERE g.rut_proveedor != '' AND LENGTH(g.rut_proveedor) >= 8
              AND EXISTS (SELECT 1 FROM ordenes_items o WHERE o.rut_proveedor = g.rut_proveedor)
            GROUP BY g.rut_proveedor
        """)
        n = conn.execute("SELECT COUNT(*) FROM cruce_gastos_proveedores").fetchone()[0]
        conn.commit()
        logger.info("  ✅ %d proveedores de campaña son también proveedores del Estado (cruce por RUT exacto)", n)


def main():
    total = 0
    for label, url in SOURCES_GASTOS:
        try:
            df = load_gastos(label, url)
            n = save_gastos(df)
            logger.info("  -> +%d gastos (%s)", n, label)
            total += n
        except Exception as exc:
            logger.error("fallo %s: %s", label, exc)
    with sqlite3.connect(DB_NAME) as conn:
        tot = conn.execute("SELECT COUNT(*) FROM gastos_servel").fetchone()[0]
    logger.info("═══ +%d nuevos | TOTAL gastos_servel: %d ═══", total, tot)
    build_cruce_gastos()


if __name__ == "__main__":
    main()
