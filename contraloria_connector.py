"""
ContraloriaConnector — Conector de la Contraloría General de la República
═════════════════════════════════════════════════════════════════════════
Accede a datos públicos de fiscalizaciones, informes de auditoría y
dictámenes de la CGR.

Fuentes:
  • Fiscalizaciones en curso: https://www.contraloria.cl/web/cgr/fiscalizaciones-en-curso
  • Informes de auditoría (SICA): PDFs públicos vía portal CGR
  • Buscador de jurisprudencia: dictámenes y resoluciones

Datos disponibles:
  • Fiscalizaciones: región, sector, entidad, período, tipo, materia
  • Informes: institución auditada, hallazgos, montos observados
  • Dictámenes: número, fecha, materia, extracto

Uso forense:
    from contraloria_connector import ContraloriaConnector
    cgr = ContraloriaConnector()

    # Obtener fiscalizaciones en curso
    fisc = cgr.obtener_fiscalizaciones()

    # Buscar si una entidad está bajo investigación
    resultado = cgr.buscar_fiscalizacion_entidad("HOSPITAL")

    # Buscar dictámenes sobre una materia
    dict_result = cgr.buscar_dictamenes("contrato honorarios")
"""

from __future__ import annotations

import logging
import re
import sqlite3
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config import DB_NAME, REQUEST_TIMEOUT

logger = logging.getLogger(__name__)

# ─────────────────── URLs ─────────────────── #

CGR_BASE: str = "https://www.contraloria.cl"
FISCALIZACIONES_URL: str = f"{CGR_BASE}/web/cgr/fiscalizaciones-en-curso"
BUSCAR_URL: str = f"{CGR_BASE}/web/cgr/buscar"

# ─────────────────── SQL para tabla local ─────────────────── #

CREATE_FISCALIZACIONES_SQL: str = """
CREATE TABLE IF NOT EXISTS fiscalizaciones_cgr (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    region TEXT,
    sector TEXT,
    entidad TEXT NOT NULL,
    periodo TEXT,
    tipo_fiscalizacion TEXT,
    materia TEXT,
    fecha_ingreso TEXT DEFAULT (datetime('now'))
)
"""

CREATE_INFORMES_SQL: str = """
CREATE TABLE IF NOT EXISTS informes_cgr (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fecha TEXT,
    entidad TEXT NOT NULL,
    titulo TEXT,
    url_informe TEXT,
    fecha_ingreso TEXT DEFAULT (datetime('now'))
)
"""


class ContraloriaConnector:
    """Conector para datos públicos de la Contraloría General."""

    def __init__(self, db_path: str = DB_NAME) -> None:
        self.db_path = db_path
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "MonitorComprasChile/1.0 (Auditoría Cívica)",
            "Accept": "text/html,application/xhtml+xml",
        })
        self._init_db()

    def _init_db(self) -> None:
        """Crea las tablas si no existen."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(CREATE_FISCALIZACIONES_SQL)
                conn.execute(CREATE_INFORMES_SQL)
        except sqlite3.Error as exc:
            logger.error("Error inicializando tablas CGR: %s", exc)

    # ═══════════════════════════════════════════════════════════
    # Fiscalizaciones en curso (scraping de tabla HTML)
    # ═══════════════════════════════════════════════════════════

    def obtener_fiscalizaciones(self) -> list[dict[str, str]]:
        """
        Obtiene la lista de fiscalizaciones en curso desde el portal CGR.
        Parsea la tabla HTML de la página.

        Returns:
            Lista de dicts con: region, sector, entidad, periodo,
                                tipo_fiscalizacion, materia
        """
        results: list[dict[str, str]] = []

        try:
            resp = self.session.get(FISCALIZACIONES_URL, timeout=REQUEST_TIMEOUT * 2)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, "html.parser")

            # La tabla de fiscalizaciones tiene filas con datos
            # Buscar filas de tabla con datos de fiscalizaciones
            rows = soup.select("table tr, .table-data tr, tbody tr")

            for row in rows:
                cells = row.find_all(["td", "th"])
                texts = [c.get_text(strip=True) for c in cells]

                # Filtrar filas vacías o headers
                if len(texts) >= 4 and not any(
                    h in texts[0].upper() for h in ["REGIÓN", "SECTOR", "FILTRAR"]
                ):
                    entry = {
                        "region": texts[0] if len(texts) > 0 else "",
                        "sector": texts[1] if len(texts) > 1 else "",
                        "entidad": texts[2] if len(texts) > 2 else "",
                        "periodo": texts[3] if len(texts) > 3 else "",
                        "tipo_fiscalizacion": texts[4] if len(texts) > 4 else "",
                        "materia": texts[5] if len(texts) > 5 else "",
                    }
                    if entry["entidad"]:
                        results.append(entry)

            logger.info("CGR: %d fiscalizaciones en curso obtenidas.", len(results))

        except requests.exceptions.RequestException as exc:
            logger.error("Error obteniendo fiscalizaciones CGR: %s", exc)

        return results

    # ═══════════════════════════════════════════════════════════
    # Buscar fiscalizaciones por entidad
    # ═══════════════════════════════════════════════════════════

    def buscar_fiscalizacion_entidad(self, nombre_entidad: str) -> list[dict[str, str]]:
        """
        Busca fiscalizaciones en la DB local para una entidad específica.
        Primero intenta en DB local, si vacía, busca en el portal.
        """
        if not nombre_entidad or not nombre_entidad.strip():
            return []

        # Primero buscar en DB local
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' "
                    "AND name='fiscalizaciones_cgr'"
                )
                if cursor.fetchone():
                    df = pd.read_sql_query(
                        """
                        SELECT * FROM fiscalizaciones_cgr
                        WHERE UPPER(entidad) LIKE ?
                        ORDER BY periodo DESC
                        """,
                        conn,
                        params=[f"%{nombre_entidad.upper()}%"],
                    )
                    if not df.empty:
                        return df.to_dict("records")
        except (sqlite3.Error, pd.errors.DatabaseError):
            pass

        # Si no hay datos locales, buscar en la web
        all_fisc = self.obtener_fiscalizaciones()
        matches = [
            f for f in all_fisc
            if nombre_entidad.upper() in f.get("entidad", "").upper()
        ]
        return matches

    # ═══════════════════════════════════════════════════════════
    # Informes de auditoría destacados
    # ═══════════════════════════════════════════════════════════

    def obtener_informes_destacados(self) -> list[dict[str, str]]:
        """
        Obtiene los informes de auditoría destacados desde el portal CGR.
        Estos aparecen en la portada del sitio.

        Returns:
            Lista de dicts con: fecha, entidad, titulo, url_informe
        """
        results: list[dict[str, str]] = []

        try:
            resp = self.session.get(f"{CGR_BASE}/web/cgr", timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, "html.parser")

            # Los informes destacados están en links con URLs de SICA
            sica_links = soup.find_all("a", href=re.compile(r"SicaProd.*servletficha"))

            for link in sica_links:
                texto = link.get_text(strip=True)
                href = link.get("href", "")

                # Extraer fecha y entidad del texto
                # Formato: "DD-MM-YYYY ENTIDAD Informe Final..."
                match = re.match(
                    r"(\d{2}-\d{2}-\d{4})\s+(.+?)\s+(Informe\s+.+)",
                    texto, re.IGNORECASE,
                )
                if match:
                    fecha_raw = match.group(1)
                    # Convertir DD-MM-YYYY a YYYY-MM-DD
                    parts = fecha_raw.split("-")
                    fecha = f"{parts[2]}-{parts[1]}-{parts[0]}" if len(parts) == 3 else fecha_raw

                    results.append({
                        "fecha": fecha,
                        "entidad": match.group(2).strip(),
                        "titulo": match.group(3).strip()[:200],
                        "url_informe": href if href.startswith("http") else f"{CGR_BASE}{href}",
                    })

            logger.info("CGR: %d informes destacados obtenidos.", len(results))

        except requests.exceptions.RequestException as exc:
            logger.error("Error obteniendo informes CGR: %s", exc)

        return results

    # ═══════════════════════════════════════════════════════════
    # Guardar fiscalizaciones en DB local
    # ═══════════════════════════════════════════════════════════

    def guardar_fiscalizaciones(self, fiscalizaciones: list[dict[str, str]]) -> int:
        """Guarda fiscalizaciones en la DB local. Retorna cantidad insertada."""
        if not fiscalizaciones:
            return 0

        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(CREATE_FISCALIZACIONES_SQL)
                # Limpiar tabla antes de recargar (snapshot completo)
                conn.execute("DELETE FROM fiscalizaciones_cgr")
                df = pd.DataFrame(fiscalizaciones)
                cols = ["region", "sector", "entidad", "periodo",
                        "tipo_fiscalizacion", "materia"]
                for c in cols:
                    if c not in df.columns:
                        df[c] = ""
                df[cols].to_sql("fiscalizaciones_cgr", conn, if_exists="append", index=False)
                count = len(df)
                logger.info("CGR: %d fiscalizaciones guardadas en DB.", count)
                return count
        except (sqlite3.Error, Exception) as exc:
            logger.error("Error guardando fiscalizaciones: %s", exc)
            return 0

    def guardar_informes(self, informes: list[dict[str, str]]) -> int:
        """Guarda informes de auditoría en la DB local."""
        if not informes:
            return 0

        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(CREATE_INFORMES_SQL)
                df = pd.DataFrame(informes)
                cols = ["fecha", "entidad", "titulo", "url_informe"]
                for c in cols:
                    if c not in df.columns:
                        df[c] = ""
                df[cols].to_sql("informes_cgr", conn, if_exists="append", index=False)
                count = len(df)
                logger.info("CGR: %d informes guardados en DB.", count)
                return count
        except (sqlite3.Error, Exception) as exc:
            logger.error("Error guardando informes: %s", exc)
            return 0

    # ═══════════════════════════════════════════════════════════
    # Cruce forense: entidad compradora ↔ bajo fiscalización
    # ═══════════════════════════════════════════════════════════

    def entidad_bajo_fiscalizacion(self, nombre_comprador: str) -> bool:
        """
        Verifica si un organismo comprador está actualmente bajo
        fiscalización de la Contraloría.
        """
        fisc = self.buscar_fiscalizacion_entidad(nombre_comprador)
        return len(fisc) > 0

    def cruzar_compradores_fiscalizados(self) -> pd.DataFrame:
        """
        Cruza los organismos compradores de la DB de compras con las
        fiscalizaciones en curso de la CGR.

        Returns:
            DataFrame con compradores que están bajo fiscalización.
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Obtener organismos compradores únicos
                compradores = pd.read_sql_query(
                    """
                    SELECT DISTINCT nombre_comprador,
                           COUNT(*) as total_ocs,
                           SUM(monto_total_item) as gasto_total
                    FROM ordenes_items
                    WHERE estado != '9'
                    GROUP BY nombre_comprador
                    """,
                    conn,
                )

                # Verificar tabla de fiscalizaciones
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' "
                    "AND name='fiscalizaciones_cgr'"
                )
                if not cursor.fetchone():
                    logger.info("Tabla fiscalizaciones_cgr vacía. Cargar datos primero.")
                    return pd.DataFrame()

                fiscalizaciones = pd.read_sql_query(
                    "SELECT DISTINCT entidad, tipo_fiscalizacion, materia FROM fiscalizaciones_cgr",
                    conn,
                )

            if compradores.empty or fiscalizaciones.empty:
                return pd.DataFrame()

            # Cruce fuzzy por nombre
            cruces = []
            for _, comp in compradores.iterrows():
                nombre = comp["nombre_comprador"].upper()
                for _, fisc in fiscalizaciones.iterrows():
                    entidad = fisc["entidad"].upper()
                    # Match si el nombre del comprador contiene la entidad o viceversa
                    if (entidad in nombre or nombre in entidad or
                            _fuzzy_match(nombre, entidad)):
                        cruces.append({
                            "nombre_comprador": comp["nombre_comprador"],
                            "total_ocs": comp["total_ocs"],
                            "gasto_total": comp["gasto_total"],
                            "entidad_fiscalizada": fisc["entidad"],
                            "tipo_fiscalizacion": fisc["tipo_fiscalizacion"],
                            "materia_fiscalizacion": fisc["materia"],
                        })

            result = pd.DataFrame(cruces)
            if not result.empty:
                logger.warning(
                    "🚨 CGR: %d compradores bajo fiscalización activa.",
                    result["nombre_comprador"].nunique(),
                )
            return result

        except (sqlite3.Error, pd.errors.DatabaseError) as exc:
            logger.error("Error en cruce compradores-fiscalizaciones: %s", exc)
            return pd.DataFrame()


def _fuzzy_match(a: str, b: str, threshold: float = 0.6) -> bool:
    """Match simple por palabras compartidas."""
    words_a = set(a.split())
    words_b = set(b.split())
    if not words_a or not words_b:
        return False
    intersection = words_a & words_b
    # Descartar palabras comunes que no aportan
    stopwords = {"DE", "DEL", "LA", "LOS", "LAS", "EL", "Y", "EN", "A"}
    meaningful = intersection - stopwords
    total_meaningful_a = words_a - stopwords
    if not total_meaningful_a:
        return False
    return len(meaningful) / len(total_meaningful_a) >= threshold
