"""
DipresConnector — Conector de la Dirección de Presupuestos (DIPRES)
═══════════════════════════════════════════════════════════════════
Accede a datos presupuestarios del Estado de Chile publicados por DIPRES.

Fuentes:
  • DIPRES presupuestos por institución: https://www.dipres.gob.cl/597/w3-channel.html
  • datos.gob.cl datasets de DIPRES (CKAN API)
  • Transparencia activa: dotación y gasto en personal

Datos disponibles:
  • Presupuesto asignado vs ejecutado por institución
  • Dotación de personal por servicio público
  • Gasto en personal (honorarios, contratas, planta)

Uso forense:
    from dipres_connector import DipresConnector
    dp = DipresConnector()

    # Buscar datasets de personal y presupuesto
    datasets = dp.buscar_datasets_personal()

    # Obtener gastos en personal de un organismo
    gastos = dp.buscar_datos_gob("personal municipalidad")
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Any

import pandas as pd
import requests

from config import DB_NAME, REQUEST_TIMEOUT

logger = logging.getLogger(__name__)

# ─────────────────── URLs ─────────────────── #

CKAN_BASE: str = "https://datos.gob.cl/api/3/action"

# Datasets conocidos de DIPRES en datos.gob.cl
# Estos IDs pueden cambiar — se buscan dinámicamente como fallback
DATASETS_DIPRES: dict[str, str] = {
    "dotacion_personal": "dotacion-de-personal-del-gobierno-central",
    "gastos_personal": "gastos-en-personal-del-gobierno-central",
    "ejecucion_presupuestaria": "ejecucion-presupuestaria-del-gobierno-central",
}

# ─────────────────── SQL ─────────────────── #

CREATE_PRESUPUESTO_SQL: str = """
CREATE TABLE IF NOT EXISTS presupuesto_dipres (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    anio INTEGER,
    institucion TEXT NOT NULL,
    subtitulo TEXT,
    presupuesto_inicial INTEGER,
    presupuesto_vigente INTEGER,
    gasto_devengado INTEGER,
    fuente TEXT,
    fecha_ingreso TEXT DEFAULT (datetime('now'))
)
"""

CREATE_DOTACION_SQL: str = """
CREATE TABLE IF NOT EXISTS dotacion_dipres (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    anio INTEGER,
    institucion TEXT NOT NULL,
    tipo_contrato TEXT,
    cantidad INTEGER,
    gasto_total INTEGER,
    fuente TEXT,
    fecha_ingreso TEXT DEFAULT (datetime('now'))
)
"""


class DipresConnector:
    """Conector para datos presupuestarios de DIPRES."""

    def __init__(self, db_path: str = DB_NAME) -> None:
        self.db_path = db_path
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "MonitorComprasChile/1.0 (Auditoría Cívica)",
        })
        self._init_db()

    def _init_db(self) -> None:
        """Crea las tablas si no existen."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(CREATE_PRESUPUESTO_SQL)
                conn.execute(CREATE_DOTACION_SQL)
        except sqlite3.Error as exc:
            logger.error("Error inicializando tablas DIPRES: %s", exc)

    # ═══════════════════════════════════════════════════════════
    # Buscar datasets en datos.gob.cl
    # ═══════════════════════════════════════════════════════════

    def buscar_datasets_personal(self) -> list[dict[str, Any]]:
        """
        Busca datasets de personal y presupuesto en datos.gob.cl.

        Returns:
            Lista de datasets disponibles con id, title, resources.
        """
        queries = [
            "personal gobierno central dotación",
            "presupuesto ejecución instituciones",
            "honorarios funcionarios públicos",
            "remuneraciones sector público",
        ]

        all_datasets: list[dict[str, Any]] = []
        seen_ids: set[str] = set()

        for q in queries:
            try:
                resp = self.session.get(
                    f"{CKAN_BASE}/package_search",
                    params={"q": q, "rows": 5},
                    timeout=REQUEST_TIMEOUT,
                )
                resp.raise_for_status()
                data = resp.json()

                if data.get("success"):
                    for ds in data["result"]["results"]:
                        ds_id = ds.get("id", "")
                        if ds_id not in seen_ids:
                            seen_ids.add(ds_id)
                            all_datasets.append({
                                "id": ds_id,
                                "name": ds.get("name", ""),
                                "title": ds.get("title", ""),
                                "organization": ds.get("organization", {}).get("title", "N/D"),
                                "resources": [
                                    {
                                        "id": r.get("id", ""),
                                        "name": r.get("name", ""),
                                        "format": r.get("format", ""),
                                        "datastore_active": r.get("datastore_active", False),
                                    }
                                    for r in ds.get("resources", [])
                                ],
                            })

            except requests.exceptions.RequestException as exc:
                logger.error("Error buscando datasets DIPRES [%s]: %s", q, exc)

        logger.info("DIPRES: %d datasets encontrados en datos.gob.cl.", len(all_datasets))
        return all_datasets

    # ═══════════════════════════════════════════════════════════
    # Buscar datos genéricos en datos.gob.cl
    # ═══════════════════════════════════════════════════════════

    def buscar_datos_gob(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        """
        Busca datasets en datos.gob.cl por query libre.

        Returns:
            Lista de datasets con metadata y recursos.
        """
        try:
            resp = self.session.get(
                f"{CKAN_BASE}/package_search",
                params={"q": query, "rows": limit},
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()

            if not data.get("success"):
                return []

            results = []
            for ds in data["result"]["results"]:
                results.append({
                    "id": ds.get("id", ""),
                    "name": ds.get("name", ""),
                    "title": ds.get("title", ""),
                    "organization": ds.get("organization", {}).get("title", "N/D"),
                    "num_resources": len(ds.get("resources", [])),
                    "url": f"https://datos.gob.cl/dataset/{ds.get('name', '')}",
                })
            return results

        except requests.exceptions.RequestException as exc:
            logger.error("Error en datos.gob.cl: %s", exc)
            return []

    # ═══════════════════════════════════════════════════════════
    # Descargar recurso DataStore
    # ═══════════════════════════════════════════════════════════

    def descargar_recurso(self, resource_id: str, limit: int = 1000) -> pd.DataFrame:
        """
        Descarga un recurso del DataStore de datos.gob.cl como DataFrame.
        Solo funciona para recursos con datastore_active=True.
        """
        try:
            resp = self.session.get(
                f"{CKAN_BASE}/datastore_search",
                params={"resource_id": resource_id, "limit": limit},
                timeout=REQUEST_TIMEOUT * 2,
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get("success"):
                records = data["result"].get("records", [])
                if records:
                    df = pd.DataFrame(records)
                    logger.info("DIPRES: %d registros descargados de %s.", len(df), resource_id)
                    return df

        except requests.exceptions.RequestException as exc:
            logger.error("Error descargando recurso %s: %s", resource_id, exc)

        return pd.DataFrame()

    # ═══════════════════════════════════════════════════════════
    # Cruce forense: presupuesto vs gasto en compras
    # ═══════════════════════════════════════════════════════════

    def cruzar_presupuesto_compras(self) -> pd.DataFrame:
        """
        Cruza el gasto en compras públicas por organismo con
        los datos de presupuesto de DIPRES (si están disponibles).

        Permite detectar organismos que gastan desproporcionadamente
        más en compras de lo que su presupuesto asignado sugiere.
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Obtener gasto en compras por organismo
                compras = pd.read_sql_query(
                    """
                    SELECT nombre_comprador as institucion,
                           COUNT(DISTINCT codigo_oc) as total_ocs,
                           SUM(monto_total_item) as gasto_compras
                    FROM ordenes_items
                    WHERE estado != '9'
                    GROUP BY nombre_comprador
                    ORDER BY gasto_compras DESC
                    """,
                    conn,
                )

                # Verificar si hay datos de presupuesto
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' "
                    "AND name='presupuesto_dipres'"
                )
                if not cursor.fetchone():
                    # Sin datos DIPRES, retornar solo compras
                    return compras

                presup = pd.read_sql_query(
                    "SELECT * FROM presupuesto_dipres ORDER BY anio DESC",
                    conn,
                )

            if presup.empty:
                return compras

            # Merge por nombre de institución (fuzzy)
            # TODO: Mejorar matching con normalización de nombres
            return compras

        except (sqlite3.Error, pd.errors.DatabaseError) as exc:
            logger.error("Error en cruce presupuesto-compras: %s", exc)
            return pd.DataFrame()
