"""
DatosGobConnector — Conector de datos.gob.cl (CKAN API)
═══════════════════════════════════════════════════════
Conecta con el portal de datos abiertos del Estado de Chile (datos.gob.cl)
que utiliza la API CKAN estándar.

Permite:
  • Buscar datasets por palabras clave
  • Descargar recursos (CSV, XLS) de cualquier organismo público
  • Consultar el DataStore con SQL para datasets que lo soporten

Uso:
    from datos_gob_connector import DatosGobConnector
    conn = DatosGobConnector()

    # Buscar datasets sobre subvenciones
    results = conn.search_datasets("subvenciones municipales", rows=5)

    # Descargar un recurso como DataFrame
    df = conn.download_resource("3a902cdf-8115-4365-8628-c10c9109c3a5")

    # Consulta SQL al DataStore
    df = conn.query_datastore("SELECT * FROM \"resource_id\" LIMIT 10")
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
import requests

from config import REQUEST_TIMEOUT

logger = logging.getLogger(__name__)

# ─────────────────── Constantes ─────────────────── #

CKAN_BASE_URL: str = "https://datos.gob.cl/api/3/action"


class DatosGobConnector:
    """Conector para la API CKAN de datos.gob.cl."""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    # ─────────────────── Búsqueda de datasets ─────────────────── #

    def search_datasets(
        self,
        query: str,
        rows: int = 10,
        organization: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Busca datasets en datos.gob.cl.

        Args:
            query: Texto de búsqueda.
            rows: Cantidad máxima de resultados.
            organization: Filtrar por organismo (ej. "municipalidad_de_maule").

        Returns:
            Lista de datasets con id, title, notes, organization, resources.
        """
        params: dict[str, Any] = {
            "q": query,
            "rows": rows,
        }
        if organization:
            params["fq"] = f"organization:{organization}"

        try:
            response = self.session.get(
                f"{CKAN_BASE_URL}/package_search",
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()

            if data.get("success"):
                results = data["result"]["results"]
                logger.info(
                    "Búsqueda '%s': %d datasets encontrados (de %d total).",
                    query, len(results), data["result"]["count"],
                )
                return results
            else:
                logger.error("Error en búsqueda CKAN: %s", data)
                return []

        except requests.exceptions.RequestException as exc:
            logger.error("Error conectando a datos.gob.cl: %s", exc)
            return []

    # ─────────────────── Listar organizaciones ─────────────────── #

    def list_organizations(self) -> list[dict[str, Any]]:
        """Lista todas las organizaciones (organismos públicos) en datos.gob.cl."""
        try:
            response = self.session.get(
                f"{CKAN_BASE_URL}/organization_list",
                params={"all_fields": True, "limit": 1000},
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()

            if data.get("success"):
                orgs = data["result"]
                logger.info("Organizaciones en datos.gob.cl: %d", len(orgs))
                return orgs
            return []

        except requests.exceptions.RequestException as exc:
            logger.error("Error listando organizaciones: %s", exc)
            return []

    # ─────────────────── Descargar recurso como DataFrame ─────────────────── #

    def download_resource(self, resource_id: str) -> pd.DataFrame:
        """
        Descarga un recurso del DataStore como DataFrame.

        Args:
            resource_id: UUID del recurso.

        Returns:
            DataFrame con los datos del recurso.
        """
        try:
            response = self.session.get(
                f"{CKAN_BASE_URL}/datastore_search",
                params={"resource_id": resource_id, "limit": 32000},
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()

            if data.get("success"):
                records = data["result"]["records"]
                df = pd.DataFrame(records)
                # Remover columna interna _id de CKAN
                if "_id" in df.columns:
                    df = df.drop(columns=["_id"])
                logger.info(
                    "Recurso %s descargado: %d registros.",
                    resource_id, len(df),
                )
                return df
            else:
                logger.error("Error descargando recurso %s: %s", resource_id, data)
                return pd.DataFrame()

        except requests.exceptions.RequestException as exc:
            logger.error("Error descargando recurso %s: %s", resource_id, exc)
            return pd.DataFrame()

    # ─────────────────── Consulta SQL al DataStore ─────────────────── #

    def query_datastore(self, sql: str) -> pd.DataFrame:
        """
        Ejecuta una consulta SQL contra el DataStore de datos.gob.cl.

        Args:
            sql: Sentencia SQL (ej. SELECT * FROM "resource_id" LIMIT 10).

        Returns:
            DataFrame con los resultados.
        """
        try:
            response = self.session.get(
                f"{CKAN_BASE_URL}/datastore_search_sql",
                params={"sql": sql},
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()

            if data.get("success"):
                records = data["result"]["records"]
                df = pd.DataFrame(records)
                logger.info("Consulta SQL ejecutada: %d resultados.", len(df))
                return df
            else:
                logger.error("Error en consulta SQL: %s", data)
                return pd.DataFrame()

        except requests.exceptions.RequestException as exc:
            logger.error("Error en consulta SQL: %s", exc)
            return pd.DataFrame()

    # ─────────────── Búsqueda específica: Subvenciones/Transferencias ─────── #

    def buscar_subvenciones(self, rows: int = 20) -> list[dict[str, Any]]:
        """Busca datasets relacionados con subvenciones y transferencias públicas."""
        return self.search_datasets("subvenciones transferencias municipales", rows=rows)

    # ─────────────── Búsqueda específica: Personal y remuneraciones ─────── #

    def buscar_remuneraciones(self, rows: int = 20) -> list[dict[str, Any]]:
        """Busca datasets de remuneraciones del sector público."""
        return self.search_datasets("remuneraciones personal planta contrata", rows=rows)

    # ─────────────── Búsqueda específica: Presupuesto municipal ─────── #

    def buscar_presupuesto(self, rows: int = 20) -> list[dict[str, Any]]:
        """Busca datasets de presupuesto y ejecución presupuestaria."""
        return self.search_datasets("presupuesto ejecucion municipal", rows=rows)
