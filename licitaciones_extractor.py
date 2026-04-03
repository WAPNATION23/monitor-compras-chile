"""
LicitacionesExtractor
═════════════════════
Clase encargada de conectarse a la API pública de Mercado Público (ChileCompra)
y descargar licitaciones por fecha, estado o código.

Complementa al MercadoPublicoExtractor (órdenes de compra) con datos de
licitaciones que incluyen:
  • Tipo de compra (Convenio Marco, Trato Directo, Licitación Pública)
  • Montos estimados
  • Plazos de evaluación
  • Estado de adjudicación

Uso:
    from licitaciones_extractor import LicitacionesExtractor
    ext = LicitacionesExtractor()
    lics = ext.extract_by_date(date(2026, 3, 15))
    lics_adj = ext.extract_by_date(date(2026, 3, 15), estado="adjudicada")
"""

from __future__ import annotations

import logging
import time
from datetime import date
from typing import Any

import requests

from config import (
    API_LICITACIONES_URL,
    API_TICKET,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    RETRY_BACKOFF,
)

logger = logging.getLogger(__name__)


class LicitacionesExtractor:
    """Extrae licitaciones desde la API de Mercado Público de Chile."""

    # Estados válidos para consultar
    ESTADOS_VALIDOS = {
        "publicada", "cerrada", "desierta", "adjudicada",
        "revocada", "suspendida", "activas", "todos",
    }

    def __init__(self, ticket: str = API_TICKET) -> None:
        self.ticket = ticket
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    # ────────────────────── Helpers de conexión ────────────────────── #

    def _get_with_retry(self, url: str, params: dict[str, str]) -> dict[str, Any]:
        """Realiza un GET con reintentos exponenciales."""
        last_exception: Exception | None = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.debug("GET %s (intento %d/%d)", url, attempt, MAX_RETRIES)
                response = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as exc:
                last_exception = exc
                wait = RETRY_BACKOFF ** attempt
                logger.warning(
                    "Error en intento %d/%d: %s — reintentando en %.1fs",
                    attempt, MAX_RETRIES, exc, wait,
                )
                time.sleep(wait)
        raise requests.exceptions.ConnectionError(
            f"Agotados {MAX_RETRIES} reintentos. Último error: {last_exception}"
        )

    # ──────── Listar licitaciones por fecha ──────── #

    def extract_by_date(
        self,
        fecha: date,
        estado: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Obtiene licitaciones publicadas en *fecha*.

        Args:
            fecha: Fecha de consulta.
            estado: Filtro de estado opcional. Valores válidos:
                    publicada, cerrada, desierta, adjudicada,
                    revocada, suspendida, activas, todos.

        Returns:
            Lista de dicts con la info básica de cada licitación.
        """
        fecha_str: str = fecha.strftime("%d%m%Y")
        params: dict[str, str] = {
            "fecha": fecha_str,
            "ticket": self.ticket,
        }

        if estado:
            estado_lower = estado.lower()
            if estado_lower not in self.ESTADOS_VALIDOS:
                logger.warning(
                    "Estado '%s' no reconocido. Valores válidos: %s",
                    estado, ", ".join(self.ESTADOS_VALIDOS),
                )
            params["estado"] = estado_lower

        data: dict[str, Any] = self._get_with_retry(API_LICITACIONES_URL, params)

        listado: list[dict[str, Any]] = data.get("Listado", [])
        logger.info(
            "Fecha %s (estado=%s) → %d licitaciones encontradas.",
            fecha_str, estado or "todos", len(listado),
        )
        return listado

    # ──────── Detalle de una licitación por código ──────── #

    def extract_by_code(self, codigo: str) -> dict[str, Any] | None:
        """
        Descarga el detalle completo de una licitación dado su código.

        Args:
            codigo: Código de la licitación (ej. "1509-5-L114").

        Returns:
            dict con el JSON de detalle, o None si falla.
        """
        params: dict[str, str] = {
            "codigo": codigo,
            "ticket": self.ticket,
        }

        try:
            data: dict[str, Any] = self._get_with_retry(API_LICITACIONES_URL, params)
            listado: list[dict[str, Any]] = data.get("Listado", [])
            if listado:
                return listado[0]
            logger.warning("Licitación %s: respuesta vacía.", codigo)
            return None
        except requests.exceptions.RequestException as exc:
            logger.error("No se pudo obtener licitación %s: %s", codigo, exc)
            return None

    # ──────── Búsqueda por organismo comprador ──────── #

    def extract_by_organismo(
        self,
        fecha: date,
        codigo_organismo: int,
    ) -> list[dict[str, Any]]:
        """
        Obtiene licitaciones de un organismo público específico.

        Args:
            fecha: Fecha de consulta.
            codigo_organismo: Código numérico del organismo en Mercado Público.

        Returns:
            Lista de licitaciones del organismo.
        """
        fecha_str: str = fecha.strftime("%d%m%Y")
        params: dict[str, str] = {
            "fecha": fecha_str,
            "CodigoOrganismo": str(codigo_organismo),
            "ticket": self.ticket,
        }

        data: dict[str, Any] = self._get_with_retry(API_LICITACIONES_URL, params)
        listado: list[dict[str, Any]] = data.get("Listado", [])
        logger.info(
            "Organismo %d (fecha %s) → %d licitaciones.",
            codigo_organismo, fecha_str, len(listado),
        )
        return listado

    # ──────── Búsqueda por proveedor ──────── #

    def extract_by_proveedor(
        self,
        fecha: date,
        codigo_proveedor: int,
    ) -> list[dict[str, Any]]:
        """
        Obtiene licitaciones asociadas a un proveedor específico.

        Args:
            fecha: Fecha de consulta.
            codigo_proveedor: Código numérico del proveedor en Mercado Público.

        Returns:
            Lista de licitaciones del proveedor.
        """
        fecha_str: str = fecha.strftime("%d%m%Y")
        params: dict[str, str] = {
            "fecha": fecha_str,
            "CodigoProveedor": str(codigo_proveedor),
            "ticket": self.ticket,
        }

        data: dict[str, Any] = self._get_with_retry(API_LICITACIONES_URL, params)
        listado: list[dict[str, Any]] = data.get("Listado", [])
        logger.info(
            "Proveedor %d (fecha %s) → %d licitaciones.",
            codigo_proveedor, fecha_str, len(listado),
        )
        return listado
