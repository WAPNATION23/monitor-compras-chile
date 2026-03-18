"""
MercadoPublicoExtractor
═══════════════════════
Clase encargada de conectarse a la API pública de Mercado Público (ChileCompra)
y descargar las órdenes de compra de una fecha específica.

Maneja:
  • Paginación automática (la API no pagina, pero el listado puede venir truncado).
  • Reintentos con back-off exponencial ante errores de red.
  • Consulta del detalle de cada OC para obtener los ítems.
"""

from __future__ import annotations

import logging
import time
from datetime import date
from typing import Any

import requests

from config import (
    API_BASE_URL,
    API_TICKET,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    RETRY_BACKOFF,
)

logger = logging.getLogger(__name__)


class MercadoPublicoExtractor:
    """Extrae órdenes de compra desde la API de Mercado Público de Chile."""

    def __init__(self, ticket: str = API_TICKET) -> None:
        self.ticket = ticket
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    # ────────────────────── Helpers de conexión ────────────────────── #

    def _get_with_retry(self, url: str, params: dict[str, str]) -> dict[str, Any]:
        """
        Realiza un GET con reintentos exponenciales.

        Returns:
            dict con la respuesta JSON parseada.

        Raises:
            requests.exceptions.RequestException si se agotan los reintentos.
        """
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

    # ────────── Paso 1: Listar códigos de OC de una fecha ──────── #

    def _fetch_oc_codes(self, fecha: date) -> list[str]:
        """
        Obtiene los códigos de todas las OC publicadas en *fecha*.

        La API retorna un listado con los códigos que luego deben consultarse
        individualmente para obtener el detalle con ítems.

        Args:
            fecha: Fecha de consulta.

        Returns:
            Lista de códigos de OC (ej. ["2097-241-SE14", ...]).
        """
        # Formato requerido: ddmmaaaa
        fecha_str: str = fecha.strftime("%d%m%Y")
        params: dict[str, str] = {
            "fecha": fecha_str,
            "ticket": self.ticket,
        }

        data: dict[str, Any] = self._get_with_retry(API_BASE_URL, params)

        # La respuesta tiene la forma:
        # { "Cantidad": N, "Listado": [ { "Codigo": "...", ... }, ... ] }
        listado: list[dict[str, Any]] = data.get("Listado", [])
        codigos: list[str] = [oc["Codigo"] for oc in listado if "Codigo" in oc]

        logger.info(
            "Fecha %s → %d órdenes de compra encontradas.", fecha_str, len(codigos)
        )
        return codigos

    # ────────── Paso 2: Detalle de una OC individual ───────────── #

    def _fetch_oc_detail(self, codigo: str) -> dict[str, Any] | None:
        """
        Descarga el detalle completo de una OC dado su código.

        Args:
            codigo: Código de la OC (ej. "2097-241-SE14").

        Returns:
            dict con el JSON de detalle, o None si falla.
        """
        params: dict[str, str] = {
            "codigo": codigo,
            "ticket": self.ticket,
        }

        try:
            data: dict[str, Any] = self._get_with_retry(API_BASE_URL, params)
            # El detalle viene anidado bajo "Listado" → primer elemento
            listado: list[dict[str, Any]] = data.get("Listado", [])
            if listado:
                return listado[0]
            logger.warning("OC %s: respuesta vacía (sin Listado).", codigo)
            return None
        except requests.exceptions.RequestException as exc:
            logger.error("No se pudo obtener detalle de OC %s: %s", codigo, exc)
            return None

    # ───────────── Método público: pipeline completo ────────────── #

    def extract(self, fecha: date) -> list[dict[str, Any]]:
        """
        Pipeline completo: lista OC → descarga detalle de cada una.

        Args:
            fecha: Fecha de las órdenes de compra a extraer.

        Returns:
            Lista de dicts con el detalle completo de cada OC.
        """
        codigos: list[str] = self._fetch_oc_codes(fecha)
        detalles: list[dict[str, Any]] = []

        for i, codigo in enumerate(codigos, start=1):
            logger.info("Descargando detalle %d/%d: %s", i, len(codigos), codigo)
            detalle: dict[str, Any] | None = self._fetch_oc_detail(codigo)
            if detalle is not None:
                detalles.append(detalle)
            # Pausa cortés para no saturar la API
            time.sleep(0.3)

        logger.info("Extracción finalizada: %d OC descargadas.", len(detalles))
        return detalles
