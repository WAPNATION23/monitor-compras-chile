"""
MercadoPublicoExtractor
═══════════════════════
Clase encargada de conectarse a la API pública de Mercado Público (ChileCompra)
y descargar las órdenes de compra de una fecha específica.

Maneja:
  • Paginación por estado cuando la API trunca resultados.
  • Reintentos con back-off exponencial ante errores de red.
  • Rate limiting inteligente (1 req/seg para evitar "peticiones simultáneas").
  • Límite configurable de OC a descargar por ejecución.
  • Modo rápido: extrae solo el listado SIN consultar detalle individual.
  • Modo completo: extrae listado + detalle de cada OC (lento pero completo).
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
    OC_ESTADO_LABELS,
)

logger = logging.getLogger(__name__)

# Máximo de OC a descargar en detalle por ejecución.
# Con ~18,000 OC diarias y 1 req/seg, 5000 ≈ ~1.4 h. Usar --max-oc para ajustar.
MAX_OC_PER_RUN: int = 5000
# Delay entre requests individuales al API (segundos)
REQUEST_DELAY: float = 1.0


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

    def _fetch_oc_codes(self, fecha: date) -> list[dict[str, Any]]:
        """
        Obtiene el listado de OC publicadas en *fecha*.

        Si la respuesta inicial está truncada (menos resultados de los que
        reporta la API), reintenta con filtros por código de estado para
        obtener la mayor cantidad posible de OCs.

        Args:
            fecha: Fecha de consulta.

        Returns:
            Lista de dicts con info básica de cada OC.
        """
        # Formato requerido: ddmmaaaa
        fecha_str: str = fecha.strftime("%d%m%Y")
        params: dict[str, str] = {
            "fecha": fecha_str,
            "ticket": self.ticket,
        }

        data: dict[str, Any] = self._get_with_retry(API_BASE_URL, params)

        cantidad_total: int = data.get("Cantidad", 0)
        listado: list[dict[str, Any]] = data.get("Listado", [])

        if len(listado) >= cantidad_total or cantidad_total == 0:
            logger.info(
                "Fecha %s → %d OC encontradas.", fecha_str, len(listado),
            )
            return listado

        # La API truncó resultados — intentar obtener más filtrando por estado
        logger.warning(
            "Fecha %s: API reporta %d OC pero retornó solo %d. "
            "Reintentando con filtros por estado...",
            fecha_str, cantidad_total, len(listado),
        )

        seen_codes: set[str] = {oc["Codigo"] for oc in listado if "Codigo" in oc}
        all_ocs: list[dict[str, Any]] = list(listado)

        for estado_code in OC_ESTADO_LABELS:
            params_estado: dict[str, str] = {
                "fecha": fecha_str,
                "ticket": self.ticket,
                "CodigoEstado": estado_code,
            }
            try:
                data_estado = self._get_with_retry(API_BASE_URL, params_estado)
                nuevas = [
                    oc for oc in data_estado.get("Listado", [])
                    if oc.get("Codigo") and oc["Codigo"] not in seen_codes
                ]
                for oc in nuevas:
                    seen_codes.add(oc["Codigo"])
                all_ocs.extend(nuevas)
                time.sleep(REQUEST_DELAY)
            except requests.exceptions.RequestException as exc:
                logger.warning("Error consultando estado %s: %s", estado_code, exc)

        logger.info(
            "Fecha %s → %d OC recuperadas (de %d reportadas por API).",
            fecha_str, len(all_ocs), cantidad_total,
        )
        if len(all_ocs) < cantidad_total:
            logger.warning(
                "Datos posiblemente incompletos: %d/%d OC recuperadas.",
                len(all_ocs), cantidad_total,
            )

        return all_ocs

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
            listado: list[dict[str, Any]] = data.get("Listado", [])
            if listado:
                return listado[0]
            logger.warning("OC %s: respuesta vacía (sin Listado).", codigo)
            return None
        except requests.exceptions.RequestException as exc:
            logger.error("No se pudo obtener detalle de OC %s: %s", codigo, exc)
            return None

    # ───────────── Método público: extracción RÁPIDA ────────────── #

    def extract_fast(self, fecha: date) -> list[dict[str, Any]]:
        """
        Extracción rápida: solo obtiene el listado (sin detalle de ítems).

        Útil para:
          • Obtener una visión general del volumen de compras
          • Filtrar antes de hacer la extracción completa
          • Estadísticas de cantidad de OC por día

        Returns:
            Lista de dicts con info básica {Codigo, Nombre, CodigoEstado}.
        """
        return self._fetch_oc_codes(fecha)

    # ───────────── Método público: extracción COMPLETA ────────────── #

    def extract(
        self,
        fecha: date,
        max_oc: int = MAX_OC_PER_RUN,
        delay: float = REQUEST_DELAY,
    ) -> list[dict[str, Any]]:
        """
        Pipeline completo: lista OC → descarga detalle de cada una.

        Con 18,000+ OC diarias, se limita a `max_oc` para no saturar la API.
        Las OC se seleccionan priorizando las más recientes.
        La paginación por estado se aplica automáticamente si la API trunca.

        Args:
            fecha: Fecha de las órdenes de compra a extraer.
            max_oc: Máximo de OC a descargar en detalle (default: 200).
            delay: Segundos de espera entre cada request (default: 1.0).

        Returns:
            Lista de dicts con el detalle completo de cada OC.
        """
        listado: list[dict[str, Any]] = self._fetch_oc_codes(fecha)

        if not listado:
            return []

        # Limitar la cantidad de OC a procesar (max_oc=0 → sin límite)
        codigos: list[str] = [oc["Codigo"] for oc in listado if "Codigo" in oc]

        if max_oc > 0 and len(codigos) > max_oc:
            logger.info(
                "Limitando a %d OC de %d disponibles (día %s).",
                max_oc, len(codigos), fecha.strftime("%d/%m/%Y"),
            )
            codigos = codigos[:max_oc]

        detalles: list[dict[str, Any]] = []
        errores: int = 0

        for i, codigo in enumerate(codigos, start=1):
            if i % 50 == 0 or i == 1:
                logger.info(
                    "Progreso: %d/%d OC descargadas (%d errores)...",
                    i, len(codigos), errores,
                )

            detalle: dict[str, Any] | None = self._fetch_oc_detail(codigo)
            if detalle is not None:
                detalles.append(detalle)
            else:
                errores += 1

            # Rate limiting — respetar la API
            time.sleep(delay)

        logger.info(
            "Extracción finalizada: %d OC descargadas (%d errores de %d intentos).",
            len(detalles), errores, len(codigos),
        )
        return detalles
