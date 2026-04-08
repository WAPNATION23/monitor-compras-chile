"""
ProveedorLookup
═══════════════
Módulo para consultar datos de Proveedores y Organismos Compradores
utilizando los endpoints de búsqueda de la API de Mercado Público.

Endpoints utilizados:
  • BuscarProveedor: Busca proveedor por RUT
  • BuscarComprador: Lista todos los organismos públicos

Uso:
    from proveedor_lookup import ProveedorLookup
    lookup = ProveedorLookup()

    # Buscar proveedor por RUT
    info = lookup.buscar_proveedor("70.017.820-k")

    # Listar todos los compradores (organismos públicos)
    compradores = lookup.listar_compradores()
"""

from __future__ import annotations

import logging
from typing import Any

import requests

from config import (
    API_BUSCAR_COMPRADOR_URL,
    API_BUSCAR_PROVEEDOR_URL,
    API_TICKET,
    REQUEST_TIMEOUT,
)

logger = logging.getLogger(__name__)


class ProveedorLookup:
    """Consulta información de proveedores y compradores en Mercado Público."""

    def __init__(self, ticket: str = API_TICKET) -> None:
        self.ticket = ticket
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def buscar_proveedor(self, rut: str) -> dict[str, Any] | None:
        """
        Busca un proveedor por RUT.

        Args:
            rut: RUT del proveedor con puntos, guión y dígito verificador.
                 Ejemplo: "70.017.820-k"

        Returns:
            dict con datos del proveedor, o None si no se encuentra.
            Incluye: CodigoEmpresa, NombreEmpresa
        """
        params: dict[str, str] = {
            "rutempresaproveedor": rut,
            "ticket": self.ticket,
        }

        try:
            response = self.session.get(
                API_BUSCAR_PROVEEDOR_URL,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            data: dict[str, Any] = response.json()

            # Extraer info del proveedor
            listado = data.get("listaEmpresas", data.get("Listado", []))
            if listado:
                proveedor = listado[0] if isinstance(listado, list) else listado
                logger.info(
                    "Proveedor encontrado: %s (Código: %s)",
                    proveedor.get("NombreEmpresa", "?"),
                    proveedor.get("CodigoEmpresa", "?"),
                )
                return proveedor

            logger.warning("Proveedor con RUT %s no encontrado.", rut)
            return None

        except requests.exceptions.RequestException as exc:
            logger.error("Error buscando proveedor %s: %s", rut, exc)
            return None

    def listar_compradores(self) -> list[dict[str, Any]]:
        """
        Lista todos los organismos públicos registrados en Mercado Público.

        Returns:
            Lista de dicts con CodigoEmpresa y NombreEmpresa de cada organismo.
        """
        params: dict[str, str] = {
            "ticket": self.ticket,
        }

        try:
            response = self.session.get(
                API_BUSCAR_COMPRADOR_URL,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            data: dict[str, Any] = response.json()

            listado = data.get("listaEmpresas", data.get("Listado", []))
            if isinstance(listado, list):
                logger.info("Organismos compradores encontrados: %d", len(listado))
                return listado

            logger.warning("No se obtuvieron organismos compradores.")
            return []

        except requests.exceptions.RequestException as exc:
            logger.error("Error listando compradores: %s", exc)
            return []

    def obtener_codigo_proveedor(self, rut: str) -> int | None:
        """
        Obtiene el código numérico de un proveedor dado su RUT.
        Este código es necesario para consultar licitaciones u OC por proveedor.

        Args:
            rut: RUT del proveedor (ej. "70.017.820-k")

        Returns:
            Código numérico del proveedor, o None si no se encuentra.
        """
        info = self.buscar_proveedor(rut)
        if info:
            codigo = info.get("CodigoEmpresa")
            if codigo:
                return int(codigo)
        return None
