"""
DataProcessor
═════════════
Toma el JSON crudo de las órdenes de compra, aplana (flatten) cada ítem a una
fila, y persiste los datos relevantes en SQLite.

Campos almacenados por ítem:
  • codigo_oc          – Código de la orden de compra
  • nombre_producto    – Descripción del producto/servicio
  • categoria          – Categoría del producto (si existe)
  • cantidad           – Cantidad solicitada
  • precio_unitario    – Precio neto unitario ($CLP)
  • monto_total_item   – Precio unitario × cantidad
  • rut_comprador      – RUT del organismo comprador
  • nombre_comprador   – Nombre del organismo comprador
  • rut_proveedor      – RUT del proveedor adjudicado
  • nombre_proveedor   – Nombre del proveedor
  • fecha_creacion     – Fecha de creación de la OC
  • estado             – Estado de la OC
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd

from config import DB_NAME

logger = logging.getLogger(__name__)

# ─────────────────────── Esquema de la tabla ──────────────────────── #

_CREATE_TABLE_SQL: str = """
CREATE TABLE IF NOT EXISTS ordenes_items (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    codigo_oc        TEXT    NOT NULL,
    nombre_producto  TEXT,
    categoria        TEXT,
    cantidad         REAL,
    precio_unitario  REAL,
    monto_total_item REAL,
    rut_comprador    TEXT,
    nombre_comprador TEXT,
    rut_proveedor    TEXT,
    nombre_proveedor TEXT,
    fecha_creacion   TEXT,
    estado           TEXT,
    fecha_ingreso    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_CREATE_INDEX_SQL: str = """
CREATE INDEX IF NOT EXISTS idx_nombre_producto
    ON ordenes_items (nombre_producto);
"""


class DataProcessor:
    """Aplana las OC y las persiste en SQLite."""

    def __init__(self, db_path: str | Path = DB_NAME) -> None:
        self.db_path = Path(db_path)
        self._init_db()

    # ─────────────────── Inicialización de la BD ──────────────────── #

    def _init_db(self) -> None:
        """Crea la tabla e índices si no existen."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(_CREATE_TABLE_SQL)
                conn.execute(_CREATE_INDEX_SQL)
                conn.commit()
            logger.info("Base de datos inicializada: %s", self.db_path)
        except sqlite3.Error as exc:
            logger.error("Error inicializando la BD: %s", exc)
            raise

    # ──────────────── Flatten: JSON crudo → DataFrame ─────────────── #

    @staticmethod
    def _flatten_oc(oc: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Aplana una OC descomponiendo cada ítem en una fila independiente.

        La estructura esperada de la API (detalle de una OC):
        {
          "Codigo": "...",
          "Nombre": "...",
          "CodigoEstado": 6,
          "FechaCreacion": "...",
          "Comprador": { "RutUnidad": "...", "NombreUnidad": "..." },
          "Proveedor": { "RutProveedor": "...", "Nombre": "..." },
          "Items": {
            "Cantidad": N,
            "Listado": [
              {
                "Correlativo": 1,
                "CodigoCategoria": 12345678,
                "Categoria": "...",
                "CodigoProducto": 12345678,
                "NombreProducto": "...",
                "Descripcion": "...",
                "Cantidad": 10.0,
                "PrecioNeto": 5000.0,
                ...
              },
              ...
            ]
          }
        }
        """
        codigo_oc: str = oc.get("Codigo", "DESCONOCIDO")
        estado: str = str(oc.get("CodigoEstado", ""))
        fecha_creacion: str = oc.get("FechaCreacion", "")

        # Comprador
        comprador: dict[str, Any] = oc.get("Comprador", {})
        rut_comprador: str = comprador.get("RutUnidad", "")
        nombre_comprador: str = comprador.get("NombreUnidad", "")

        # Proveedor
        proveedor: dict[str, Any] = oc.get("Proveedor", {})
        rut_proveedor: str = proveedor.get("RutProveedor", "")
        nombre_proveedor: str = proveedor.get("Nombre", "")

        # Ítems
        items_wrapper: dict[str, Any] = oc.get("Items", {})
        items: list[dict[str, Any]] = items_wrapper.get("Listado", [])

        rows: list[dict[str, Any]] = []
        for item in items:
            cantidad: float = float(item.get("Cantidad", 0))
            precio_unitario: float = float(item.get("PrecioNeto", 0))
            rows.append(
                {
                    "codigo_oc": codigo_oc,
                    "nombre_producto": item.get("NombreProducto", item.get("Descripcion", "")),
                    "categoria": item.get("Categoria", ""),
                    "cantidad": cantidad,
                    "precio_unitario": precio_unitario,
                    "monto_total_item": cantidad * precio_unitario,
                    "rut_comprador": rut_comprador,
                    "nombre_comprador": nombre_comprador,
                    "rut_proveedor": rut_proveedor,
                    "nombre_proveedor": nombre_proveedor,
                    "fecha_creacion": fecha_creacion,
                    "estado": estado,
                }
            )

        return rows

    # ───────────── Procesamiento masivo + almacenamiento ──────────── #

    def process_and_store(self, ordenes: list[dict[str, Any]]) -> pd.DataFrame:
        """
        Aplana todas las OC y las guarda en SQLite.

        Args:
            ordenes: Lista de dicts (JSON crudo del detalle de cada OC).

        Returns:
            DataFrame de Pandas con los datos aplanados.
        """
        all_rows: list[dict[str, Any]] = []
        for oc in ordenes:
            try:
                all_rows.extend(self._flatten_oc(oc))
            except Exception as exc:
                logger.warning(
                    "Error aplanando OC %s: %s", oc.get("Codigo", "?"), exc
                )

        if not all_rows:
            logger.warning("No se obtuvieron ítems para almacenar.")
            return pd.DataFrame()

        df: pd.DataFrame = pd.DataFrame(all_rows)

        # Limpieza básica: eliminar filas sin precio o con precio ≤ 0
        df = df[df["precio_unitario"] > 0].copy()
        df["nombre_producto"] = df["nombre_producto"].str.strip().str.upper()

        # Persistir en SQLite (append)
        try:
            with sqlite3.connect(self.db_path) as conn:
                df.to_sql("ordenes_items", conn, if_exists="append", index=False)
            logger.info(
                "✓ %d ítems almacenados en %s (tabla 'ordenes_items').",
                len(df), self.db_path,
            )
        except sqlite3.Error as exc:
            logger.error("Error escribiendo en la BD: %s", exc)
            raise

        return df
