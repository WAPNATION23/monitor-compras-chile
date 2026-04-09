"""
DataProcessor
═════════════
Toma el JSON crudo de las órdenes de compra, aplana (flatten) cada ítem a una
fila, y persiste los datos relevantes en SQLite.

Mejoras respecto a la versión original:
  • Columna `categoria_riesgo` clasificada automáticamente.
  • Columna `tipo_oc` extraída del código de OC.
  • Deduplicación: INSERT OR IGNORE basado en UNIQUE(codigo_oc, nombre_producto, precio_unitario, cantidad).
  • Filtro de OC canceladas (estado "9").

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
  • estado             – Estado de la OC (código numérico)
  • tipo_oc            – Tipo de OC (SE, CM, D1, etc.)
  • categoria_riesgo   – Clasificación automática de riesgo
"""

from __future__ import annotations

import logging
import re
import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd

from config import DB_NAME, RISK_CLASSIFICATION

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
    tipo_oc          TEXT    DEFAULT '',
    categoria_riesgo TEXT    DEFAULT 'GENERAL',
    fecha_ingreso    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(codigo_oc, nombre_producto, precio_unitario, cantidad)
);
"""

_CREATE_INDEXES_SQL: list[str] = [
    "CREATE INDEX IF NOT EXISTS idx_nombre_producto ON ordenes_items (nombre_producto);",
    "CREATE INDEX IF NOT EXISTS idx_categoria_riesgo ON ordenes_items (categoria_riesgo);",
    "CREATE INDEX IF NOT EXISTS idx_rut_proveedor ON ordenes_items (rut_proveedor);",
    "CREATE INDEX IF NOT EXISTS idx_rut_comprador ON ordenes_items (rut_comprador);",
    "CREATE INDEX IF NOT EXISTS idx_fecha_creacion ON ordenes_items (fecha_creacion);",
]

# Migración: agregar columnas si faltan (para BD existentes)
_MIGRATION_COLUMNS: list[tuple[str, str]] = [
    ("tipo_oc", "TEXT DEFAULT ''"),
    ("categoria_riesgo", "TEXT DEFAULT 'GENERAL'"),
]


class DataProcessor:
    """Aplana las OC y las persiste en SQLite con deduplicación y clasificación."""

    def __init__(self, db_path: str | Path = DB_NAME) -> None:
        self.db_path = Path(db_path)
        self._init_db()

    # ─────────────────── Inicialización de la BD ──────────────────── #

    def _init_db(self) -> None:
        """Crea la tabla, índices y ejecuta migraciones si es necesario."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(_CREATE_TABLE_SQL)
                for idx_sql in _CREATE_INDEXES_SQL:
                    conn.execute(idx_sql)

                # Migraciones: agregar columnas faltantes
                for col_name, col_def in _MIGRATION_COLUMNS:
                    try:
                        conn.execute(
                            f"ALTER TABLE ordenes_items ADD COLUMN {col_name} {col_def}"
                        )
                        logger.info("Migración: columna '%s' agregada.", col_name)
                    except sqlite3.OperationalError:
                        pass  # Columna ya existe

                # Migración: actualizar UNIQUE constraint si falta 'cantidad'
                self._migrate_unique_constraint(conn)

                conn.commit()
            logger.info("Base de datos inicializada: %s", self.db_path)
        except sqlite3.Error as exc:
            logger.error("Error inicializando la BD: %s", exc)
            raise

    @staticmethod
    def _migrate_unique_constraint(conn: sqlite3.Connection) -> None:
        """
        Detecta si la tabla usa el constraint viejo (sin 'cantidad') y
        la reconstruye con el constraint correcto.
        SQLite no soporta ALTER CONSTRAINT, así que se hace via tabla temporal.
        """
        # Leer el SQL original de CREATE TABLE desde sqlite_master
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='ordenes_items'"
        ).fetchone()
        if row is None:
            return

        create_sql: str = row[0]
        # Si la constraint ya incluye 'cantidad', ya está migrada
        if "cantidad" in create_sql.split("UNIQUE")[-1]:
            return

        logger.info(
            "Migración: reconstruyendo tabla para actualizar UNIQUE constraint "
            "(agregando 'cantidad')."
        )

        conn.execute("ALTER TABLE ordenes_items RENAME TO _ordenes_items_old")
        conn.execute(_CREATE_TABLE_SQL)
        conn.execute(
            """
            INSERT OR IGNORE INTO ordenes_items
                (id, codigo_oc, nombre_producto, categoria, cantidad,
                 precio_unitario, monto_total_item, rut_comprador,
                 nombre_comprador, rut_proveedor, nombre_proveedor,
                 fecha_creacion, estado, tipo_oc, categoria_riesgo,
                 fecha_ingreso)
            SELECT
                id, codigo_oc, nombre_producto, categoria, cantidad,
                precio_unitario, monto_total_item, rut_comprador,
                nombre_comprador, rut_proveedor, nombre_proveedor,
                fecha_creacion, estado, tipo_oc, categoria_riesgo,
                fecha_ingreso
            FROM _ordenes_items_old
            """
        )
        conn.execute("DROP TABLE _ordenes_items_old")
        logger.info("Migración de UNIQUE constraint completada.")

    # ──────────────── Clasificación de riesgo ─────────────── #

    @staticmethod
    def _classify_risk(nombre_comprador: str) -> str:
        """
        Clasifica la categoría de riesgo basándose en el nombre del comprador.
        Retorna la categoría que coincida con las palabras clave, o 'GENERAL'.
        """
        if not nombre_comprador:
            return "GENERAL"

        upper = nombre_comprador.upper()
        for categoria, keywords in RISK_CLASSIFICATION.items():
            for keyword in keywords:
                if keyword in upper:
                    return categoria
        return "GENERAL"

    # ──────────────── Extraer tipo de OC del código ─────────────── #

    @staticmethod
    def _extract_tipo_oc(codigo_oc: str) -> str:
        """
        Extrae el tipo de OC del código.
        Ejemplo: '2097-241-SE14' → 'SE'
                 '3401-120-CM26' → 'CM'
                 '7310-305-D126' → 'D1'
        """
        if not codigo_oc:
            return ""
        # El tipo de OC son los 2 caracteres alfanuméricos tras el último guión,
        # antes de los dígitos secuenciales (ej. D1, SE, CM, AG, R1)
        match = re.search(r"-([A-Z][A-Z0-9])\d+$", codigo_oc)
        return match.group(1) if match else ""

    # ──────────────── Normalizar RUT chileno ─────────────── #

    @staticmethod
    def _normalize_rut(rut: str) -> str:
        """
        Normaliza un RUT chileno al formato XX.XXX.XXX-X.
        Acepta con o sin puntos/guión. Retorna string vacío si es inválido.
        """
        if not rut:
            return ""
        clean = rut.replace(".", "").replace(" ", "").strip().upper()
        if not re.fullmatch(r"\d{7,8}-[\dK]", clean):
            return rut  # Devolver sin cambios si no cumple formato básico
        return clean

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
        fechas: dict[str, Any] = oc.get("Fechas", {})
        fecha_creacion: str = (
            fechas.get("FechaCreacion", "") if isinstance(fechas, dict) else ""
        ) or oc.get("FechaCreacion", "")

        # Comprador
        comprador: dict[str, Any] = oc.get("Comprador", {})
        rut_comprador: str = DataProcessor._normalize_rut(comprador.get("RutUnidad", ""))
        nombre_comprador: str = comprador.get("NombreUnidad", "")

        # Proveedor
        proveedor: dict[str, Any] = oc.get("Proveedor", {})
        rut_proveedor: str = DataProcessor._normalize_rut(proveedor.get("RutProveedor", ""))
        nombre_proveedor: str = proveedor.get("Nombre", "")

        # Tipo de OC y clasificación de riesgo
        tipo_oc: str = DataProcessor._extract_tipo_oc(codigo_oc)
        categoria_riesgo: str = DataProcessor._classify_risk(nombre_comprador)

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
                    "tipo_oc": tipo_oc,
                    "categoria_riesgo": categoria_riesgo,
                }
            )

        return rows

    # ───────────── Procesamiento masivo + almacenamiento ──────────── #

    def process_and_store(self, ordenes: list[dict[str, Any]]) -> tuple[pd.DataFrame, int]:
        """
        Aplana todas las OC y las guarda en SQLite con deduplicación.

        Args:
            ordenes: Lista de dicts (JSON crudo del detalle de cada OC).

        Returns:
            Tupla (DataFrame aplanado, cantidad de nuevos ítems insertados).
        """
        all_rows: list[dict[str, Any]] = []
        skipped_cancelled: int = 0

        for oc in ordenes:
            # Filtrar OC canceladas (estado "9")
            estado = str(oc.get("CodigoEstado", ""))
            if estado == "9":
                skipped_cancelled += 1
                continue

            try:
                all_rows.extend(self._flatten_oc(oc))
            except Exception as exc:
                logger.warning(
                    "Error aplanando OC %s: %s", oc.get("Codigo", "?"), exc
                )

        if skipped_cancelled:
            logger.info("Omitidas %d OC canceladas (estado 9).", skipped_cancelled)

        if not all_rows:
            logger.warning("No se obtuvieron ítems para almacenar.")
            return pd.DataFrame(), 0

        df: pd.DataFrame = pd.DataFrame(all_rows)

        # Limpieza básica: eliminar filas sin precio o con precio ≤ 0
        df = df[df["precio_unitario"] > 0].copy()
        df["nombre_producto"] = df["nombre_producto"].str.strip().str.upper()

        # Persistir en SQLite con deduplicación (INSERT OR IGNORE)
        inserted: int = 0
        try:
            with sqlite3.connect(self.db_path) as conn:
                before_count = conn.execute("SELECT COUNT(*) FROM ordenes_items").fetchone()[0]

                records = [
                    (
                        row["codigo_oc"], row["nombre_producto"],
                        row["categoria"], row["cantidad"],
                        row["precio_unitario"], row["monto_total_item"],
                        row["rut_comprador"], row["nombre_comprador"],
                        row["rut_proveedor"], row["nombre_proveedor"],
                        row["fecha_creacion"], row["estado"],
                        row["tipo_oc"], row["categoria_riesgo"],
                    )
                    for row in df.to_dict("records")
                ]

                conn.executemany(
                    """
                    INSERT OR IGNORE INTO ordenes_items
                        (codigo_oc, nombre_producto, categoria, cantidad,
                         precio_unitario, monto_total_item, rut_comprador,
                         nombre_comprador, rut_proveedor, nombre_proveedor,
                         fecha_creacion, estado, tipo_oc, categoria_riesgo)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    records,
                )
                conn.commit()

                after_count = conn.execute("SELECT COUNT(*) FROM ordenes_items").fetchone()[0]
                inserted = after_count - before_count

            logger.info(
                "✓ %d nuevos ítems almacenados en %s (%d duplicados omitidos).",
                inserted, self.db_path, len(df) - inserted,
            )
        except sqlite3.Error as exc:
            logger.error("Error escribiendo en la BD: %s", exc)
            raise

        return df, inserted
