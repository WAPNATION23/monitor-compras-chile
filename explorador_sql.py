"""
Explorador SQL Forense — Monitor Compras Chile
═══════════════════════════════════════════════
Conecta a auditoria_estado.db, verifica (o crea) datos de prueba,
y ejecuta 3 consultas forenses para análisis de compras públicas.

Uso:
    py explorador_sql.py
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pandas as pd

from config import DB_NAME

# ─────────────────────── Constantes ──────────────────────── #

DB_PATH: Path = Path(DB_NAME)

# Esquema idéntico al de processor.py para mantener compatibilidad
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


# ═══════════════════════════════════════════════════════════════ #
#                     FUNCIONES PRINCIPALES                      #
# ═══════════════════════════════════════════════════════════════ #


def verificar_o_crear_datos(conn: sqlite3.Connection) -> int:
    """
    Verifica si la tabla ordenes_items existe y tiene datos.
    Si no existe, la crea vacía.

    Returns:
        Cantidad de registros en la tabla.
    """
    conn.execute(_CREATE_TABLE_SQL)
    conn.commit()

    cursor = conn.execute("SELECT COUNT(*) FROM ordenes_items")
    count: int = cursor.fetchone()[0]

    if count > 0:
        print(f"  ✅ Tabla 'ordenes_items' encontrada con {count} registros.\n")
    else:
        print(
            "  ⚠  Tabla 'ordenes_items' vacía. Carga datos reales desde Mercado Público\n"
            "     usando extractor.py o extractor_masivo.py antes de ejecutar consultas.\n"
        )

    return count


# ────────────── Consulta 1: Top 5 compras más caras ──────────── #

QUERY_TOP_5_CARAS: str = """
    SELECT
        codigo_oc       AS "Código OC",
        nombre_producto AS "Producto",
        cantidad        AS "Cant.",
        precio_unitario AS "Precio Unit. ($)",
        monto_total_item AS "Monto Total ($)",
        nombre_proveedor AS "Proveedor",
        rut_proveedor   AS "RUT Proveedor"
    FROM ordenes_items
    ORDER BY precio_unitario DESC
    LIMIT 5
"""


# ────── Consulta 2: Top 3 proveedores con más adjudicaciones ──── #

QUERY_TOP_3_PROVEEDORES: str = """
    SELECT
        rut_proveedor     AS "RUT Proveedor",
        nombre_proveedor  AS "Proveedor",
        COUNT(*)          AS "N° Adjudicaciones",
        SUM(monto_total_item) AS "Monto Total Acum. ($)",
        ROUND(AVG(precio_unitario), 0) AS "Precio Unit. Prom. ($)"
    FROM ordenes_items
    GROUP BY rut_proveedor
    ORDER BY SUM(monto_total_item) DESC
    LIMIT 3
"""


# ──── Consulta 3: Productos con mayor diferencia min-max ─────── #

QUERY_DISPERSION_PRECIOS: str = """
    SELECT
        nombre_producto AS "Producto",
        COUNT(*)        AS "N° Compras",
        MIN(precio_unitario) AS "Precio Mín. ($)",
        MAX(precio_unitario) AS "Precio Máx. ($)",
        (MAX(precio_unitario) - MIN(precio_unitario)) AS "Diferencia ($)",
        ROUND(
            CAST((MAX(precio_unitario) - MIN(precio_unitario)) AS REAL)
            / NULLIF(MIN(precio_unitario), 0) * 100, 1
        ) AS "Variación (%)"
    FROM ordenes_items
    GROUP BY nombre_producto
    HAVING COUNT(*) > 1
    ORDER BY (MAX(precio_unitario) - MIN(precio_unitario)) DESC
"""


# ═══════════════════════════════════════════════════════════════ #
#                     EJECUCIÓN PRINCIPAL                        #
# ═══════════════════════════════════════════════════════════════ #

def ejecutar_consulta(conn: sqlite3.Connection, titulo: str, query: str) -> pd.DataFrame:
    """Ejecuta una consulta SQL y la muestra formateada en terminal."""
    try:
        df: pd.DataFrame = pd.read_sql_query(query, conn)
        print(titulo)
        print("─" * len(titulo.strip()))

        if df.empty:
            print("  (Sin resultados)\n")
            return df

        # Formatear montos con separador de miles
        for col in df.columns:
            if "($)" in col:
                df[col] = df[col].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "N/A")
            if "(%)" in col:
                df[col] = df[col].apply(lambda x: f"{x:,.1f}%" if pd.notna(x) else "N/A")

        print(df.to_string(index=False))
        print()
        return df

    except sqlite3.Error as exc:
        print(f"  ❌ Error ejecutando consulta: {exc}\n")
        return pd.DataFrame()


def main() -> None:
    """Pipeline principal del explorador forense."""

    print("\n" + "═" * 66)
    print("  🔎  EXPLORADOR SQL FORENSE — COMPRAS PÚBLICAS DE CHILE")
    print("═" * 66)
    print(f"  Base de datos: {DB_PATH.resolve()}\n")

    # ── Conectar a la BD ──
    try:
        conn: sqlite3.Connection = sqlite3.connect(DB_PATH)
    except sqlite3.Error as exc:
        print(f"  ❌ No se pudo conectar a la BD: {exc}")
        sys.exit(1)

    try:
        # ── Verificar datos ──
        total: int = verificar_o_crear_datos(conn)

        # Ajustar ancho de Pandas para terminal
        pd.set_option("display.width", 140)
        pd.set_option("display.max_colwidth", 40)

        # ── Consulta 1 ──
        ejecutar_consulta(
            conn,
            "  📊 [1/3] TOP 5 COMPRAS MÁS CARAS (por precio unitario)",
            QUERY_TOP_5_CARAS,
        )

        # ── Consulta 2 ──
        ejecutar_consulta(
            conn,
            "  🏢 [2/3] TOP 3 PROVEEDORES CON MAYOR MONTO ADJUDICADO",
            QUERY_TOP_3_PROVEEDORES,
        )

        # ── Consulta 3 ──
        ejecutar_consulta(
            conn,
            "  ⚖️  [3/3] PRODUCTOS CON MAYOR DISPERSIÓN DE PRECIOS (posible sobreprecio)",
            QUERY_DISPERSION_PRECIOS,
        )

        # ── Resumen ──
        print("─" * 66)
        print(f"  📋 Total de registros analizados: {total}")
        print("  💡 Los productos con alta variación (%) son candidatos a auditoría.")
        print("═" * 66 + "\n")

    except Exception as exc:
        print(f"\n  ❌ Error inesperado: {exc}")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
