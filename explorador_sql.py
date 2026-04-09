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

# ──────────────────── Mock data realista ──────────────────── #
# Incluye sobreprecios intencionales en MASCARILLAS y RESMAS DE PAPEL
# para validar las consultas forenses.

MOCK_DATA: list[dict] = [
    # ── Mascarillas: precio normal ~$150, sobreprecio ~$2.800 y $4.500 ──
    {
        "codigo_oc": "3401-120-SE26", "nombre_producto": "MASCARILLAS DESECHABLES 3 PLIEGUES",
        "categoria": "Insumos médicos", "cantidad": 5000, "precio_unitario": 150,
        "monto_total_item": 750_000, "rut_comprador": "61.602.000-0",
        "nombre_comprador": "HOSPITAL SAN JUAN DE DIOS", "rut_proveedor": "76.123.456-7",
        "nombre_proveedor": "IMPORTADORA MEDICAL SpA", "fecha_creacion": "2026-03-10", "estado": "6",
    },
    {
        "codigo_oc": "3401-121-SE26", "nombre_producto": "MASCARILLAS DESECHABLES 3 PLIEGUES",
        "categoria": "Insumos médicos", "cantidad": 2000, "precio_unitario": 180,
        "monto_total_item": 360_000, "rut_comprador": "61.602.000-0",
        "nombre_comprador": "HOSPITAL SAN JUAN DE DIOS", "rut_proveedor": "76.234.567-8",
        "nombre_proveedor": "DISTRIBUIDORA SALUD LTDA", "fecha_creacion": "2026-03-11", "estado": "6",
    },
    {   # 🚨 SOBREPRECIO x18
        "codigo_oc": "2205-045-SE26", "nombre_producto": "MASCARILLAS DESECHABLES 3 PLIEGUES",
        "categoria": "Insumos médicos", "cantidad": 500, "precio_unitario": 2_800,
        "monto_total_item": 1_400_000, "rut_comprador": "61.980.000-7",
        "nombre_comprador": "SEREMI DE SALUD VALPARAÍSO", "rut_proveedor": "76.999.888-K",
        "nombre_proveedor": "INVERSIONES FANTASMA SpA", "fecha_creacion": "2026-03-12", "estado": "6",
    },
    {   # 🚨 SOBREPRECIO x30
        "codigo_oc": "2205-099-SE26", "nombre_producto": "MASCARILLAS DESECHABLES 3 PLIEGUES",
        "categoria": "Insumos médicos", "cantidad": 300, "precio_unitario": 4_500,
        "monto_total_item": 1_350_000, "rut_comprador": "61.980.000-7",
        "nombre_comprador": "SEREMI DE SALUD VALPARAÍSO", "rut_proveedor": "76.999.888-K",
        "nombre_proveedor": "INVERSIONES FANTASMA SpA", "fecha_creacion": "2026-03-14", "estado": "6",
    },
    # ── Resmas de papel: precio normal ~$3.500, sobreprecio ~$18.900 ──
    {
        "codigo_oc": "7310-200-SE26", "nombre_producto": "RESMA DE PAPEL CARTA 75G 500 HOJAS",
        "categoria": "Útiles de oficina", "cantidad": 100, "precio_unitario": 3_200,
        "monto_total_item": 320_000, "rut_comprador": "60.805.000-4",
        "nombre_comprador": "MUNICIPALIDAD DE PROVIDENCIA", "rut_proveedor": "77.111.222-3",
        "nombre_proveedor": "COMERCIAL OFFICE CENTER LTDA", "fecha_creacion": "2026-03-10", "estado": "6",
    },
    {
        "codigo_oc": "7310-201-SE26", "nombre_producto": "RESMA DE PAPEL CARTA 75G 500 HOJAS",
        "categoria": "Útiles de oficina", "cantidad": 200, "precio_unitario": 3_800,
        "monto_total_item": 760_000, "rut_comprador": "69.070.700-7",
        "nombre_comprador": "REGISTRO CIVIL", "rut_proveedor": "77.111.222-3",
        "nombre_proveedor": "COMERCIAL OFFICE CENTER LTDA", "fecha_creacion": "2026-03-11", "estado": "6",
    },
    {   # 🚨 SOBREPRECIO x5
        "codigo_oc": "7310-305-SE26", "nombre_producto": "RESMA DE PAPEL CARTA 75G 500 HOJAS",
        "categoria": "Útiles de oficina", "cantidad": 50, "precio_unitario": 18_900,
        "monto_total_item": 945_000, "rut_comprador": "61.601.000-5",
        "nombre_comprador": "MUNICIPALIDAD DE LAS CONDES", "rut_proveedor": "76.999.888-K",
        "nombre_proveedor": "INVERSIONES FANTASMA SpA", "fecha_creacion": "2026-03-15", "estado": "6",
    },
    # ── Tóner impresora: precio razonable ────
    {
        "codigo_oc": "7310-210-SE26", "nombre_producto": "TÓNER HP 85A ORIGINAL",
        "categoria": "Insumos de impresión", "cantidad": 10, "precio_unitario": 45_000,
        "monto_total_item": 450_000, "rut_comprador": "60.805.000-4",
        "nombre_comprador": "MUNICIPALIDAD DE PROVIDENCIA", "rut_proveedor": "77.333.444-5",
        "nombre_proveedor": "TECNOPRINT S.A.", "fecha_creacion": "2026-03-13", "estado": "6",
    },
    # ── Notebook: compra grande ────
    {
        "codigo_oc": "6100-050-SE26", "nombre_producto": "NOTEBOOK LENOVO THINKPAD L14 I5 16GB",
        "categoria": "Equipos computacionales", "cantidad": 25, "precio_unitario": 689_000,
        "monto_total_item": 17_225_000, "rut_comprador": "69.070.700-7",
        "nombre_comprador": "REGISTRO CIVIL", "rut_proveedor": "76.555.666-1",
        "nombre_proveedor": "SOLUCIONES TECH SpA", "fecha_creacion": "2026-03-09", "estado": "12",
    },
    # ── Silla ergonómica ────
    {
        "codigo_oc": "6100-080-SE26", "nombre_producto": "SILLA ERGONÓMICA CON APOYABRAZOS",
        "categoria": "Mobiliario", "cantidad": 15, "precio_unitario": 189_000,
        "monto_total_item": 2_835_000, "rut_comprador": "61.601.000-5",
        "nombre_comprador": "MUNICIPALIDAD DE LAS CONDES", "rut_proveedor": "76.555.666-1",
        "nombre_proveedor": "SOLUCIONES TECH SpA", "fecha_creacion": "2026-03-12", "estado": "6",
    },
]


# ═══════════════════════════════════════════════════════════════ #
#                     FUNCIONES PRINCIPALES                      #
# ═══════════════════════════════════════════════════════════════ #


def verificar_o_crear_datos(conn: sqlite3.Connection) -> int:
    """
    Verifica si la tabla ordenes_items existe y tiene datos.
    Si no, la crea e inserta mock data.

    Returns:
        Cantidad de registros en la tabla.
    """
    # Crear tabla si no existe
    conn.execute(_CREATE_TABLE_SQL)
    conn.commit()

    # Contar registros existentes
    cursor = conn.execute("SELECT COUNT(*) FROM ordenes_items")
    count: int = cursor.fetchone()[0]

    if count > 0:
        print(f"  ✅ Tabla 'ordenes_items' encontrada con {count} registros reales.\n")
        return count

    # ── Insertar mock data ──
    print("  ⚠  Tabla vacía. Insertando 10 registros de prueba (mock data)...\n")
    df_mock: pd.DataFrame = pd.DataFrame(MOCK_DATA)
    df_mock.to_sql("ordenes_items", conn, if_exists="append", index=False)
    conn.commit()
    print(f"  ✅ {len(MOCK_DATA)} registros ficticios insertados correctamente.\n")
    return len(MOCK_DATA)


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
