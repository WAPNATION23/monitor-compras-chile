"""
Fixtures compartidas para tests.
Mock data realista de compras públicas chilenas.
"""

from __future__ import annotations

MOCK_DATA: list[dict] = [
    {
        "codigo_oc": "3401-120-SE26", "nombre_producto": "MASCARILLAS DESECHABLES 3 PLIEGUES",
        "categoria": "Insumos médicos", "cantidad": 5000, "precio_unitario": 150,
        "monto_total_item": 750_000, "rut_comprador": "61.602.000-0",
        "nombre_comprador": "HOSPITAL SAN JUAN DE DIOS", "rut_proveedor": "76.123.456-7",
        "nombre_proveedor": "IMPORTADORA MEDICAL SpA", "fecha_creacion": "2026-03-10", "estado": "6",
        "tipo_oc": "SE", "categoria_riesgo": "GENERAL",
    },
    {
        "codigo_oc": "3401-121-SE26", "nombre_producto": "MASCARILLAS DESECHABLES 3 PLIEGUES",
        "categoria": "Insumos médicos", "cantidad": 2000, "precio_unitario": 180,
        "monto_total_item": 360_000, "rut_comprador": "61.602.000-0",
        "nombre_comprador": "HOSPITAL SAN JUAN DE DIOS", "rut_proveedor": "76.234.567-8",
        "nombre_proveedor": "DISTRIBUIDORA SALUD LTDA", "fecha_creacion": "2026-03-11", "estado": "6",
        "tipo_oc": "SE", "categoria_riesgo": "GENERAL",
    },
    {
        "codigo_oc": "2205-045-SE26", "nombre_producto": "MASCARILLAS DESECHABLES 3 PLIEGUES",
        "categoria": "Insumos médicos", "cantidad": 500, "precio_unitario": 2_800,
        "monto_total_item": 1_400_000, "rut_comprador": "61.980.000-7",
        "nombre_comprador": "SEREMI DE SALUD VALPARAÍSO", "rut_proveedor": "76.999.888-K",
        "nombre_proveedor": "INVERSIONES FANTASMA SpA", "fecha_creacion": "2026-03-12", "estado": "6",
        "tipo_oc": "SE", "categoria_riesgo": "GENERAL",
    },
    {
        "codigo_oc": "2205-099-SE26", "nombre_producto": "MASCARILLAS DESECHABLES 3 PLIEGUES",
        "categoria": "Insumos médicos", "cantidad": 300, "precio_unitario": 4_500,
        "monto_total_item": 1_350_000, "rut_comprador": "61.980.000-7",
        "nombre_comprador": "SEREMI DE SALUD VALPARAÍSO", "rut_proveedor": "76.999.888-K",
        "nombre_proveedor": "INVERSIONES FANTASMA SpA", "fecha_creacion": "2026-03-14", "estado": "6",
        "tipo_oc": "SE", "categoria_riesgo": "GENERAL",
    },
    {
        "codigo_oc": "7310-200-SE26", "nombre_producto": "RESMA DE PAPEL CARTA 75G 500 HOJAS",
        "categoria": "Útiles de oficina", "cantidad": 100, "precio_unitario": 3_200,
        "monto_total_item": 320_000, "rut_comprador": "60.805.000-4",
        "nombre_comprador": "MUNICIPALIDAD DE PROVIDENCIA", "rut_proveedor": "77.111.222-3",
        "nombre_proveedor": "COMERCIAL OFFICE CENTER LTDA", "fecha_creacion": "2026-03-10", "estado": "6",
        "tipo_oc": "SE", "categoria_riesgo": "MUNICIPALIDAD",
    },
    {
        "codigo_oc": "7310-201-SE26", "nombre_producto": "RESMA DE PAPEL CARTA 75G 500 HOJAS",
        "categoria": "Útiles de oficina", "cantidad": 200, "precio_unitario": 3_800,
        "monto_total_item": 760_000, "rut_comprador": "69.070.700-7",
        "nombre_comprador": "REGISTRO CIVIL", "rut_proveedor": "77.111.222-3",
        "nombre_proveedor": "COMERCIAL OFFICE CENTER LTDA", "fecha_creacion": "2026-03-11", "estado": "6",
        "tipo_oc": "SE", "categoria_riesgo": "GENERAL",
    },
    {
        "codigo_oc": "7310-305-SE26", "nombre_producto": "RESMA DE PAPEL CARTA 75G 500 HOJAS",
        "categoria": "Útiles de oficina", "cantidad": 50, "precio_unitario": 18_900,
        "monto_total_item": 945_000, "rut_comprador": "61.601.000-5",
        "nombre_comprador": "MUNICIPALIDAD DE LAS CONDES", "rut_proveedor": "76.999.888-K",
        "nombre_proveedor": "INVERSIONES FANTASMA SpA", "fecha_creacion": "2026-03-15", "estado": "6",
        "tipo_oc": "SE", "categoria_riesgo": "MUNICIPALIDAD",
    },
    {
        "codigo_oc": "7310-210-SE26", "nombre_producto": "TÓNER HP 85A ORIGINAL",
        "categoria": "Insumos de impresión", "cantidad": 10, "precio_unitario": 45_000,
        "monto_total_item": 450_000, "rut_comprador": "60.805.000-4",
        "nombre_comprador": "MUNICIPALIDAD DE PROVIDENCIA", "rut_proveedor": "77.333.444-5",
        "nombre_proveedor": "TECNOPRINT S.A.", "fecha_creacion": "2026-03-13", "estado": "6",
        "tipo_oc": "SE", "categoria_riesgo": "MUNICIPALIDAD",
    },
    {
        "codigo_oc": "6100-050-SE26", "nombre_producto": "NOTEBOOK LENOVO THINKPAD L14 I5 16GB",
        "categoria": "Equipos computacionales", "cantidad": 25, "precio_unitario": 689_000,
        "monto_total_item": 17_225_000, "rut_comprador": "69.070.700-7",
        "nombre_comprador": "REGISTRO CIVIL", "rut_proveedor": "76.555.666-1",
        "nombre_proveedor": "SOLUCIONES TECH SpA", "fecha_creacion": "2026-03-09", "estado": "12",
        "tipo_oc": "SE", "categoria_riesgo": "GENERAL",
    },
    {
        "codigo_oc": "6100-080-SE26", "nombre_producto": "SILLA ERGONÓMICA CON APOYABRAZOS",
        "categoria": "Mobiliario", "cantidad": 15, "precio_unitario": 189_000,
        "monto_total_item": 2_835_000, "rut_comprador": "61.601.000-5",
        "nombre_comprador": "MUNICIPALIDAD DE LAS CONDES", "rut_proveedor": "76.555.666-1",
        "nombre_proveedor": "SOLUCIONES TECH SpA", "fecha_creacion": "2026-03-12", "estado": "6",
        "tipo_oc": "SE", "categoria_riesgo": "MUNICIPALIDAD",
    },
    # Datos extra para alcanzar MIN_OBSERVATIONS=5 en mascarillas
    {
        "codigo_oc": "3401-122-SE26", "nombre_producto": "MASCARILLAS DESECHABLES 3 PLIEGUES",
        "categoria": "Insumos médicos", "cantidad": 3000, "precio_unitario": 160,
        "monto_total_item": 480_000, "rut_comprador": "61.601.000-5",
        "nombre_comprador": "MUNICIPALIDAD DE LAS CONDES", "rut_proveedor": "76.123.456-7",
        "nombre_proveedor": "IMPORTADORA MEDICAL SpA", "fecha_creacion": "2026-03-13", "estado": "6",
        "tipo_oc": "SE", "categoria_riesgo": "MUNICIPALIDAD",
    },
    {
        "codigo_oc": "3401-123-SE26", "nombre_producto": "MASCARILLAS DESECHABLES 3 PLIEGUES",
        "categoria": "Insumos médicos", "cantidad": 1000, "precio_unitario": 170,
        "monto_total_item": 170_000, "rut_comprador": "61.601.000-5",
        "nombre_comprador": "MUNICIPALIDAD DE LAS CONDES", "rut_proveedor": "76.234.567-8",
        "nombre_proveedor": "DISTRIBUIDORA SALUD LTDA", "fecha_creacion": "2026-03-14", "estado": "6",
        "tipo_oc": "SE", "categoria_riesgo": "MUNICIPALIDAD",
    },
    {
        "codigo_oc": "3401-124-SE26", "nombre_producto": "MASCARILLAS DESECHABLES 3 PLIEGUES",
        "categoria": "Insumos médicos", "cantidad": 4000, "precio_unitario": 155,
        "monto_total_item": 620_000, "rut_comprador": "69.070.700-7",
        "nombre_comprador": "REGISTRO CIVIL", "rut_proveedor": "76.123.456-7",
        "nombre_proveedor": "IMPORTADORA MEDICAL SpA", "fecha_creacion": "2026-03-15", "estado": "6",
        "tipo_oc": "SE", "categoria_riesgo": "GENERAL",
    },
    {
        "codigo_oc": "3401-125-SE26", "nombre_producto": "MASCARILLAS DESECHABLES 3 PLIEGUES",
        "categoria": "Insumos médicos", "cantidad": 2500, "precio_unitario": 165,
        "monto_total_item": 412_500, "rut_comprador": "60.805.000-4",
        "nombre_comprador": "MUNICIPALIDAD DE PROVIDENCIA", "rut_proveedor": "76.234.567-8",
        "nombre_proveedor": "DISTRIBUIDORA SALUD LTDA", "fecha_creacion": "2026-03-16", "estado": "6",
        "tipo_oc": "SE", "categoria_riesgo": "MUNICIPALIDAD",
    },
    # OC extra para MUNICIPALIDAD DE LAS CONDES (necesita >= 3 OC para ranking riesgo)
    {
        "codigo_oc": "6100-081-SE26", "nombre_producto": "MONITOR LED 24 PULGADAS",
        "categoria": "Equipos computacionales", "cantidad": 10, "precio_unitario": 120_000,
        "monto_total_item": 1_200_000, "rut_comprador": "61.601.000-5",
        "nombre_comprador": "MUNICIPALIDAD DE LAS CONDES", "rut_proveedor": "76.555.666-1",
        "nombre_proveedor": "SOLUCIONES TECH SpA", "fecha_creacion": "2026-03-16", "estado": "6",
        "tipo_oc": "SE", "categoria_riesgo": "MUNICIPALIDAD",
    },
]

CREATE_TABLE_SQL: str = """
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
    UNIQUE(codigo_oc, nombre_producto, precio_unitario)
);
"""
