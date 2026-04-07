"""
Tests para Monitor Compras Chile.
Usa mock data realista para validar los módulos core.

Ejecutar:
    pytest tests/ -v
"""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pandas as pd
import pytest

# ──────── Fixtures ──────── #

MOCK_DATA = [
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

CREATE_TABLE_SQL = """
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


@pytest.fixture
def test_db(tmp_path):
    """Crea una BD temporal con mock data y retorna su path."""
    db_path = tmp_path / "test_auditoria.db"
    conn = sqlite3.connect(db_path)
    conn.execute(CREATE_TABLE_SQL)
    df = pd.DataFrame(MOCK_DATA)
    df.to_sql("ordenes_items", conn, if_exists="append", index=False)
    conn.commit()
    conn.close()
    return db_path


# ══════════════════════════════════════════════
# Tests: Processor
# ══════════════════════════════════════════════

class TestProcessor:
    def test_classify_risk_municipalidad(self):
        from processor import DataProcessor
        assert DataProcessor._classify_risk("MUNICIPALIDAD DE PROVIDENCIA") == "MUNICIPALIDAD"

    def test_classify_risk_ffaa(self):
        from processor import DataProcessor
        assert DataProcessor._classify_risk("CARABINEROS DE CHILE") == "FUERZAS ARMADAS/ORDEN"

    def test_classify_risk_general(self):
        from processor import DataProcessor
        assert DataProcessor._classify_risk("REGISTRO CIVIL") == "GENERAL"

    def test_classify_risk_empty(self):
        from processor import DataProcessor
        assert DataProcessor._classify_risk("") == "GENERAL"

    def test_extract_tipo_oc(self):
        from processor import DataProcessor
        assert DataProcessor._extract_tipo_oc("2097-241-SE14") == "SE"
        assert DataProcessor._extract_tipo_oc("3401-120-CM26") == "CM"
        assert DataProcessor._extract_tipo_oc("") == ""

    def test_flatten_oc(self):
        from processor import DataProcessor
        oc = {
            "Codigo": "TEST-001-SE26",
            "CodigoEstado": 6,
            "FechaCreacion": "2026-03-15",
            "Comprador": {"RutUnidad": "61.602.000-0", "NombreUnidad": "HOSPITAL TEST"},
            "Proveedor": {"RutProveedor": "76.111.222-3", "Nombre": "PROVEEDOR TEST"},
            "Items": {
                "Cantidad": 2,
                "Listado": [
                    {"NombreProducto": "PRODUCTO A", "Cantidad": 10, "PrecioNeto": 1000, "Categoria": "Test"},
                    {"NombreProducto": "PRODUCTO B", "Cantidad": 5, "PrecioNeto": 2000, "Categoria": "Test"},
                ],
            },
        }
        rows = DataProcessor._flatten_oc(oc)
        assert len(rows) == 2
        assert rows[0]["codigo_oc"] == "TEST-001-SE26"
        assert rows[0]["precio_unitario"] == 1000
        assert rows[0]["monto_total_item"] == 10000
        assert rows[1]["precio_unitario"] == 2000

    def test_process_and_store(self, tmp_path):
        from processor import DataProcessor
        db_path = tmp_path / "proc_test.db"
        proc = DataProcessor(db_path=db_path)
        ordenes = [
            {
                "Codigo": "TEST-002-SE26",
                "CodigoEstado": 6,
                "FechaCreacion": "2026-03-15",
                "Comprador": {"RutUnidad": "61.602.000-0", "NombreUnidad": "MUNICIPALIDAD DE TEST"},
                "Proveedor": {"RutProveedor": "76.111.222-3", "Nombre": "PROVEEDOR X"},
                "Items": {
                    "Cantidad": 1,
                    "Listado": [
                        {"NombreProducto": "Papel A4", "Cantidad": 100, "PrecioNeto": 3500, "Categoria": "Oficina"},
                    ],
                },
            },
        ]
        df, inserted = proc.process_and_store(ordenes)
        assert len(df) == 1
        assert inserted == 1
        assert df.iloc[0]["precio_unitario"] == 3500

        # Verificar en la BD
        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM ordenes_items").fetchone()[0]
        conn.close()
        assert count == 1

    def test_migrate_unique_constraint(self, tmp_path):
        """Opening a DB with the old 3-column UNIQUE should rebuild to 4-column."""
        db_path = tmp_path / "old_schema.db"
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE ordenes_items (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                codigo_oc        TEXT NOT NULL,
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
                tipo_oc          TEXT DEFAULT '',
                categoria_riesgo TEXT DEFAULT 'GENERAL',
                fecha_ingreso    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(codigo_oc, nombre_producto, precio_unitario)
            )
        """)
        # Insert two rows that differ only in cantidad — old schema rejects this
        conn.execute(
            "INSERT INTO ordenes_items (codigo_oc, nombre_producto, precio_unitario, cantidad) "
            "VALUES ('OC-1', 'ITEM', 100, 5)"
        )
        try:
            conn.execute(
                "INSERT INTO ordenes_items (codigo_oc, nombre_producto, precio_unitario, cantidad) "
                "VALUES ('OC-1', 'ITEM', 100, 10)"
            )
            assert False, "Old schema should reject duplicate"
        except sqlite3.IntegrityError:
            pass
        conn.commit()
        conn.close()

        # DataProcessor should migrate the constraint on init
        from processor import DataProcessor
        proc = DataProcessor(db_path=db_path)

        # Now the same insert should succeed
        conn2 = sqlite3.connect(db_path)
        conn2.execute(
            "INSERT INTO ordenes_items (codigo_oc, nombre_producto, precio_unitario, cantidad) "
            "VALUES ('OC-1', 'ITEM', 100, 10)"
        )
        conn2.commit()
        count = conn2.execute("SELECT COUNT(*) FROM ordenes_items").fetchone()[0]
        conn2.close()
        assert count == 2

    def test_skips_cancelled(self, tmp_path):
        from processor import DataProcessor
        db_path = tmp_path / "cancel_test.db"
        proc = DataProcessor(db_path=db_path)
        ordenes = [
            {
                "Codigo": "CANCEL-001-SE26",
                "CodigoEstado": 9,  # Cancelada
                "FechaCreacion": "2026-03-15",
                "Comprador": {"RutUnidad": "61.602.000-0", "NombreUnidad": "TEST"},
                "Proveedor": {"RutProveedor": "76.111.222-3", "Nombre": "PROV"},
                "Items": {"Cantidad": 1, "Listado": [{"NombreProducto": "X", "Cantidad": 1, "PrecioNeto": 100, "Categoria": "Y"}]},
            },
        ]
        df, inserted = proc.process_and_store(ordenes)
        assert df.empty
        assert inserted == 0


# ══════════════════════════════════════════════
# Tests: Infiltrador IA
# ══════════════════════════════════════════════

class TestInfiltradorIA:
    def test_infiltrar_rut_no_results(self, tmp_path, monkeypatch):
        """When API returns empty Listado, infiltrar_rut returns 0."""
        from infiltrador_ia import infiltrar_rut

        # Patch the extractor to return empty
        monkeypatch.setattr(
            "infiltrador_ia.MercadoPublicoExtractor._get_with_retry",
            lambda self, url, params: {"Listado": []},
        )
        monkeypatch.setattr(
            "infiltrador_ia.DataProcessor.__init__",
            lambda self: None,
        )
        result = infiltrar_rut("76.111.222-3")
        assert result == 0

    def test_infiltrar_rut_processes_results(self, tmp_path, monkeypatch):
        """When API returns OCs, process_and_store is called and inserted count returned."""
        from infiltrador_ia import infiltrar_rut
        from processor import DataProcessor

        detail = {
            "Codigo": "TEST-001-D126",
            "CodigoEstado": 6,
            "Fechas": {"FechaCreacion": "2026-01-10"},
            "Comprador": {"RutUnidad": "61.602.000-0", "NombreUnidad": "MUNICIPALIDAD X"},
            "Proveedor": {"RutProveedor": "76.111.222-3", "Nombre": "PROV TEST"},
            "Items": {
                "Cantidad": 1,
                "Listado": [{"NombreProducto": "Widget", "Cantidad": 5, "PrecioNeto": 1000, "Categoria": "Test"}],
            },
        }

        # Create the real processor BEFORE monkeypatching __init__
        db_path = tmp_path / "infil_test.db"
        real_proc = DataProcessor(db_path=db_path)
        # Save bound method reference before patching the class
        original_process = real_proc.process_and_store.__func__

        monkeypatch.setattr(
            "infiltrador_ia.MercadoPublicoExtractor._get_with_retry",
            lambda self, url, params: {"Listado": [{"Codigo": "TEST-001-D126"}]},
        )
        monkeypatch.setattr(
            "infiltrador_ia.MercadoPublicoExtractor._fetch_oc_detail",
            lambda self, codigo: detail,
        )
        monkeypatch.setattr(
            "infiltrador_ia.DataProcessor.__init__",
            lambda self: setattr(self, "db_path", db_path) or None,
        )
        monkeypatch.setattr(
            "infiltrador_ia.DataProcessor.process_and_store",
            lambda self, ordenes: original_process(real_proc, ordenes),
        )

        result = infiltrar_rut("76.111.222-3")
        assert result == 1


# ══════════════════════════════════════════════
# Tests: Detector
# ══════════════════════════════════════════════

class TestDetector:
    def test_detect_iqr_finds_overpriced_masks(self, test_db):
        from detector import AnomalyDetector
        det = AnomalyDetector(db_path=test_db)
        anomalies = det.detect(method="iqr")
        assert not anomalies.empty
        # Mascarillas a $2800 y $4500 deben saltar
        mask_products = anomalies[anomalies["nombre_producto"] == "MASCARILLAS DESECHABLES 3 PLIEGUES"]
        assert len(mask_products) >= 1

    def test_detect_zscore_finds_outliers(self, test_db):
        from detector import AnomalyDetector
        det = AnomalyDetector(db_path=test_db)
        anomalies = det.detect(method="zscore")
        assert not anomalies.empty

    def test_detect_serenata_runs_all(self, test_db):
        from detector import AnomalyDetector
        det = AnomalyDetector(db_path=test_db)
        anomalies = det.detect(method="serenata")
        # Serenata should find at least statistical anomalies
        assert not anomalies.empty

    def test_detect_empty_db(self, tmp_path):
        from detector import AnomalyDetector
        db_path = tmp_path / "empty.db"
        conn = sqlite3.connect(db_path)
        conn.execute(CREATE_TABLE_SQL)
        conn.commit()
        conn.close()

        det = AnomalyDetector(db_path=db_path)
        anomalies = det.detect(method="iqr")
        assert anomalies.empty

    def test_spider_detects_round_consultancy(self, test_db):
        """Verifica que Red de Araña detecte asesorías millonarias redondas."""
        from detector import AnomalyDetector

        # Insertar una asesoría sospechosa
        conn = sqlite3.connect(test_db)
        conn.execute(
            "INSERT INTO ordenes_items (codigo_oc, nombre_producto, categoria, cantidad, "
            "precio_unitario, monto_total_item, rut_comprador, nombre_comprador, "
            "rut_proveedor, nombre_proveedor, fecha_creacion, estado, tipo_oc, categoria_riesgo) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("SPIDER-001-D126", "ASESORÍA EN GESTIÓN ESTRATÉGICA", "Servicios", 1,
             10_000_000, 10_000_000, "61.601.000-5", "MUNICIPALIDAD DE LAS CONDES",
             "76.999.888-K", "INVERSIONES FANTASMA SpA", "2026-03-20", "6", "D1", "MUNICIPALIDAD"),
        )
        conn.commit()
        conn.close()

        det = AnomalyDetector(db_path=test_db)
        df = det._load_data()
        spider_result = det._detect_spider(df)
        assert not spider_result.empty


# ══════════════════════════════════════════════
# Tests: CrossReferencer
# ══════════════════════════════════════════════

class TestCrossReferencer:
    def test_concentracion_capital(self, test_db):
        from cross_referencer import CrossReferencer
        xref = CrossReferencer(db_path=test_db)
        result = xref.concentracion_capital(top_n=5)
        assert not result.empty
        assert "pct_del_total" in result.columns
        # El top proveedor debe tener el mayor porcentaje
        assert result.iloc[0]["pct_del_total"] > 0

    def test_ranking_riesgo_organismos(self, test_db):
        from cross_referencer import CrossReferencer
        xref = CrossReferencer(db_path=test_db)
        result = xref.ranking_riesgo_organismos()
        assert not result.empty
        assert "score_riesgo" in result.columns

    def test_ranking_proveedores_sospechosos(self, test_db):
        from cross_referencer import CrossReferencer
        xref = CrossReferencer(db_path=test_db)
        result = xref.ranking_proveedores_sospechosos(top_n=5)
        assert not result.empty
        assert "score_sospecha" in result.columns

    def test_reporte_ejecutivo(self, test_db):
        from cross_referencer import CrossReferencer
        xref = CrossReferencer(db_path=test_db)
        report = xref.reporte_ejecutivo()
        assert report["total_items"] == 15
        assert report["monto_total_clp"] > 0

    def test_servel_cruce_empty(self, test_db):
        """Sin tabla aportes_servel, debe retornar DataFrame vacío."""
        from cross_referencer import CrossReferencer
        xref = CrossReferencer(db_path=test_db)
        result = xref.cruce_servel_compras()
        assert result.empty


# ══════════════════════════════════════════════
# Tests: Config
# ══════════════════════════════════════════════

class TestConfig:
    def test_no_hardcoded_ticket(self):
        """Verifica que no haya un ticket hardcodeado como fallback."""
        from config import API_TICKET
        import os
        # Si no hay variable de entorno, debe ser string vacío
        if not os.getenv("MERCADO_PUBLICO_TICKET"):
            assert API_TICKET == ""


# ══════════════════════════════════════════════
# Tests: Queries
# ══════════════════════════════════════════════

class TestQueries:
    def test_load_data_returns_dataframe(self, test_db, monkeypatch):
        import queries
        monkeypatch.setattr(queries, "DB_PATH", str(test_db))
        df = queries.load_data()
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "codigo_oc" in df.columns

    def test_load_data_empty_db(self, tmp_path, monkeypatch):
        import queries
        db = tmp_path / "empty.db"
        sqlite3.connect(str(db)).close()
        monkeypatch.setattr(queries, "DB_PATH", str(db))
        df = queries.load_data()
        assert df.empty

    def test_load_data_filters_estado_9(self, test_db, monkeypatch):
        import queries
        monkeypatch.setattr(queries, "DB_PATH", str(test_db))
        # Insert a row with estado=9 (cancelled)
        with sqlite3.connect(str(test_db)) as conn:
            conn.execute(
                "INSERT INTO ordenes_items (codigo_oc, nombre_producto, cantidad, precio_unitario, "
                "monto_total_item, nombre_comprador, nombre_proveedor, estado, tipo_oc) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("CANCEL-001", "Producto Cancelado", 1, 100, 100, "Comprador", "Proveedor", "9", "SE"),
            )
        df = queries.load_data()
        assert "CANCEL-001" not in df["codigo_oc"].values

    def test_feedback_roundtrip(self, tmp_path, monkeypatch):
        import queries
        db = tmp_path / "feedback.db"
        monkeypatch.setattr(queries, "DB_PATH", str(db))
        queries.init_feedback_db()
        queries.save_feedback("Desfalco", "76.111.222-3", "Prueba de reporte")
        with sqlite3.connect(str(db)) as conn:
            rows = conn.execute("SELECT * FROM feedback_comunidad").fetchall()
        assert len(rows) == 1
        assert rows[0][2] == "76.111.222-3"

    def test_rate_limit_roundtrip(self, tmp_path, monkeypatch):
        import queries
        db = tmp_path / "rate.db"
        monkeypatch.setattr(queries, "DB_PATH", str(db))
        assert queries.get_rate_limit_usage("1.2.3.4", "2026-04-07") == 0
        queries.increment_rate_limit_usage("1.2.3.4", "2026-04-07")
        assert queries.get_rate_limit_usage("1.2.3.4", "2026-04-07") == 1
        queries.increment_rate_limit_usage("1.2.3.4", "2026-04-07")
        assert queries.get_rate_limit_usage("1.2.3.4", "2026-04-07") == 2

    def test_rate_limit_different_ips(self, tmp_path, monkeypatch):
        import queries
        db = tmp_path / "rate2.db"
        monkeypatch.setattr(queries, "DB_PATH", str(db))
        queries.increment_rate_limit_usage("1.1.1.1", "2026-04-07")
        queries.increment_rate_limit_usage("2.2.2.2", "2026-04-07")
        assert queries.get_rate_limit_usage("1.1.1.1", "2026-04-07") == 1
        assert queries.get_rate_limit_usage("2.2.2.2", "2026-04-07") == 1

    def test_load_licitaciones_no_table(self, tmp_path, monkeypatch):
        import queries
        db = tmp_path / "nolic.db"
        sqlite3.connect(str(db)).close()
        monkeypatch.setattr(queries, "DB_PATH", str(db))
        assert queries.load_licitaciones().empty

    def test_format_clp_billions(self):
        from queries import format_clp
        assert "B CLP" in format_clp(5_900_000_000)

    def test_format_clp_millions(self):
        from queries import format_clp
        assert "M CLP" in format_clp(276_000_000)

    def test_format_clp_small(self):
        from queries import format_clp
        result = format_clp(50_000)
        assert "CLP" in result

    def test_format_clp_full(self):
        from queries import format_clp_full
        result = format_clp_full(276_000_000)
        assert "$" in result


# ══════════════════════════════════════════════
# Tests: ChatService
# ══════════════════════════════════════════════

class TestChatService:
    def test_extract_keywords(self):
        from chat_service import _extract_keywords
        words = _extract_keywords("Investiga la fundación Democracia Viva")
        assert "fundación" in words or "fundacion" in words
        assert "democracia" in words
        assert "viva" in words
        # stopwords excluded
        assert "investiga" not in words

    def test_extract_keywords_short_words_excluded(self):
        from chat_service import _extract_keywords
        words = _extract_keywords("el es un de al")
        assert words == []

    def test_build_db_context_no_keywords(self):
        from chat_service import build_db_context
        result = build_db_context("el de la")
        assert result == ""

    def test_build_db_context_with_data(self, test_db, monkeypatch):
        import chat_service
        monkeypatch.setattr(chat_service, "DB_PATH", str(test_db))
        result = chat_service.build_db_context("INVERSIONES FANTASMA")
        assert "INVERSIONES FANTASMA" in result
        assert "DATOS ENCONTRADOS" in result

    def test_build_db_context_no_match(self, test_db, monkeypatch):
        import chat_service
        monkeypatch.setattr(chat_service, "DB_PATH", str(test_db))
        result = chat_service.build_db_context("empresa inexistente xyz")
        # No provider matches, but comprador search runs too
        assert isinstance(result, str)

    def test_build_system_prompt_contains_date(self):
        from chat_service import build_system_prompt
        prompt = build_system_prompt("web ctx", "db ctx")
        assert "2026" in prompt
        assert "web ctx" in prompt
        assert "db ctx" in prompt
        assert "Ojo del Pueblo" in prompt

    def test_call_deepseek_no_api_key(self, monkeypatch):
        monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
        from chat_service import call_deepseek
        result = call_deepseek(
            [{"role": "user", "content": "test"}], "web", "db"
        )
        assert "DEEPSEEK_API_KEY" in result

    def test_call_deepseek_success(self, monkeypatch):
        monkeypatch.setenv("DEEPSEEK_API_KEY", "fake-key")
        import chat_service

        class MockResponse:
            status_code = 200
            def json(self):
                return {"choices": [{"message": {"content": "respuesta mock"}}]}

        monkeypatch.setattr(chat_service.requests, "post", lambda *a, **kw: MockResponse())
        result = chat_service.call_deepseek(
            [{"role": "user", "content": "test"}], "", ""
        )
        assert result == "respuesta mock"

    def test_call_deepseek_timeout(self, monkeypatch):
        monkeypatch.setenv("DEEPSEEK_API_KEY", "fake-key")
        import chat_service

        def raise_timeout(*a, **kw):
            raise chat_service.requests.exceptions.Timeout("timeout")

        monkeypatch.setattr(chat_service.requests, "post", raise_timeout)
        # Patch sleep to avoid waiting
        monkeypatch.setattr("time.sleep", lambda x: None)
        result = chat_service.call_deepseek(
            [{"role": "user", "content": "test"}], "", ""
        )
        assert "timeout" in result.lower()

    def test_call_deepseek_rate_limited(self, monkeypatch):
        monkeypatch.setenv("DEEPSEEK_API_KEY", "fake-key")
        import chat_service

        call_count = {"n": 0}

        class MockResponse429:
            status_code = 429

        class MockResponse200:
            status_code = 200
            def json(self):
                return {"choices": [{"message": {"content": "ok after retry"}}]}

        def mock_post(*a, **kw):
            call_count["n"] += 1
            if call_count["n"] <= 1:
                return MockResponse429()
            return MockResponse200()

        monkeypatch.setattr(chat_service.requests, "post", mock_post)
        monkeypatch.setattr("time.sleep", lambda x: None)
        result = chat_service.call_deepseek(
            [{"role": "user", "content": "test"}], "", ""
        )
        assert result == "ok after retry"
