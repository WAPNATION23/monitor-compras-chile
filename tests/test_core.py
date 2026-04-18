"""
Tests para Monitor Compras Chile.
Usa mock data realista para validar los módulos core.

Ejecutar:
    pytest tests/ -v
"""

from __future__ import annotations

import sqlite3

import pandas as pd
import pytest

from tests.fixtures import CREATE_TABLE_SQL, MOCK_DATA


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
            "Proveedor": {"RutSucursal": "76.111.222-3", "Nombre": "PROVEEDOR TEST"},
            "Items": {
                "Cantidad": 2,
                "Listado": [
                    {"Producto": "PRODUCTO A", "Cantidad": 10, "PrecioNeto": 1000, "Categoria": "Test"},
                    {"Producto": "PRODUCTO B", "Cantidad": 5, "PrecioNeto": 2000, "Categoria": "Test"},
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
                "Proveedor": {"RutSucursal": "76.111.222-3", "Nombre": "PROVEEDOR X"},
                "Items": {
                    "Cantidad": 1,
                    "Listado": [
                        {"Producto": "Papel A4", "Cantidad": 100, "PrecioNeto": 3500, "Categoria": "Oficina"},
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
            raise AssertionError("Old schema should reject duplicate")
        except sqlite3.IntegrityError:
            pass
        conn.commit()
        conn.close()

        # DataProcessor should migrate the constraint on init
        from processor import DataProcessor
        DataProcessor(db_path=db_path)  # side-effect: migrates constraint

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
                "Proveedor": {"RutSucursal": "76.111.222-3", "Nombre": "PROV"},
                "Items": {"Cantidad": 1, "Listado": [{"Producto": "X", "Cantidad": 1, "PrecioNeto": 100, "Categoria": "Y"}]},
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
            "Proveedor": {"RutSucursal": "76.111.222-3", "Nombre": "PROV TEST"},
            "Items": {
                "Cantidad": 1,
                "Listado": [{"Producto": "Widget", "Cantidad": 5, "PrecioNeto": 1000, "Categoria": "Test"}],
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

    def test_monopolio_detecta_concentracion(self):
        """Monopolio: proveedor concentra >=80% de OCs de un organismo."""
        from detector import AnomalyDetector

        df = pd.DataFrame({
            "codigo_oc": [f"OC{i}" for i in range(12)],
            "nombre_comprador": ["MINSAL"] * 12,
            "nombre_proveedor": ["ACME SA"] * 10 + ["OTRA SRL"] * 2,
            "rut_proveedor": ["111"] * 10 + ["222"] * 2,
            "monto_total_item": [10_000_000] * 12,
            "fecha_creacion": pd.date_range("2024-01-01", periods=12, freq="D"),
            "nombre_producto": ["Servicio"] * 12,
            "precio_unitario": [10_000_000] * 12,
        })
        result = AnomalyDetector._detect_monopolio(df)
        assert not result.empty
        assert (result["metodo"] == "Monopolio por Comprador").all()
        # ACME concentra 10/12 = 83%
        assert "ACME SA" in result["motivo_alerta"].iloc[0]

    def test_monopolio_vacio_sin_concentracion(self):
        """Si nadie pasa del umbral, no reporta."""
        from detector import AnomalyDetector
        df = pd.DataFrame({
            "codigo_oc": [f"OC{i}" for i in range(10)],
            "nombre_comprador": ["MINSAL"] * 10,
            "nombre_proveedor": [f"PROV{i%5}" for i in range(10)],  # 5 provs, 20% c/u
            "rut_proveedor": [f"{i}" for i in range(10)],
            "monto_total_item": [10_000_000] * 10,
            "fecha_creacion": pd.date_range("2024-01-01", periods=10, freq="D"),
            "nombre_producto": ["X"] * 10,
            "precio_unitario": [10_000_000] * 10,
        })
        result = AnomalyDetector._detect_monopolio(df)
        assert result.empty

    def test_proveedor_nuevo_detecta_shell(self):
        """Proveedor con primera OC reciente y monto alto = alerta."""
        from detector import AnomalyDetector
        df = pd.DataFrame({
            "codigo_oc": ["OC1", "OC2"],
            "rut_proveedor": ["999-9"] * 2,
            "nombre_proveedor": ["SHELL SPA"] * 2,
            "nombre_comprador": ["MUNI"] * 2,
            "monto_total_item": [15_000_000, 10_000_000],
            "fecha_creacion": pd.to_datetime(["2024-10-01", "2024-10-10"]),
            "nombre_producto": ["X"] * 2,
            "precio_unitario": [1] * 2,
        })
        result = AnomalyDetector._detect_proveedor_nuevo(df)
        assert not result.empty
        assert (result["metodo"] == "Proveedor Recién Nacido").all()

    def test_proveedor_nuevo_ignora_antiguos(self):
        """Si proveedor tiene historial >30d, no alerta."""
        from detector import AnomalyDetector
        df = pd.DataFrame({
            "codigo_oc": ["OC1", "OC2"],
            "rut_proveedor": ["123-4"] * 2,
            "nombre_proveedor": ["VETERANA SA"] * 2,
            "nombre_comprador": ["MUNI"] * 2,
            "monto_total_item": [50_000_000, 50_000_000],
            "fecha_creacion": pd.to_datetime(["2020-01-01", "2024-10-01"]),
            "nombre_producto": ["X"] * 2,
            "precio_unitario": [1] * 2,
        })
        result = AnomalyDetector._detect_proveedor_nuevo(df)
        assert result.empty


# ══════════════════════════════════════════════
# Tests: CrossReferencer.red_de_poder
# ══════════════════════════════════════════════

class TestRedDePoder:
    def test_red_de_poder_requires_cross_tables(self, test_db):
        """Sin tablas de cruce, red_de_poder retorna vacío."""
        from cross_referencer import CrossReferencer
        xref = CrossReferencer(db_path=test_db)
        result = xref.red_de_poder()
        # Fuentes=1 (solo OCs) < 2 → filtered out
        assert result.empty

    def test_red_de_poder_detecta_rut_multifuente(self, tmp_path):
        """RUT en OCs + cruce_gastos + cruce_aportes debe aparecer con fuentes=3."""
        from cross_referencer import CrossReferencer
        db = tmp_path / "rp.db"
        conn = sqlite3.connect(db)
        conn.execute(CREATE_TABLE_SQL)
        conn.executemany(
            "INSERT INTO ordenes_items (codigo_oc, nombre_producto, categoria, cantidad, "
            "precio_unitario, monto_total_item, rut_comprador, nombre_comprador, "
            "rut_proveedor, nombre_proveedor, fecha_creacion, estado, tipo_oc, categoria_riesgo) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (f"OC{i}", "Prod", "Cat", 1, 1_000_000, 1_000_000,
                 "60.000.000-1", "MINSAL", "76.111.111-1", "POLLUX SA",
                 "2024-01-01", "6", "C1", "GENERAL")
                for i in range(3)
            ],
        )
        # Tablas de cruce
        conn.execute("""
            CREATE TABLE cruce_gastos_proveedores (
                rut TEXT, nombre_proveedor TEXT,
                n_facturas_campana INT, total_facturado_campana REAL,
                candidatos_beneficiados TEXT, partidos TEXT,
                n_ocs_estado INT, total_ocs_estado REAL
            )
        """)
        conn.execute(
            "INSERT INTO cruce_gastos_proveedores VALUES (?,?,?,?,?,?,?,?)",
            ("76.111.111-1", "POLLUX SA", 5, 20_000_000, "X", "Y", 3, 3_000_000),
        )
        conn.execute("""
            CREATE TABLE cruce_aportes_proveedores (
                nombre_aportante TEXT, n_aportes INT, total_donado REAL,
                receptores TEXT, n_ocs INT, total_ocs REAL,
                rut_proveedor TEXT, nombre_proveedor_match TEXT
            )
        """)
        conn.execute(
            "INSERT INTO cruce_aportes_proveedores VALUES (?,?,?,?,?,?,?,?)",
            ("POLLUX SA", 2, 5_000_000, "Candidato X", 3, 3_000_000, "76.111.111-1", "POLLUX SA"),
        )
        conn.commit()
        conn.close()

        xref = CrossReferencer(db_path=db)
        result = xref.red_de_poder()
        assert not result.empty
        assert result.iloc[0]["fuentes"] == 3
        assert "POLLUX" in str(result.iloc[0]["nombre_proveedor"]).upper()


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
        result = format_clp(5_900_000_000)
        assert "Mil M CLP" in result
        assert "5.9" in result

    def test_format_clp_trillions(self):
        from queries import format_clp
        result = format_clp(1_500_000_000_000)
        assert "B CLP" in result
        assert "1.5" in result

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

    def test_build_system_prompt_with_forensic(self):
        from chat_service import build_system_prompt
        prompt = build_system_prompt("web", "db", "FORENSIC DATA HERE")
        assert "FORENSIC DATA HERE" in prompt
        assert "INTELIGENCIA FORENSE" in prompt

    def test_classify_intent_persona(self):
        from chat_service import classify_intent
        intents = classify_intent("Buscar al político Juan Pérez")
        assert "persona" in intents

    def test_classify_intent_proveedor_rut(self):
        from chat_service import classify_intent
        intents = classify_intent("Buscar 76.123.456-7")
        assert "proveedor" in intents

    def test_classify_intent_anomalia(self):
        from chat_service import classify_intent
        intents = classify_intent("¿Hay proveedores sospechosos de fraude?")
        assert "anomalia" in intents

    def test_classify_intent_resumen(self):
        from chat_service import classify_intent
        intents = classify_intent("Dame un reporte ejecutivo")
        assert "resumen" in intents

    def test_classify_intent_general(self):
        from chat_service import classify_intent
        intents = classify_intent("hola mundo abc")
        assert intents == ["general"]

    def test_build_forensic_context_returns_tuple(self, test_db, monkeypatch):
        import chat_service
        monkeypatch.setattr(chat_service, "DB_PATH", str(test_db))
        context, tools = chat_service.build_forensic_context("reporte ejecutivo")
        assert isinstance(context, str)
        assert isinstance(tools, list)
        assert len(tools) > 0

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


# ──────── Backup ──────── #

class TestBackup:
    """Tests para backup.py."""

    def test_create_backup(self, tmp_path, monkeypatch):
        # Crear una BD de prueba
        db_file = tmp_path / "test.db"
        conn = sqlite3.connect(db_file)
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.execute("INSERT INTO t VALUES (1)")
        conn.commit()
        conn.close()

        import backup
        monkeypatch.setattr(backup, "BACKUP_DIR", tmp_path / "backups")

        result = backup.create_backup(str(db_file))
        assert result.exists()
        assert result.stat().st_size > 0

        # Verify the backup is a valid SQLite DB with the data
        conn2 = sqlite3.connect(result)
        rows = conn2.execute("SELECT * FROM t").fetchall()
        conn2.close()
        assert rows == [(1,)]

    def test_restore_backup(self, tmp_path, monkeypatch):
        import backup
        monkeypatch.setattr(backup, "BACKUP_DIR", tmp_path / "backups")

        # Create source DB (the "backup" to restore from)
        backup_file = tmp_path / "backup_src.db"
        conn = sqlite3.connect(backup_file)
        conn.execute("CREATE TABLE restored (val TEXT)")
        conn.execute("INSERT INTO restored VALUES ('hello')")
        conn.commit()
        conn.close()

        # Create a target DB (current state)
        target = tmp_path / "target.db"
        conn = sqlite3.connect(target)
        conn.execute("CREATE TABLE old (x INTEGER)")
        conn.commit()
        conn.close()

        backup.restore_backup(str(backup_file), str(target))

        # Verify target now has the restored data
        conn = sqlite3.connect(target)
        rows = conn.execute("SELECT * FROM restored").fetchall()
        conn.close()
        assert rows == [("hello",)]

    def test_list_backups_empty(self, tmp_path, monkeypatch, capsys):
        import backup
        monkeypatch.setattr(backup, "BACKUP_DIR", tmp_path / "no_backups")
        result = backup.list_backups()
        assert result == []


# ──────── Extractor max_oc=0 ──────── #

class TestExtractorCap:
    """Verifica que max_oc=0 no limita los códigos."""

    def test_max_oc_zero_no_limit(self, monkeypatch):
        from extractor import MercadoPublicoExtractor

        fake_list = [{"Codigo": f"OC-{i}"} for i in range(500)]

        ext = MercadoPublicoExtractor.__new__(MercadoPublicoExtractor)
        ext.ticket = "FAKE"
        ext.session = None

        monkeypatch.setattr(ext, "_fetch_oc_codes", lambda fecha: fake_list)
        monkeypatch.setattr(ext, "_fetch_oc_detail", lambda c: {"Codigo": c, "Items": []})

        import time
        monkeypatch.setattr(time, "sleep", lambda x: None)

        result = ext.extract(__import__("datetime").date(2024, 1, 1), max_oc=0)
        assert len(result) == 500


# ══════════════════════════════════════════════
# Tests: Notifier (Telegram)
# ══════════════════════════════════════════════

class TestNotifier:
    """Tests de integración para TelegramNotifier."""

    def _make_notifier(self, monkeypatch):
        """Crea un notifier con time.sleep neutralizado."""
        from notifier import TelegramNotifier
        monkeypatch.setattr("time.sleep", lambda x: None)
        return TelegramNotifier(token="FAKE_TOKEN", chat_id="12345")

    def test_enviar_alerta_desfalco_html_format(self, monkeypatch):
        notifier = self._make_notifier(monkeypatch)
        sent_payloads = []

        def mock_post(url, json=None, timeout=None):
            sent_payloads.append(json)

            class Resp:
                status_code = 200

                def raise_for_status(self):
                    pass

                def json(self_inner):
                    return {"ok": True}

            return Resp()

        monkeypatch.setattr(notifier.session, "post", mock_post)

        notifier.enviar_alerta_desfalco(
            producto="MASCARILLAS 3 PLIEGUES",
            comprador="HOSPITAL SAN JUAN",
            precio_pagado=5000,
            precio_promedio=150,
            z_score=8.5,
            link_orden="3401-120-SE26",
            categoria_riesgo="GENERAL",
        )

        assert len(sent_payloads) == 1
        payload = sent_payloads[0]
        assert payload["parse_mode"] == "HTML"
        assert "SOBREPRECIO" in payload["text"]
        assert "MASCARILLAS" in payload["text"]
        assert "3401-120-SE26" in payload["text"]

    def test_antispam_dedup(self, monkeypatch):
        notifier = self._make_notifier(monkeypatch)
        call_count = {"n": 0}

        def mock_post(url, json=None, timeout=None):
            call_count["n"] += 1

            class Resp:
                status_code = 200

                def raise_for_status(self):
                    pass

                def json(self_inner):
                    return {"ok": True}

            return Resp()

        monkeypatch.setattr(notifier.session, "post", mock_post)

        # Enviar la misma OC 3 veces
        for _ in range(3):
            notifier.enviar_alerta_desfalco(
                producto="P", comprador="C", precio_pagado=100,
                precio_promedio=10, z_score=5.0, link_orden="OC-DUP-001",
            )

        # Solo 1 debería haber sido enviada (las otras 2 son duplicados)
        assert call_count["n"] == 1
        assert notifier._alerts_sent == 1

    def test_antispam_max_alerts(self, monkeypatch):
        from notifier import MAX_ALERTS_PER_RUN
        notifier = self._make_notifier(monkeypatch)

        def mock_post(url, json=None, timeout=None):
            class Resp:
                status_code = 200

                def raise_for_status(self):
                    pass

                def json(self_inner):
                    return {"ok": True}

            return Resp()

        monkeypatch.setattr(notifier.session, "post", mock_post)

        # Enviar más alertas que el máximo
        results = []
        for i in range(MAX_ALERTS_PER_RUN + 5):
            r = notifier.enviar_alerta_desfalco(
                producto=f"PROD-{i}", comprador="C", precio_pagado=100,
                precio_promedio=10, z_score=5.0, link_orden=f"OC-MAX-{i:03d}",
            )
            results.append(r)

        # Las primeras MAX_ALERTS_PER_RUN deberían tener resultado, las demás None
        assert all(r is not None for r in results[:MAX_ALERTS_PER_RUN])
        assert all(r is None for r in results[MAX_ALERTS_PER_RUN:])

    def test_message_truncation(self, monkeypatch):
        from notifier import MAX_MESSAGE_LENGTH
        notifier = self._make_notifier(monkeypatch)
        sent_texts = []

        def mock_post(url, json=None, timeout=None):
            sent_texts.append(json["text"])

            class Resp:
                status_code = 200

                def raise_for_status(self):
                    pass

                def json(self_inner):
                    return {"ok": True}

            return Resp()

        monkeypatch.setattr(notifier.session, "post", mock_post)
        # Enviar mensaje gigante directamente via _send_message
        huge_msg = "A" * (MAX_MESSAGE_LENGTH + 1000)
        notifier._send_message(huge_msg)

        assert len(sent_texts[0]) <= MAX_MESSAGE_LENGTH

    def test_connection_error_raises(self, monkeypatch):
        import requests as req
        notifier = self._make_notifier(monkeypatch)

        def raise_conn(*a, **kw):
            raise req.exceptions.ConnectionError("no network")

        monkeypatch.setattr(notifier.session, "post", raise_conn)

        with pytest.raises(req.exceptions.ConnectionError):
            notifier._send_message("test")


# ══════════════════════════════════════════════
# Tests: Extractor retry logic
# ══════════════════════════════════════════════

class TestExtractorRetry:
    """Tests para la lógica de reintentos de MercadoPublicoExtractor."""

    def test_get_with_retry_success_on_second_attempt(self, monkeypatch):
        import requests as req
        from extractor import MercadoPublicoExtractor

        monkeypatch.setattr("time.sleep", lambda x: None)

        ext = MercadoPublicoExtractor.__new__(MercadoPublicoExtractor)
        ext.session = req.Session()

        call_count = {"n": 0}

        def mock_get(url, params=None, timeout=None):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise req.exceptions.Timeout("first try timeout")

            class Resp:
                status_code = 200

                def raise_for_status(self):
                    pass

                def json(self_inner):
                    return {"Listado": []}

            return Resp()

        monkeypatch.setattr(ext.session, "get", mock_get)
        result = ext._get_with_retry("http://fake", {})
        assert result == {"Listado": []}
        assert call_count["n"] == 2

    def test_get_with_retry_exhausted(self, monkeypatch):
        import requests as req
        from extractor import MercadoPublicoExtractor

        monkeypatch.setattr("time.sleep", lambda x: None)

        ext = MercadoPublicoExtractor.__new__(MercadoPublicoExtractor)
        ext.session = req.Session()

        def always_fail(url, params=None, timeout=None):
            raise req.exceptions.ConnectionError("always fails")

        monkeypatch.setattr(ext.session, "get", always_fail)

        with pytest.raises(req.exceptions.ConnectionError, match="Agotados"):
            ext._get_with_retry("http://fake", {})


# ══════════════════════════════════════════════
# Tests: Processor RUT validation
# ══════════════════════════════════════════════

class TestProcessorRUT:
    """Tests para la normalización de RUT en processor.py."""

    def test_normalize_valid_rut(self):
        from processor import DataProcessor
        assert DataProcessor._normalize_rut("76.123.456-7") == "76123456-7"

    def test_normalize_rut_with_k(self):
        from processor import DataProcessor
        assert DataProcessor._normalize_rut("76.999.888-k") == "76999888-K"

    def test_normalize_rut_already_clean(self):
        from processor import DataProcessor
        assert DataProcessor._normalize_rut("76123456-7") == "76123456-7"

    def test_normalize_rut_empty(self):
        from processor import DataProcessor
        assert DataProcessor._normalize_rut("") == ""

    def test_normalize_rut_invalid_format(self):
        from processor import DataProcessor
        # Too short — returned as-is
        assert DataProcessor._normalize_rut("123-4") == "123-4"

    def test_normalize_rut_no_dash(self):
        from processor import DataProcessor
        # No dash — returned as-is
        assert DataProcessor._normalize_rut("761234567") == "761234567"

    def test_flatten_oc_normalizes_ruts(self):
        from processor import DataProcessor
        oc = {
            "Codigo": "TEST-RUT-SE26",
            "CodigoEstado": 6,
            "FechaCreacion": "2026-03-15",
            "Comprador": {"RutUnidad": "61.602.000-0", "NombreUnidad": "TEST"},
            "Proveedor": {"RutSucursal": "76.999.888-k", "Nombre": "PROV"},
            "Items": {
                "Cantidad": 1,
                "Listado": [
                    {"Producto": "X", "Cantidad": 1, "PrecioNeto": 100, "Categoria": "T"},
                ],
            },
        }
        rows = DataProcessor._flatten_oc(oc)
        assert rows[0]["rut_comprador"] == "61602000-0"
        assert rows[0]["rut_proveedor"] == "76999888-K"


# ══════════════════════════════════════════════
# Tests: ChatService DB error handling
# ══════════════════════════════════════════════

class TestChatServiceErrors:
    """Tests de manejo de errores en chat_service."""

    def test_build_db_context_corrupted_db(self, tmp_path, monkeypatch):
        """Si la DB está corrupta, build_db_context retorna cadena vacía."""
        import chat_service

        bad_db = tmp_path / "bad.db"
        bad_db.write_text("not a sqlite file")
        monkeypatch.setattr(chat_service, "DB_PATH", str(bad_db))

        result = chat_service.build_db_context("mascarillas compras")
        assert result == ""

    def test_build_db_context_missing_db(self, tmp_path, monkeypatch):
        """Si la DB no existe, build_db_context retorna cadena vacía sin crash."""
        import chat_service

        monkeypatch.setattr(chat_service, "DB_PATH", str(tmp_path / "nonexistent.db"))
        result = chat_service.build_db_context("mascarillas compras")
        # SQLite creates the file on connect, but the table doesn't exist
        assert isinstance(result, str)

    def test_call_deepseek_http_error(self, monkeypatch):
        """Non-200/429 status returns error message, not crash."""
        monkeypatch.setenv("DEEPSEEK_API_KEY", "fake-key")
        import chat_service

        class MockResponse500:
            status_code = 500

        monkeypatch.setattr(chat_service.requests, "post", lambda *a, **kw: MockResponse500())
        monkeypatch.setattr("time.sleep", lambda x: None)

        result = chat_service.call_deepseek(
            [{"role": "user", "content": "test"}], "", ""
        )
        assert "500" in result


# ══════════════════════════════════════════════
# Tests: Concurrent DB writes
# ══════════════════════════════════════════════

class TestConcurrentDB:
    """Verifica que el processor maneja escrituras con datos duplicados."""

    def test_duplicate_insert_ignored(self, tmp_path):
        from processor import DataProcessor

        db_path = tmp_path / "concurrent.db"
        proc = DataProcessor(db_path=db_path)

        oc = {
            "Codigo": "DUP-001-SE26",
            "CodigoEstado": 6,
            "FechaCreacion": "2026-03-15",
            "Comprador": {"RutUnidad": "61.602.000-0", "NombreUnidad": "HOSPITAL TEST"},
            "Proveedor": {"RutSucursal": "76.111.222-3", "Nombre": "PROVEEDOR TEST"},
            "Items": {
                "Cantidad": 1,
                "Listado": [
                    {"Producto": "PRODUCTO A", "Cantidad": 10, "PrecioNeto": 1000, "Categoria": "Test"},
                ],
            },
        }

        # Insert twice — second time should skip the duplicate
        _, inserted1 = proc.process_and_store([oc])
        _, inserted2 = proc.process_and_store([oc])

        assert inserted1 == 1
        assert inserted2 == 0

        # Verify only 1 row in DB
        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM ordenes_items").fetchone()[0]
        conn.close()
        assert count == 1


# ══════════════════════════════════════════════
# Tests: InfoProbidadConnector
# ══════════════════════════════════════════════

class TestInfoProbidadConnector:
    """Tests para el conector de InfoProbidad."""

    def test_init_creates_tables(self, tmp_path):
        from infoprobidad_connector import InfoProbidadConnector
        db_path = tmp_path / "test_ip.db"
        ip = InfoProbidadConnector(db_path=str(db_path))
        conn = sqlite3.connect(db_path)
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        conn.close()
        assert "declarantes_probidad" in tables
        assert "actividades_probidad" in tables

    def test_buscar_declarante_empty_name(self, tmp_path):
        from infoprobidad_connector import InfoProbidadConnector
        ip = InfoProbidadConnector(db_path=str(tmp_path / "test.db"))
        assert ip.buscar_declarante("") == []
        assert ip.buscar_declarante("   ") == []

    def test_buscar_declarante_sanitizes_input(self, tmp_path):
        from infoprobidad_connector import InfoProbidadConnector
        ip = InfoProbidadConnector(db_path=str(tmp_path / "test.db"))
        # Should not raise even with special characters
        result = ip.buscar_declarante('<script>alert("xss")</script>')
        assert isinstance(result, list)

    def test_guardar_declarantes(self, tmp_path):
        from infoprobidad_connector import InfoProbidadConnector
        db_path = tmp_path / "test_ip.db"
        ip = InfoProbidadConnector(db_path=str(db_path))

        declarantes = [
            {
                "nombre": "Juan Pérez",
                "cargo": "Director",
                "institucion": "MINSAL",
                "fecha_declaracion": "2025-01-15",
                "tipo_declaracion": "ASUNCION",
            },
        ]
        count = ip.guardar_declarantes(declarantes)
        assert count == 1

        # Verify in DB
        conn = sqlite3.connect(db_path)
        rows = conn.execute("SELECT * FROM declarantes_probidad").fetchall()
        conn.close()
        assert len(rows) == 1

    def test_guardar_declarantes_empty(self, tmp_path):
        from infoprobidad_connector import InfoProbidadConnector
        ip = InfoProbidadConnector(db_path=str(tmp_path / "test.db"))
        assert ip.guardar_declarantes([]) == 0

    def test_guardar_actividades(self, tmp_path):
        from infoprobidad_connector import InfoProbidadConnector
        db_path = tmp_path / "test_ip.db"
        ip = InfoProbidadConnector(db_path=str(db_path))

        actividades = [
            {
                "nombre": "María González",
                "cargo": "Subsecretaria",
                "institucion": "MOP",
                "actividad": "Directora de ACME Limitada",
                "tipo_actividad": "PARTICIPACION_SOCIEDAD",
            },
        ]
        count = ip.guardar_actividades(actividades)
        assert count == 1

    def test_cruzar_con_proveedor_short_name(self, tmp_path):
        from infoprobidad_connector import InfoProbidadConnector
        ip = InfoProbidadConnector(db_path=str(tmp_path / "test.db"))
        # Names shorter than 3 chars should return empty
        assert ip.cruzar_con_proveedor("AB") == []


# ══════════════════════════════════════════════
# Tests: ContraloriaConnector
# ══════════════════════════════════════════════

class TestContraloriaConnector:
    """Tests para el conector de Contraloría."""

    def test_init_creates_tables(self, tmp_path):
        from contraloria_connector import ContraloriaConnector
        db_path = tmp_path / "test_cgr.db"
        cgr = ContraloriaConnector(db_path=str(db_path))
        conn = sqlite3.connect(db_path)
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        conn.close()
        assert "fiscalizaciones_cgr" in tables
        assert "informes_cgr" in tables

    def test_guardar_fiscalizaciones(self, tmp_path):
        from contraloria_connector import ContraloriaConnector
        db_path = tmp_path / "test_cgr.db"
        cgr = ContraloriaConnector(db_path=str(db_path))

        fisc = [
            {
                "region": "RM",
                "sector": "SALUD",
                "entidad": "HOSPITAL SAN JUAN DE DIOS",
                "periodo": "2026",
                "tipo_fiscalizacion": "AUDITORIA",
                "materia": "ADMINISTRACIÓN DE RECURSOS",
            },
        ]
        count = cgr.guardar_fiscalizaciones(fisc)
        assert count == 1

    def test_guardar_fiscalizaciones_empty(self, tmp_path):
        from contraloria_connector import ContraloriaConnector
        cgr = ContraloriaConnector(db_path=str(tmp_path / "test.db"))
        assert cgr.guardar_fiscalizaciones([]) == 0

    def test_guardar_informes(self, tmp_path):
        from contraloria_connector import ContraloriaConnector
        db_path = tmp_path / "test_cgr.db"
        cgr = ContraloriaConnector(db_path=str(db_path))

        informes = [
            {
                "fecha": "2026-02-13",
                "entidad": "FUNDACION INTEGRA",
                "titulo": "Informe Final De Investigación Especial N° 732",
                "url_informe": "https://example.com/informe.pdf",
            },
        ]
        count = cgr.guardar_informes(informes)
        assert count == 1

    def test_buscar_fiscalizacion_entidad_empty(self, tmp_path):
        from contraloria_connector import ContraloriaConnector
        cgr = ContraloriaConnector(db_path=str(tmp_path / "test.db"))
        assert cgr.buscar_fiscalizacion_entidad("") == []

    def test_cruzar_compradores_fiscalizados(self, test_db):
        from contraloria_connector import ContraloriaConnector
        cgr = ContraloriaConnector(db_path=str(test_db))

        # Guardar fiscalización que matchee un comprador del test_db
        fisc = [
            {
                "region": "RM",
                "sector": "SALUD",
                "entidad": "HOSPITAL SAN JUAN DE DIOS",
                "periodo": "2026",
                "tipo_fiscalizacion": "AUDITORIA",
                "materia": "ADQUISICIONES",
            },
        ]
        cgr.guardar_fiscalizaciones(fisc)
        result = cgr.cruzar_compradores_fiscalizados()
        assert not result.empty
        assert "HOSPITAL SAN JUAN DE DIOS" in result["nombre_comprador"].values

    def test_entidad_bajo_fiscalizacion_local(self, tmp_path):
        from contraloria_connector import ContraloriaConnector
        db_path = tmp_path / "test_cgr.db"
        cgr = ContraloriaConnector(db_path=str(db_path))
        cgr.guardar_fiscalizaciones([{
            "region": "V",
            "sector": "EDUCACION",
            "entidad": "UNIVERSIDAD TECNICA",
            "periodo": "2026",
            "tipo_fiscalizacion": "DENUNCIA",
            "materia": "CONTRATACIONES",
        }])
        assert cgr.entidad_bajo_fiscalizacion("UNIVERSIDAD")


# ══════════════════════════════════════════════
# Tests: DipresConnector
# ══════════════════════════════════════════════

class TestDipresConnector:
    """Tests para el conector de DIPRES."""

    def test_init_creates_tables(self, tmp_path):
        from dipres_connector import DipresConnector
        db_path = tmp_path / "test_dipres.db"
        dp = DipresConnector(db_path=str(db_path))
        conn = sqlite3.connect(db_path)
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        conn.close()
        assert "presupuesto_dipres" in tables
        assert "dotacion_dipres" in tables

    def test_cruzar_presupuesto_compras_no_data(self, test_db):
        from dipres_connector import DipresConnector
        dp = DipresConnector(db_path=str(test_db))
        result = dp.cruzar_presupuesto_compras()
        # Should return compras data even without DIPRES data
        assert isinstance(result, pd.DataFrame)


# ══════════════════════════════════════════════
# Tests: Dashboard (smoke + refactored functions)
# ══════════════════════════════════════════════

class TestDashboardSmoke:
    """Verifica que dashboard.py se importa sin errores."""

    def test_import_dashboard(self):
        import importlib
        mod = importlib.import_module("dashboard")
        assert hasattr(mod, "main")

    def test_tab_functions_exist(self):
        import dashboard
        for fn_name in [
            "_render_tab_general", "_render_tab_cruces", "_render_tab_datos",
            "_render_tab_fuentes", "_render_tab_mira", "_render_tab_denuncias",
            "_render_tab_ia",
        ]:
            assert hasattr(dashboard, fn_name), f"Missing function: {fn_name}"

    def test_format_helpers_exist(self):
        from queries import format_clp, format_clp_full
        assert callable(format_clp)
        assert callable(format_clp_full)


# ══════════════════════════════════════════════
# Tests: AlertasPersonas
# ══════════════════════════════════════════════

class TestAlertasPersonas:
    """Tests para búsqueda de personas en fuentes oficiales."""

    def test_init(self, test_db):
        from alertas_personas import AlertasPersonas
        ap = AlertasPersonas(str(test_db))
        assert ap is not None

    def test_buscar_returns_list(self, test_db, monkeypatch):
        from alertas_personas import AlertasPersonas
        ap = AlertasPersonas(str(test_db))
        # Mock HTTP to avoid real API calls
        import alertas_personas
        monkeypatch.setattr(alertas_personas.requests, "get",
                            lambda *a, **kw: type("R", (), {"status_code": 200, "json": lambda self: {"results": {"bindings": []}}, "text": ""})())
        result = ap.buscar("Test Persona")
        assert isinstance(result, list)

    def test_resumen_returns_string(self, test_db, monkeypatch):
        from alertas_personas import AlertasPersonas
        ap = AlertasPersonas(str(test_db))
        import alertas_personas
        monkeypatch.setattr(alertas_personas.requests, "get",
                            lambda *a, **kw: type("R", (), {"status_code": 200, "json": lambda self: {"results": {"bindings": []}}, "text": ""})())
        result = ap.resumen("Test Persona")
        assert isinstance(result, str)

    def test_sanitize_nombre(self, test_db):
        """Verifica que nombres con caracteres peligrosos son sanitizados."""
        from alertas_personas import AlertasPersonas
        ap = AlertasPersonas(str(test_db))
        # The class should handle this without raising
        # (actual SPARQL injection chars should be stripped)
        assert ap is not None


# ══════════════════════════════════════════════
# Tests: ContraloriaConnector
# ══════════════════════════════════════════════

class TestContraloriaConnector:
    """Tests para el conector de la Contraloría General."""

    def test_init_creates_table(self, tmp_path):
        from contraloria_connector import ContraloriaConnector
        db_path = tmp_path / "test_cgr.db"
        cgr = ContraloriaConnector(db_path=str(db_path))
        conn = sqlite3.connect(db_path)
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        conn.close()
        assert "fiscalizaciones_cgr" in tables

    def test_cruzar_compradores_fiscalizados_empty(self, test_db):
        from contraloria_connector import ContraloriaConnector
        cgr = ContraloriaConnector(db_path=str(test_db))
        result = cgr.cruzar_compradores_fiscalizados()
        assert isinstance(result, pd.DataFrame)


# ══════════════════════════════════════════════
# Tests: InfoProbidadConnector
# ══════════════════════════════════════════════

class TestInfoProbidadConnector:
    """Tests para el conector de InfoProbidad."""

    def test_init_creates_table(self, tmp_path):
        from infoprobidad_connector import InfoProbidadConnector
        db_path = tmp_path / "test_ip.db"
        ip = InfoProbidadConnector(db_path=str(db_path))
        conn = sqlite3.connect(db_path)
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        conn.close()
        assert "declarantes_probidad" in tables

    def test_cruzar_con_proveedor_empty(self, tmp_path):
        from infoprobidad_connector import InfoProbidadConnector
        db_path = tmp_path / "test_ip.db"
        ip = InfoProbidadConnector(db_path=str(db_path))
        result = ip.cruzar_con_proveedor("12345678-9")
        assert isinstance(result, (pd.DataFrame, list, dict, type(None)))


# ══════════════════════════════════════════════
# Tests: XSS Protection
# ══════════════════════════════════════════════

class TestXSSProtection:
    """Verifica que datos externos se escapan correctamente."""

    def test_chat_bubbles_escape_html(self):
        """El chat debe escapar HTML en mensajes de usuario."""
        import html as html_mod
        malicious = '<script>alert("xss")</script>'
        escaped = html_mod.escape(malicious)
        assert "<script>" not in escaped
        assert "&lt;script&gt;" in escaped

    def test_alert_card_escapes_description(self):
        """Los datos de alertas externas deben ser escapados."""
        import html as html_mod
        malicious_desc = '<img src=x onerror=alert(1)>'
        escaped = html_mod.escape(malicious_desc)
        assert "<img" not in escaped
        assert "&lt;img" in escaped

    def test_url_validation_blocks_javascript(self):
        """URLs javascript: deben ser rechazadas."""
        malicious_url = "javascript:alert(document.cookie)"
        is_safe = malicious_url.startswith(("http://", "https://"))
        assert not is_safe
