"""
Capa de acceso a datos — SQLite por defecto, PostgreSQL opcional vía DATABASE_URL.

Permite escalar a Railway Postgres sin reescribir todo el proyecto de una vez.
Los módulos nuevos y el processor usan esta capa; el resto sigue en sqlite3 directo
hasta migración completa.
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from config import DB_NAME

_USE_PG = False
_pg_pool: Any = None


def use_postgres() -> bool:
    url = os.getenv("DATABASE_URL", "").strip()
    return url.startswith("postgres")


def db_path() -> str:
    return str(Path(DB_NAME).resolve())


@contextmanager
def connect() -> Iterator[Any]:
    """Context manager de conexión. SQLite por defecto."""
    if use_postgres():
        try:
            import psycopg2  # type: ignore
            from psycopg2.extras import RealDictCursor  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "DATABASE_URL apunta a PostgreSQL pero psycopg2-binary no está instalado."
            ) from exc
        conn = psycopg2.connect(os.environ["DATABASE_URL"], cursor_factory=RealDictCursor)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    else:
        conn = sqlite3.connect(db_path())
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()


def placeholder(n: int = 1) -> str:
    """Placeholder SQL según backend."""
    if use_postgres():
        return ", ".join(["%s"] * n)
    return ", ".join(["?"] * n)


def init_postgres_schema() -> None:
    """Crea tablas en PostgreSQL si DATABASE_URL está configurado."""
    if not use_postgres():
        return
    from processor import DataProcessor

    DataProcessor()  # side-effect: crea schema vía sqlite path — re-run on PG
    with connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ordenes_items (
                id SERIAL PRIMARY KEY,
                codigo_oc TEXT NOT NULL,
                nombre_producto TEXT,
                categoria TEXT,
                cantidad DOUBLE PRECISION,
                precio_unitario DOUBLE PRECISION,
                monto_total_item DOUBLE PRECISION,
                rut_comprador TEXT,
                nombre_comprador TEXT,
                nombre_unidad TEXT,
                nombre_organismo TEXT,
                rut_proveedor TEXT,
                nombre_proveedor TEXT,
                fecha_creacion TEXT,
                estado TEXT,
                tipo_oc TEXT DEFAULT '',
                categoria_riesgo TEXT DEFAULT 'GENERAL',
                tipo_moneda TEXT DEFAULT 'CLP',
                monto_total_oc DOUBLE PRECISION,
                precio_unitario_clp DOUBLE PRECISION,
                monto_total_item_clp DOUBLE PRECISION,
                fecha_ingreso TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(codigo_oc, nombre_producto, precio_unitario, cantidad)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS alertas_revision (
                id SERIAL PRIMARY KEY,
                caso_id TEXT UNIQUE NOT NULL,
                titulo TEXT,
                resumen TEXT,
                nivel_gravedad TEXT DEFAULT 'medio',
                estado TEXT DEFAULT 'pendiente',
                rut_objetivo TEXT,
                nombre_objetivo TEXT,
                fuentes_json TEXT,
                creado_por TEXT DEFAULT 'ia',
                revisado_por TEXT,
                notas_revision TEXT,
                creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                actualizado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS alert_feedback (
                id SERIAL PRIMARY KEY,
                caso_id TEXT,
                alerta_tipo TEXT,
                es_real BOOLEAN,
                comentario TEXT,
                registrado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS suscripciones_alertas (
                id SERIAL PRIMARY KEY,
                chat_id TEXT NOT NULL,
                filtro_tipo TEXT NOT NULL,
                filtro_valor TEXT NOT NULL,
                activo BOOLEAN DEFAULT TRUE,
                creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(chat_id, filtro_tipo, filtro_valor)
            )
        """)
