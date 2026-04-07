"""
Funciones de acceso a datos y utilidades de formato.
Separadas de dashboard.py para mantener la UI desacoplada de la lógica de datos.
"""

import sqlite3
import pandas as pd

DB_PATH = "auditoria_estado.db"


def load_data() -> pd.DataFrame:
    """Carga y prepara las órdenes de compra desde SQLite."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='ordenes_items';"
            )
            if not cursor.fetchone():
                return pd.DataFrame()

            query = """
                SELECT 
                    codigo_oc, nombre_producto, cantidad, precio_unitario, 
                    monto_total_item, nombre_comprador, nombre_proveedor, 
                    rut_proveedor, rut_comprador,
                    fecha_creacion, estado,
                    IFNULL(tipo_oc, '') as tipo_oc,
                    IFNULL(categoria_riesgo, 'GENERAL') as categoria_riesgo
                FROM ordenes_items
                WHERE precio_unitario > 0
                  AND estado != '9'
            """
            df = pd.read_sql_query(query, conn)
            if not df.empty:
                df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion"], errors="coerce")
                df["precio_unitario"] = pd.to_numeric(df["precio_unitario"], errors="coerce")
                df["monto_total_item"] = pd.to_numeric(df["monto_total_item"], errors="coerce")
                df["cantidad"] = pd.to_numeric(df["cantidad"], errors="coerce")
            return df
    except sqlite3.Error:
        return pd.DataFrame()


def init_feedback_db() -> None:
    """Crea la tabla de feedback comunitario si no existe."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS feedback_comunidad (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tipo_reporte TEXT,
                dato_reportado TEXT,
                comentario TEXT,
                fecha_reporte TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()


def save_feedback(tipo: str, dato: str, comentario: str) -> None:
    """Guarda un reporte de la comunidad."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO feedback_comunidad (tipo_reporte, dato_reportado, comentario) VALUES (?, ?, ?)",
            (tipo, dato, comentario),
        )
        conn.commit()


def load_licitaciones(limit: int = 5000) -> pd.DataFrame:
    """Carga licitaciones desde SQLite si la tabla existe."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='licitaciones';"
            )
            if not cursor.fetchone():
                return pd.DataFrame()
            return pd.read_sql_query(f"SELECT * FROM licitaciones LIMIT {int(limit)}", conn)
    except sqlite3.Error:
        return pd.DataFrame()


def get_rate_limit_usage(ip: str, fecha: str) -> int:
    """Obtiene cuántas consultas hizo una IP en una fecha dada."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS rate_limit (
                ip TEXT,
                fecha TEXT,
                consultas INTEGER,
                PRIMARY KEY(ip, fecha)
            )
            """
        )
        row = conn.execute(
            "SELECT consultas FROM rate_limit WHERE ip=? AND fecha=?",
            (ip, fecha),
        ).fetchone()
        return int(row[0]) if row else 0


def increment_rate_limit_usage(ip: str, fecha: str) -> None:
    """Incrementa el contador diario de consultas para una IP."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS rate_limit (
                ip TEXT,
                fecha TEXT,
                consultas INTEGER,
                PRIMARY KEY(ip, fecha)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO rate_limit (ip, fecha, consultas)
            VALUES (?, ?, 1)
            ON CONFLICT(ip, fecha) DO UPDATE SET consultas = consultas + 1
            """,
            (ip, fecha),
        )
        conn.commit()


def format_clp(value: float) -> str:
    """Formato corto de montos CLP (ej. $5.9B CLP)."""
    if value >= 1_000_000_000:
        return f"${value / 1_000_000_000:,.1f}B CLP"
    elif value >= 1_000_000:
        return f"${value / 1_000_000:,.0f}M CLP"
    else:
        return f"${value:,.0f} CLP".replace(",", ".")


def format_clp_full(value: float) -> str:
    """Formato completo de montos CLP (ej. $276,000,000)."""
    return f"${value:,.0f}".replace(",", ".")
