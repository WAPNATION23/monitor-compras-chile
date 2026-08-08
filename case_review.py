"""
Flujo de revisión humana y feedback de precisión — Fase 4.

Tablas:
  • alertas_revision — casos generados por IA pendientes de validación
  • alert_feedback — registro de si una alerta fue real o falso positivo
  • suscripciones_alertas — filtros Telegram por organismo/proveedor/comuna
"""

from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from datetime import datetime
from typing import Any

from config import DB_NAME

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS alertas_revision (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
);

CREATE TABLE IF NOT EXISTS alert_feedback (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    caso_id TEXT,
    alerta_tipo TEXT,
    es_real INTEGER,
    comentario TEXT,
    registrado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS suscripciones_alertas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id TEXT NOT NULL,
    filtro_tipo TEXT NOT NULL,
    filtro_valor TEXT NOT NULL,
    activo INTEGER DEFAULT 1,
    creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(chat_id, filtro_tipo, filtro_valor)
);

CREATE INDEX IF NOT EXISTS idx_alertas_estado ON alertas_revision (estado);
CREATE INDEX IF NOT EXISTS idx_suscripciones_tipo ON suscripciones_alertas (filtro_tipo, filtro_valor);
"""


def init_review_tables(db_path: str = DB_NAME) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.executescript(_SCHEMA)
        conn.commit()


def crear_caso_revision(
    titulo: str,
    resumen: str,
    *,
    rut_objetivo: str = "",
    nombre_objetivo: str = "",
    nivel: str = "medio",
    fuentes: list[dict[str, Any]] | None = None,
    creado_por: str = "ia",
    db_path: str = DB_NAME,
) -> str:
    """Registra un caso para revisión humana. Retorna caso_id."""
    init_review_tables(db_path)
    caso_id = str(uuid.uuid4())[:12]
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO alertas_revision
            (caso_id, titulo, resumen, nivel_gravedad, rut_objetivo,
             nombre_objetivo, fuentes_json, creado_por)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                caso_id,
                titulo[:500],
                resumen[:8000],
                nivel,
                rut_objetivo,
                nombre_objetivo[:300],
                json.dumps(fuentes or [], ensure_ascii=False),
                creado_por,
            ),
        )
        conn.commit()
    return caso_id


def registrar_feedback(
    caso_id: str,
    alerta_tipo: str,
    es_real: bool,
    comentario: str = "",
    db_path: str = DB_NAME,
) -> None:
    init_review_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO alert_feedback (caso_id, alerta_tipo, es_real, comentario)
            VALUES (?, ?, ?, ?)
            """,
            (caso_id, alerta_tipo, 1 if es_real else 0, comentario[:2000]),
        )
        conn.commit()


def listar_casos_pendientes(limit: int = 20, db_path: str = DB_NAME) -> list[dict[str, Any]]:
    init_review_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT caso_id, titulo, resumen, nivel_gravedad, rut_objetivo,
                   nombre_objetivo, creado_en
            FROM alertas_revision
            WHERE estado = 'pendiente'
            ORDER BY creado_en DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def marcar_caso_revisado(
    caso_id: str,
    estado: str,
    revisado_por: str = "editor",
    notas: str = "",
    db_path: str = DB_NAME,
) -> bool:
    init_review_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            """
            UPDATE alertas_revision
            SET estado = ?, revisado_por = ?, notas_revision = ?,
                actualizado_en = ?
            WHERE caso_id = ?
            """,
            (estado, revisado_por, notas[:2000], datetime.now().isoformat(), caso_id),
        )
        conn.commit()
        return cur.rowcount > 0


def agregar_suscripcion(
    chat_id: str,
    filtro_tipo: str,
    filtro_valor: str,
    db_path: str = DB_NAME,
) -> None:
    """filtro_tipo: organismo | proveedor | comuna | rut_proveedor"""
    init_review_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO suscripciones_alertas
            (chat_id, filtro_tipo, filtro_valor, activo)
            VALUES (?, ?, ?, 1)
            """,
            (chat_id, filtro_tipo.lower(), filtro_valor.upper() if filtro_tipo == "rut_proveedor" else filtro_valor),
        )
        conn.commit()


def suscripciones_que_coinciden(
    organismo: str = "",
    proveedor: str = "",
    comuna: str = "",
    rut_proveedor: str = "",
    db_path: str = DB_NAME,
) -> list[str]:
    """Retorna chat_ids suscritos que matchean el evento."""
    init_review_tables(db_path)
    chat_ids: set[str] = set()
    with sqlite3.connect(db_path) as conn:
        for row in conn.execute(
            "SELECT chat_id, filtro_tipo, filtro_valor FROM suscripciones_alertas WHERE activo = 1"
        ):
            cid, ftipo, fval = row
            fval_l = (fval or "").lower()
            if ftipo == "organismo" and organismo and fval_l in organismo.lower():
                chat_ids.add(cid)
            elif ftipo == "proveedor" and proveedor and fval_l in proveedor.lower():
                chat_ids.add(cid)
            elif ftipo == "comuna" and comuna and fval_l in comuna.lower():
                chat_ids.add(cid)
            elif ftipo == "rut_proveedor" and rut_proveedor:
                clean = rut_proveedor.replace(".", "").upper()
                if clean == fval.replace(".", "").upper():
                    chat_ids.add(cid)
    return list(chat_ids)


def precision_por_tipo(db_path: str = DB_NAME) -> list[dict[str, Any]]:
    """Métricas de precisión agregadas por tipo de alerta."""
    init_review_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT alerta_tipo,
                   COUNT(*) AS total,
                   SUM(es_real) AS confirmadas
            FROM alert_feedback
            GROUP BY alerta_tipo
            ORDER BY total DESC
            """
        ).fetchall()
    out = []
    for tipo, total, confirmadas in rows:
        pct = round(100 * (confirmadas or 0) / max(total, 1), 1)
        out.append({"tipo": tipo, "total": total, "confirmadas": confirmadas or 0, "precision_pct": pct})
    return out
