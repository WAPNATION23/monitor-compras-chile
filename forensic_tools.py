"""
Herramientas forenses invocables por el Cerebro Forense (Conan).

Centraliza consultas a BD y APIs para que chat_service.py orqueste
todas las fuentes sin depender solo del prompt stuffing.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from typing import Any

from config import DB_NAME

logger = logging.getLogger(__name__)
DB_PATH = DB_NAME


def _extract_keywords(text: str, stopwords: frozenset[str]) -> list[str]:
    return [
        w
        for w in re.findall(r"[a-záéíóúñü]+", text.lower())
        if len(w) >= 3 and w not in stopwords
    ][:10]


def tool_licitaciones(prompt: str, stopwords: frozenset[str]) -> tuple[str, str]:
    """Busca licitaciones en la tabla local por organismo o nombre."""
    keywords = _extract_keywords(prompt, stopwords)
    if not keywords:
        return "Licitaciones", "Sin palabras clave para buscar licitaciones."
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            # Verificar que exista la tabla
            tbl = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='licitaciones'"
            ).fetchone()
            if not tbl:
                return "Licitaciones", "Tabla licitaciones vacía — ejecutar daily_update.py --full."

            params = [f"%{k}%" for k in keywords[:3]]
            cond = " OR ".join(["nombre LIKE ? OR nombre_organismo LIKE ?" for _ in params])
            flat_params: list[str] = []
            for p in params:
                flat_params.extend([p, p])

            rows = conn.execute(
                f"""
                SELECT codigo_externo, nombre, nombre_organismo, estado,
                       fecha_publicacion, monto_estimado, tipo
                FROM licitaciones
                WHERE {cond}
                ORDER BY fecha_publicacion DESC
                LIMIT 15
                """,
                flat_params,
            ).fetchall()

        if not rows:
            return "Licitaciones", f"Sin licitaciones locales para: {', '.join(keywords[:3])}"

        lines = [f"### LICITACIONES LOCALES — {len(rows)} resultados ###"]
        for r in rows:
            monto = r["monto_estimado"] or 0
            lines.append(
                f"- [{r['codigo_externo']}] {r['nombre'][:80]} | "
                f"Org: {r['nombre_organismo']} | Estado: {r['estado']} | "
                f"Monto est.: ${monto:,.0f} | Pub: {r['fecha_publicacion']}"
            )
        return "Licitaciones (BD local)", "\n".join(lines)
    except sqlite3.Error as exc:
        return "Licitaciones", f"[Error BD: {exc}]"


def tool_dipres_presupuesto(prompt: str) -> tuple[str, str]:
    """Búsqueda de datasets presupuestarios en datos.gob.cl."""
    try:
        from dipres_connector import DipresConnector

        dip = DipresConnector(DB_PATH)
        keywords = re.findall(r"[a-záéíóúñü]{4,}", prompt.lower())[:3]
        q = " ".join(keywords) if keywords else "presupuesto dotacion"
        datasets = dip.buscar_datos_gob(q, rows=5)
        if not datasets:
            return "DIPRES/datos.gob", "Sin datasets presupuestarios encontrados."
        lines = ["### DIPRES / datos.gob.cl — datasets ###"]
        for ds in datasets[:5]:
            lines.append(f"- {ds.get('title', '?')} | {ds.get('organization', '?')} | {ds.get('url', '')}")
        return "DIPRES/datos.gob", "\n".join(lines)
    except Exception as exc:  # noqa: BLE001
        return "DIPRES/datos.gob", f"[Error: {exc}]"


def tool_datos_gob_sanciones(prompt: str, stopwords: frozenset[str]) -> tuple[str, str]:
    """Busca sanciones en datasets sancionatorios de datos.gob.cl."""
    try:
        from datos_gob_connector import DatosGobConnector

        dg = DatosGobConnector()
        keywords = _extract_keywords(prompt, stopwords)
        q = " ".join(keywords[:3]) if keywords else "sanciones multas"
        resultados = dg.search_datasets(q, rows=8)
        if not resultados:
            return "datos.gob Sanciones", "Sin registros sancionatorios para la consulta."
        lines = [f"### datos.gob.cl — {len(resultados)} registros ###"]
        for r in resultados[:8]:
            lines.append(f"- {r.get('title', '?')} | org: {r.get('organization', '?')}")
        return "datos.gob Sanciones", "\n".join(lines)
    except Exception as exc:  # noqa: BLE001
        return "datos.gob Sanciones", f"[Error: {exc}]"


def tool_infoprobidad_persona(prompt: str) -> tuple[str, str]:
    """Busca en cache local (cruces/declarantes) y luego SPARQL live."""
    try:
        from infoprobidad_connector import InfoProbidadConnector

        ip = InfoProbidadConnector(DB_PATH)
        nombre = prompt.strip()[:120]
        lines: list[str] = []

        # Cache local de cruces proveedor↔funcionario (sync semanal)
        try:
            with sqlite3.connect(DB_PATH) as conn:
                rows = conn.execute(
                    """
                    SELECT proveedor_query, funcionario, cargo, institucion,
                           vinculo_declarado, tipo_vinculo
                    FROM cruces_probidad
                    WHERE LOWER(proveedor_query) LIKE ?
                       OR LOWER(funcionario) LIKE ?
                       OR LOWER(vinculo_declarado) LIKE ?
                    LIMIT 15
                    """,
                    (f"%{nombre.lower()}%", f"%{nombre.lower()}%", f"%{nombre.lower()}%"),
                ).fetchall()
            if rows:
                lines.append(f"### INFOPROBIDAD LOCAL — {len(rows)} cruces ###")
                for r in rows:
                    lines.append(
                        f"- Proveedor~{r[0]} | Funcionario: {r[1]} | "
                        f"{r[2]} @ {r[3]} | {r[5]}: {r[4]} | Confianza: media (cache)"
                    )
        except sqlite3.Error:
            pass

        decls = ip.buscar_declarante(nombre, limit=10)
        if decls:
            lines.append(f"### INFOPROBIDAD LIVE — {len(decls)} declarantes ###")
            for d in decls[:10]:
                lines.append(
                    f"- {d.get('nombre', '?')} | Cargo: {d.get('cargo', '?')} | "
                    f"Inst: {d.get('institucion', '?')} | Confianza: media (match por nombre)"
                )

        if not lines:
            return "InfoProbidad", f"Sin declarantes/cruces para '{nombre[:60]}'."
        return "InfoProbidad (persona)", "\n".join(lines)
    except Exception as exc:  # noqa: BLE001
        return "InfoProbidad", f"[Error: {exc}]"


def format_confidence_label(fuente: str, match_type: str) -> str:
    """
    Etiquetas de confianza para el expediente Conan.
    match_type: exacto | rut | nombre | heuristica | web
    """
    levels = {
        ("mercado_publico", "exacto"): "ALTA — fuente primaria oficial",
        ("mercado_publico", "rut"): "ALTA — RUT verificado en API ChileCompra",
        ("servel", "rut"): "ALTA — cruce por RUT exacto",
        ("servel", "nombre"): "MEDIA — cruce por nombre (riesgo homónimo)",
        ("infoprobidad", "nombre"): "MEDIA — match SPARQL por substring",
        ("web", "web"): "BAJA — fuente web complementaria, verificar manualmente",
        ("heuristica", "heuristica"): "BAJA — señal estadística, no imputación",
    }
    return levels.get((fuente, match_type), "MEDIA — verificar con fuente primaria")
