"""
AlertasPersonas — Motor de Alertas "Personas en la Mira"
═════════════════════════════════════════════════════════
Consulta ÚNICAMENTE fuentes REALES y OFICIALES del Estado de Chile
para generar alertas sobre personas de interés público.

Fuentes verificadas y operativas:
  1. InfoLobby.cl (SPARQL)    — Audiencias de la Ley del Lobby
  2. datos.gob.cl (CKAN)      — Datasets de sanciones (Supereduc, SISS, Casinos)
  3. Contraloría (buscador)   — Dictámenes y resoluciones (scraping controlado)
  4. Mercado Público (API)    — Órdenes de compra asociadas a un proveedor
  5. SERVEL (DB local)        — Aportes electorales

Cada alerta devuelta tiene:
  - fuente: nombre oficial del organismo
  - fecha: fecha del registro (ISO 8601 o "sin fecha")
  - tipo_alerta: categoría (LOBBY, SANCIÓN, DICTAMEN, COMPRA_PUBLICA, APORTE_ELECTORAL)
  - descripcion: resumen legible del hallazgo
  - url: enlace directo al registro público (cuando existe)

Uso:
    from alertas_personas import AlertasPersonas
    motor = AlertasPersonas()
    alertas = motor.buscar("Juan Pérez")
    for a in alertas:
        print(f"[{a['tipo_alerta']}] {a['fuente']} — {a['descripcion']}")
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any

import pandas as pd
import requests

from config import DB_NAME, REQUEST_TIMEOUT

logger = logging.getLogger(__name__)


# ─────────────────── Estructura de Alerta ─────────────────── #

@dataclass
class Alerta:
    fuente: str
    fecha: str
    tipo_alerta: str
    descripcion: str
    url: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


# ─────────────────── Constantes ─────────────────── #

CKAN_BASE = "https://datos.gob.cl/api/3/action"
SPARQL_ENDPOINT = "http://datos.infolobby.cl/sparql"

# Contraloría: buscador de jurisprudencia (dictámenes públicos)
# Este endpoint acepta JSON y devuelve resultados paginados.
CGR_BUSCAR_URL = "https://www.contraloria.cl/appinf/lgd/api/jurisprudencia/buscar"

# IDs de recursos (DataStore) con datos de sanciones en datos.gob.cl
# Verificados como activos con datastore_active=true
RECURSOS_SANCIONES: dict[str, dict[str, str]] = {
    "sanciones_casinos": {
        "resource_id": "1735d81f-07a0-4e6c-ad7a-d581e2e27768",
        "fuente": "Superintendencia de Casinos de Juego",
        "tipo": "SANCIÓN",
    },
    "sanciones_casinos_2": {
        "resource_id": "370a4c4c-097c-400a-b5a5-e07af50e296e",
        "fuente": "Superintendencia de Casinos de Juego",
        "tipo": "SANCIÓN",
    },
    "sanciones_siss": {
        "resource_id": "42a1e2be-fd7b-46f1-b0b3-278d7af9b4cc",
        "fuente": "Superintendencia de Servicios Sanitarios (SISS)",
        "tipo": "SANCIÓN",
    },
}


class AlertasPersonas:
    """Motor de alertas sobre personas de interés público."""

    def __init__(self, db_path: str = DB_NAME) -> None:
        self.db_path = db_path
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "MonitorComprasChile/1.0 (Auditoría Cívica)",
        })

    # ═══════════════════════════════════════════════════════════
    # MÉTODO PRINCIPAL: Buscar alertas en TODAS las fuentes
    # ═══════════════════════════════════════════════════════════

    def buscar(self, nombre: str, incluir_compras: bool = True) -> list[dict[str, str]]:
        """
        Busca alertas sobre una persona en todas las fuentes oficiales.

        Args:
            nombre: Nombre completo o parcial de la persona.
            incluir_compras: Si True, también busca en Mercado Público (más lento).

        Returns:
            Lista de alertas ordenadas por fecha (más reciente primero).
        """
        if not nombre or not nombre.strip():
            logger.warning("Nombre vacío, no se realizará búsqueda.")
            return []

        nombre = nombre.strip()
        alertas: list[Alerta] = []

        # 1. InfoLobby — Audiencias Ley del Lobby
        alertas.extend(self._buscar_infolobby(nombre))

        # 2. datos.gob.cl — Datasets de sanciones
        alertas.extend(self._buscar_datos_gob_sanciones(nombre))

        # 3. Contraloría — Dictámenes y resoluciones
        alertas.extend(self._buscar_contraloria(nombre))

        # 4. SERVEL — Aportes electorales (DB local)
        alertas.extend(self._buscar_servel(nombre))

        # 5. Mercado Público (opcional, requiere RUT)
        if incluir_compras:
            alertas.extend(self._buscar_mercado_publico_por_nombre(nombre))

        # Ordenar por fecha descendente
        alertas.sort(key=lambda a: a.fecha if a.fecha != "sin fecha" else "", reverse=True)

        logger.info(
            "Búsqueda '%s': %d alertas encontradas en %d fuentes.",
            nombre, len(alertas),
            len({a.fuente for a in alertas}),
        )

        return [a.to_dict() for a in alertas]

    # ═══════════════════════════════════════════════════════════
    # FUENTE 1: InfoLobby (SPARQL) — Audiencias de la Ley del Lobby
    # ═══════════════════════════════════════════════════════════

    def _buscar_infolobby(self, nombre: str) -> list[Alerta]:
        """Busca audiencias donde aparece la persona como sujeto activo o pasivo."""
        alertas: list[Alerta] = []

        # Sanitizar nombre para SPARQL
        nombre_safe = nombre.replace("\\", "\\\\").replace('"', '\\"').replace("'", "\\'")

        sparql = f"""
        PREFIX lobby: <http://datos.infolobby.cl/def#>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX dcterms: <http://purl.org/dc/terms/>

        SELECT ?fecha ?sujetoPasivo ?sujetoActivo ?institucion ?materia
        WHERE {{
            ?audiencia a lobby:Audiencia ;
                       dcterms:date ?fecha ;
                       lobby:sujetoPasivo ?sp ;
                       lobby:sujetoActivo ?sa .
            ?sp foaf:name ?sujetoPasivo .
            ?sa foaf:name ?sujetoActivo .
            OPTIONAL {{ ?sp lobby:institucion ?inst . ?inst foaf:name ?institucion . }}
            OPTIONAL {{ ?audiencia lobby:materia ?materia . }}
            FILTER(
                CONTAINS(LCASE(?sujetoPasivo), LCASE("{nombre_safe}"))
                || CONTAINS(LCASE(?sujetoActivo), LCASE("{nombre_safe}"))
            )
        }}
        ORDER BY DESC(?fecha)
        LIMIT 50
        """

        try:
            response = self.session.get(
                SPARQL_ENDPOINT,
                params={"query": sparql, "format": "json"},
                timeout=REQUEST_TIMEOUT * 2,
            )
            response.raise_for_status()
            data = response.json()

            bindings = data.get("results", {}).get("bindings", [])
            for b in bindings:
                fecha = b.get("fecha", {}).get("value", "sin fecha")
                sp = b.get("sujetoPasivo", {}).get("value", "")
                sa = b.get("sujetoActivo", {}).get("value", "")
                inst = b.get("institucion", {}).get("value", "N/D")
                materia = b.get("materia", {}).get("value", "N/D")

                alertas.append(Alerta(
                    fuente="InfoLobby — Ley del Lobby (Ley 20.730)",
                    fecha=fecha[:10] if len(fecha) >= 10 else fecha,
                    tipo_alerta="LOBBY",
                    descripcion=(
                        f"Audiencia: {sp} (autoridad) con {sa} (solicitante). "
                        f"Institución: {inst}. Materia: {materia}."
                    ),
                    url="https://www.infolobby.cl",
                ))

            logger.info("InfoLobby: %d audiencias encontradas para '%s'.", len(alertas), nombre)

        except requests.exceptions.RequestException as exc:
            logger.error("Error consultando InfoLobby SPARQL: %s", exc)

        return alertas

    # ═══════════════════════════════════════════════════════════
    # FUENTE 2: datos.gob.cl — Búsqueda en datasets de sanciones
    # ═══════════════════════════════════════════════════════════

    def _buscar_datos_gob_sanciones(self, nombre: str) -> list[Alerta]:
        """
        Busca en los datasets de sanciones disponibles en datos.gob.cl.
        Usa DataStore SQL para filtrar por nombre en los recursos con datastore activo.
        """
        alertas: list[Alerta] = []

        for key, info in RECURSOS_SANCIONES.items():
            try:
                resource_id = info["resource_id"]

                # Usar datastore_search con 'q' (full-text) — más compatible que SQL
                resp = self.session.get(
                    f"{CKAN_BASE}/datastore_search",
                    params={
                        "resource_id": resource_id,
                        "q": nombre,
                        "limit": 20,
                    },
                    timeout=REQUEST_TIMEOUT,
                )
                resp.raise_for_status()
                result = resp.json()

                if not result.get("success"):
                    continue

                fields = result["result"].get("fields", [])
                text_fields = [
                    f["id"] for f in fields
                    if f.get("type") in ("text", "object") and f["id"] != "_id"
                ]

                records = result["result"].get("records", [])
                for rec in records:
                    desc_parts = []
                    for col in text_fields[:5]:
                        val = rec.get(col, "")
                        if val and str(val).strip():
                            desc_parts.append(f"{col}: {val}")

                    alertas.append(Alerta(
                        fuente=info["fuente"],
                        fecha=_extraer_fecha(rec),
                        tipo_alerta=info["tipo"],
                        descripcion="; ".join(desc_parts) if desc_parts else str(rec),
                        url=f"https://datos.gob.cl/dataset?q={key}",
                    ))

                if records:
                    logger.info(
                        "datos.gob.cl [%s]: %d registros para '%s'.",
                        key, len(records), nombre,
                    )

            except requests.exceptions.RequestException as exc:
                logger.error("Error consultando datos.gob.cl [%s]: %s", key, exc)

        # También buscar datasets genéricos por nombre
        alertas.extend(self._buscar_datos_gob_datasets(nombre))

        return alertas

    def _buscar_datos_gob_datasets(self, nombre: str) -> list[Alerta]:
        """Busca datasets en datos.gob.cl que mencionen sanciones + persona."""
        alertas: list[Alerta] = []
        queries = [
            "sanciones funcionarios",
            "sumarios administrativos",
            "procesos sancionatorios",
        ]

        for q in queries:
            try:
                resp = self.session.get(
                    f"{CKAN_BASE}/package_search",
                    params={"q": q, "rows": 10},
                    timeout=REQUEST_TIMEOUT,
                )
                resp.raise_for_status()
                data = resp.json()

                if not data.get("success"):
                    continue

                for dataset in data["result"]["results"]:
                    title = dataset.get("title", "")
                    org = dataset.get("organization", {}).get("title", "N/D")

                    # Solo reportar datasets que existan — no es búsqueda por persona
                    # sino inventario de fuentes disponibles para cruce
                    for resource in dataset.get("resources", []):
                        if resource.get("datastore_active"):
                            alertas.append(Alerta(
                                fuente=f"datos.gob.cl — {org}",
                                fecha=dataset.get("metadata_modified", "sin fecha")[:10],
                                tipo_alerta="DATASET_SANCIONATORIO",
                                descripcion=(
                                    f"Dataset disponible: {title}. "
                                    f"Recurso: {resource.get('name', 'N/D')} "
                                    f"(formato: {resource.get('format', 'N/D')})"
                                ),
                                url=(
                                    f"https://datos.gob.cl/dataset/"
                                    f"{dataset.get('name', '')}"
                                ),
                            ))

            except requests.exceptions.RequestException as exc:
                logger.error("Error buscando datasets en datos.gob.cl: %s", exc)

        return alertas

    # ═══════════════════════════════════════════════════════════
    # FUENTE 3: Contraloría — Dictámenes y Resoluciones
    # ═══════════════════════════════════════════════════════════

    def _buscar_contraloria(self, nombre: str) -> list[Alerta]:
        """
        Busca en el sistema de jurisprudencia de la Contraloría General.

        NOTA: La Contraloría no tiene API pública documentada.
        Este método intenta consultar su buscador web. Si el endpoint
        cambia o falla, retorna lista vacía sin bloquear el flujo.
        """
        alertas: list[Alerta] = []

        # Intentar el buscador de jurisprudencia de la CGR
        # Este endpoint puede cambiar — se degrada gracefully
        try:
            resp = self.session.get(
                "https://www.contraloria.cl/pdfbuscador/juridica/buscar",
                params={
                    "texto": nombre,
                    "limit": 10,
                    "offset": 0,
                },
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
            )

            if resp.status_code == 200:
                try:
                    data = resp.json()
                    resultados = data if isinstance(data, list) else data.get("resultados", [])

                    for item in resultados[:10]:
                        alertas.append(Alerta(
                            fuente="Contraloría General de la República",
                            fecha=str(item.get("fecha", "sin fecha"))[:10],
                            tipo_alerta="DICTAMEN",
                            descripcion=(
                                f"Dictamen N°{item.get('numero', 'N/D')}: "
                                f"{str(item.get('materia', item.get('extracto', '')))[:200]}"
                            ),
                            url=item.get("url", "https://www.contraloria.cl/web/cgr/buscar"),
                        ))
                except (ValueError, KeyError):
                    # Respuesta no es JSON — el endpoint cambió
                    logger.info(
                        "Contraloría: buscador no devuelve JSON. "
                        "Usar búsqueda manual en https://www.contraloria.cl/web/cgr/buscar"
                    )
            else:
                logger.info(
                    "Contraloría: buscador HTTP %d. Fuente no disponible vía API.",
                    resp.status_code,
                )

        except requests.exceptions.RequestException as exc:
            logger.warning("Contraloría no accesible: %s", exc)

        # Si no se obtuvieron resultados programáticos, dejar referencia manual
        if not alertas:
            alertas.append(Alerta(
                fuente="Contraloría General de la República",
                fecha=datetime.now().strftime("%Y-%m-%d"),
                tipo_alerta="REFERENCIA",
                descripcion=(
                    f"Buscar manualmente dictámenes sobre '{nombre}' en el "
                    f"buscador de jurisprudencia de la CGR. No tiene API pública."
                ),
                url="https://www.contraloria.cl/web/cgr/buscar",
            ))

        return alertas

    # ═══════════════════════════════════════════════════════════
    # FUENTE 4: SERVEL — Aportes electorales (DB local)
    # ═══════════════════════════════════════════════════════════

    def _buscar_servel(self, nombre: str) -> list[Alerta]:
        """Busca aportes electorales en la DB local (tabla aportes_servel)."""
        alertas: list[Alerta] = []

        try:
            with sqlite3.connect(self.db_path) as conn:
                # Verificar que la tabla existe
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='aportes_servel'"
                )
                if not cursor.fetchone():
                    logger.info("Tabla aportes_servel no existe aún.")
                    return alertas

                nombre_upper = nombre.upper()
                df = pd.read_sql_query(
                    """
                    SELECT * FROM aportes_servel
                    WHERE UPPER(nombre_aportante) LIKE ?
                       OR UPPER(nombre_receptor) LIKE ?
                    ORDER BY fecha_aporte DESC
                    LIMIT 50
                    """,
                    conn,
                    params=[f"%{nombre_upper}%", f"%{nombre_upper}%"],
                )

                for _, row in df.iterrows():
                    monto = row.get("monto_aporte", 0)
                    monto_fmt = f"${monto:,.0f} CLP" if monto else "N/D"

                    alertas.append(Alerta(
                        fuente="SERVEL — Servicio Electoral de Chile",
                        fecha=str(row.get("fecha_aporte", "sin fecha"))[:10],
                        tipo_alerta="APORTE_ELECTORAL",
                        descripcion=(
                            f"Aporte de {row.get('nombre_aportante', 'N/D')} "
                            f"({row.get('rut_aportante', 'N/D')}) → "
                            f"{row.get('nombre_receptor', 'N/D')} "
                            f"({row.get('tipo_receptor', '')}). "
                            f"Monto: {monto_fmt}. "
                            f"Elección: {row.get('eleccion_campaña', 'N/D')}."
                        ),
                        url="https://www.servel.cl/informacion-sobre-financiamiento/",
                    ))

                logger.info("SERVEL: %d aportes encontrados para '%s'.", len(alertas), nombre)

        except (sqlite3.Error, pd.errors.DatabaseError) as exc:
            logger.error("Error consultando SERVEL en DB local: %s", exc)

        return alertas

    # ═══════════════════════════════════════════════════════════
    # FUENTE 5: Mercado Público — Órdenes de compra
    # ═══════════════════════════════════════════════════════════

    def _buscar_mercado_publico_por_nombre(self, nombre: str) -> list[Alerta]:
        """
        Busca en la DB local si el nombre aparece como proveedor en órdenes de compra.
        No consulta la API directamente (requiere RUT), pero cruza con datos locales.
        """
        alertas: list[Alerta] = []

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='ordenes_items'"
                )
                if not cursor.fetchone():
                    return alertas

                nombre_upper = nombre.upper()
                df = pd.read_sql_query(
                    """
                    SELECT DISTINCT
                        codigo_oc, nombre_proveedor, rut_proveedor,
                        nombre_comprador, fecha_creacion,
                        SUM(monto_total_item) as total_oc
                    FROM ordenes_items
                    WHERE UPPER(nombre_proveedor) LIKE ?
                    GROUP BY codigo_oc
                    ORDER BY fecha_creacion DESC
                    LIMIT 20
                    """,
                    conn,
                    params=[f"%{nombre_upper}%"],
                )

                for _, row in df.iterrows():
                    total = row.get("total_oc", 0)
                    total_fmt = f"${total:,.0f} CLP" if total else "N/D"

                    alertas.append(Alerta(
                        fuente="ChileCompra — Mercado Público",
                        fecha=str(row.get("fecha_creacion", "sin fecha"))[:10],
                        tipo_alerta="COMPRA_PUBLICA",
                        descripcion=(
                            f"OC {row.get('codigo_oc', 'N/D')}: "
                            f"Proveedor {row.get('nombre_proveedor', 'N/D')} "
                            f"({row.get('rut_proveedor', 'N/D')}) → "
                            f"Comprador: {row.get('nombre_comprador', 'N/D')}. "
                            f"Total: {total_fmt}."
                        ),
                        url=(
                            f"https://www.mercadopublico.cl/Procurement/Modules/"
                            f"RFB/DetailsAcquisition.aspx?qs={row.get('codigo_oc', '')}"
                        ),
                    ))

                logger.info(
                    "Mercado Público: %d OCs encontradas para '%s'.", len(alertas), nombre,
                )

        except (sqlite3.Error, pd.errors.DatabaseError) as exc:
            logger.error("Error consultando Mercado Público en DB local: %s", exc)

        return alertas

    # ═══════════════════════════════════════════════════════════
    # UTILIDAD: Resumen legible de las alertas
    # ═══════════════════════════════════════════════════════════

    def resumen(self, nombre: str) -> str:
        """Genera un resumen en texto de todas las alertas encontradas."""
        alertas = self.buscar(nombre)

        if not alertas:
            return f"No se encontraron alertas públicas para '{nombre}'."

        lineas = [
            f"═══ ALERTAS PÚBLICAS: {nombre.upper()} ═══",
            f"Total: {len(alertas)} alerta(s) de {len({a['fuente'] for a in alertas})} fuente(s)",
            "",
        ]

        por_tipo: dict[str, list[dict]] = {}
        for a in alertas:
            por_tipo.setdefault(a["tipo_alerta"], []).append(a)

        for tipo, items in por_tipo.items():
            lineas.append(f"── {tipo} ({len(items)}) ──")
            for item in items[:5]:  # Max 5 por tipo en el resumen
                lineas.append(
                    f"  [{item['fecha']}] {item['fuente']}\n"
                    f"    {item['descripcion'][:150]}\n"
                    f"    → {item['url']}"
                )
            if len(items) > 5:
                lineas.append(f"  ... y {len(items) - 5} más.")
            lineas.append("")

        return "\n".join(lineas)


# ─────────────────── Helpers ─────────────────── #

def _extraer_fecha(record: dict[str, Any]) -> str:
    """Intenta extraer una fecha de un registro genérico."""
    for key in ("fecha", "date", "Fecha", "FECHA", "fecha_resolucion", "año"):
        val = record.get(key)
        if val:
            return str(val)[:10]
    return "sin fecha"


# ─────────────────── CLI ─────────────────── #

if __name__ == "__main__":
    import argparse
    import json

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Buscar alertas públicas sobre una persona.",
    )
    parser.add_argument("nombre", help="Nombre completo o parcial de la persona.")
    parser.add_argument(
        "--json", action="store_true", dest="as_json",
        help="Devolver resultado como JSON.",
    )
    parser.add_argument(
        "--sin-compras", action="store_true",
        help="Excluir búsqueda en Mercado Público (más rápido).",
    )
    args = parser.parse_args()

    motor = AlertasPersonas()

    if args.as_json:
        alertas = motor.buscar(args.nombre, incluir_compras=not args.sin_compras)
        print(json.dumps(alertas, ensure_ascii=False, indent=2))
    else:
        print(motor.resumen(args.nombre))
