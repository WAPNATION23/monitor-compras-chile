"""
InfoProbidadConnector — Conector de InfoProbidad.cl (SPARQL + CSV)
═══════════════════════════════════════════════════════════════════
Accede a las declaraciones de patrimonio e intereses de funcionarios públicos
publicadas bajo la Ley N° 20.880 sobre Probidad en la Función Pública.

Fuentes:
  • SPARQL endpoint: http://datos.cplt.cl/sparql
  • CSV catálogos: https://www.infoprobidad.cl/DatosAbiertos/Catalogos
  • 116,011+ declaraciones publicadas (actualización semanal)

Datos disponibles:
  • Declarantes: nombre, cargo, institución, comuna de desempeño
  • Actividades: participación en sociedades, directorios, gremios
  • Bienes: inmuebles, muebles (vehículos, etc.)
  • Acciones/Derechos: participación en empresas
  • Pasivos: deudas superiores a 100 UTM

Uso forense:
    from infoprobidad_connector import InfoProbidadConnector
    ip = InfoProbidadConnector()

    # Buscar declaraciones de un funcionario
    decls = ip.buscar_declarante("Juan Pérez")

    # Buscar actividades económicas (sociedades, directorios)
    acts = ip.buscar_actividades("María González")

    # Cruce: funcionarios con intereses en empresa proveedora
    cruces = ip.cruzar_con_proveedor("76.123.456-7")
"""

from __future__ import annotations

import logging
import re
import sqlite3
from typing import Any

import pandas as pd
import requests

from config import DB_NAME, REQUEST_TIMEOUT

logger = logging.getLogger(__name__)

# ─────────────────── Endpoints ─────────────────── #

SPARQL_ENDPOINT: str = "http://datos.cplt.cl/sparql"
INFOPROBIDAD_BASE: str = "https://www.infoprobidad.cl"

# Prefijos RDF del modelo InfoProbidad
SPARQL_PREFIXES: str = """
PREFIX dip: <http://datos.cplt.cl/def#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX org: <http://www.w3.org/ns/org#>
"""

# ─────────────────── SQL para tabla local ─────────────────── #

CREATE_DECLARANTES_SQL: str = """
CREATE TABLE IF NOT EXISTS declarantes_probidad (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre TEXT NOT NULL,
    cargo TEXT,
    institucion TEXT,
    comuna_desempenio TEXT,
    fecha_declaracion TEXT,
    tipo_declaracion TEXT,
    actividades TEXT,
    bienes TEXT,
    acciones_derechos TEXT,
    pasivos TEXT,
    url_declaracion TEXT,
    fecha_ingreso TEXT DEFAULT (datetime('now'))
)
"""

CREATE_ACTIVIDADES_SQL: str = """
CREATE TABLE IF NOT EXISTS actividades_probidad (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre_declarante TEXT NOT NULL,
    cargo TEXT,
    institucion TEXT,
    nombre_actividad TEXT,
    tipo_actividad TEXT,
    fecha_declaracion TEXT,
    fecha_ingreso TEXT DEFAULT (datetime('now'))
)
"""


class InfoProbidadConnector:
    """Conector para datos de InfoProbidad (declaraciones de probidad)."""

    def __init__(self, db_path: str = DB_NAME) -> None:
        self.db_path = db_path
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/sparql-results+json",
            "User-Agent": "MonitorComprasChile/1.0 (Auditoría Cívica)",
        })
        self._init_db()

    def _init_db(self) -> None:
        """Crea las tablas si no existen."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(CREATE_DECLARANTES_SQL)
                conn.execute(CREATE_ACTIVIDADES_SQL)
        except sqlite3.Error as exc:
            logger.error("Error inicializando tablas InfoProbidad: %s", exc)

    # ═══════════════════════════════════════════════════════════
    # SPARQL Queries
    # ═══════════════════════════════════════════════════════════

    def _sparql_query(self, query: str) -> list[dict[str, Any]]:
        """Ejecuta una consulta SPARQL y retorna los bindings."""
        try:
            resp = self.session.get(
                SPARQL_ENDPOINT,
                params={"query": query, "format": "json"},
                timeout=REQUEST_TIMEOUT * 2,
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("results", {}).get("bindings", [])
        except requests.exceptions.RequestException as exc:
            logger.error("Error SPARQL InfoProbidad: %s", exc)
            return []
        except (ValueError, KeyError) as exc:
            logger.error("Error parseando respuesta SPARQL: %s", exc)
            return []

    # ═══════════════════════════════════════════════════════════
    # Buscar declarantes por nombre
    # ═══════════════════════════════════════════════════════════

    def buscar_declarante(self, nombre: str, limit: int = 50) -> list[dict[str, str]]:
        """
        Busca declaraciones de un funcionario por nombre.

        Returns:
            Lista de dicts con: nombre, cargo, institucion, fecha_declaracion, tipo
        """
        nombre_safe = re.sub(r'[\\"\'\}\)\{\(<>;&|]', '', nombre).strip()
        if not nombre_safe:
            return []

        query = f"""{SPARQL_PREFIXES}
        SELECT DISTINCT ?nombre ?cargo ?institucion ?fecha ?tipo
        WHERE {{
            ?persona a dip:Persona ;
                     foaf:name ?nombre .
            OPTIONAL {{ ?persona dip:poseeCargo ?cargoNode . ?cargoNode rdfs:label ?cargo . }}
            OPTIONAL {{ ?persona org:memberOf ?instNode . ?instNode foaf:name ?institucion . }}
            OPTIONAL {{ ?persona dip:declara ?decl . ?decl dip:fechaDeclaracion ?fecha . }}
            OPTIONAL {{ ?decl dip:tipoDeclaracion ?tipo . }}
            FILTER(CONTAINS(LCASE(?nombre), LCASE("{nombre_safe}")))
        }}
        ORDER BY DESC(?fecha)
        LIMIT {limit}
        """

        bindings = self._sparql_query(query)
        results = []
        for b in bindings:
            results.append({
                "nombre": b.get("nombre", {}).get("value", ""),
                "cargo": b.get("cargo", {}).get("value", "N/D"),
                "institucion": b.get("institucion", {}).get("value", "N/D"),
                "fecha_declaracion": b.get("fecha", {}).get("value", "sin fecha")[:10],
                "tipo_declaracion": b.get("tipo", {}).get("value", "N/D"),
            })

        logger.info("InfoProbidad: %d declaraciones para '%s'.", len(results), nombre)
        return results

    # ═══════════════════════════════════════════════════════════
    # Buscar actividades económicas (sociedades, directorios)
    # ═══════════════════════════════════════════════════════════

    def buscar_actividades(self, nombre: str, limit: int = 100) -> list[dict[str, str]]:
        """
        Busca actividades económicas declaradas por un funcionario.
        Esto incluye participación en sociedades, directorios, gremios.

        Returns:
            Lista de dicts con: nombre, cargo, institucion, actividad, tipo_actividad
        """
        nombre_safe = re.sub(r'[\\"\'\}\)\{\(<>;&|]', '', nombre).strip()
        if not nombre_safe:
            return []

        query = f"""{SPARQL_PREFIXES}
        SELECT DISTINCT ?nombre ?cargo ?institucion ?actividad ?tipoActividad
        WHERE {{
            ?persona a dip:Persona ;
                     foaf:name ?nombre ;
                     dip:tieneActividad ?actNode .
            ?actNode rdfs:label ?actividad .
            OPTIONAL {{ ?actNode dip:tipoActividad ?tipoActividad . }}
            OPTIONAL {{ ?persona dip:poseeCargo ?cargoNode . ?cargoNode rdfs:label ?cargo . }}
            OPTIONAL {{ ?persona org:memberOf ?instNode . ?instNode foaf:name ?institucion . }}
            FILTER(CONTAINS(LCASE(?nombre), LCASE("{nombre_safe}")))
        }}
        LIMIT {limit}
        """

        bindings = self._sparql_query(query)
        results = []
        for b in bindings:
            results.append({
                "nombre": b.get("nombre", {}).get("value", ""),
                "cargo": b.get("cargo", {}).get("value", "N/D"),
                "institucion": b.get("institucion", {}).get("value", "N/D"),
                "actividad": b.get("actividad", {}).get("value", "N/D"),
                "tipo_actividad": b.get("tipoActividad", {}).get("value", "N/D"),
            })

        logger.info("InfoProbidad: %d actividades para '%s'.", len(results), nombre)
        return results

    # ═══════════════════════════════════════════════════════════
    # Buscar bienes declarados
    # ═══════════════════════════════════════════════════════════

    def buscar_bienes(self, nombre: str, limit: int = 100) -> list[dict[str, str]]:
        """Busca bienes muebles e inmuebles declarados por un funcionario."""
        nombre_safe = re.sub(r'[\\"\'\}\)\{\(<>;&|]', '', nombre).strip()
        if not nombre_safe:
            return []

        query = f"""{SPARQL_PREFIXES}
        SELECT DISTINCT ?nombre ?bien ?tipoBien
        WHERE {{
            ?persona a dip:Persona ;
                     foaf:name ?nombre ;
                     dip:tieneBien ?bienNode .
            ?bienNode rdfs:label ?bien .
            OPTIONAL {{ ?bienNode a ?tipoBien . FILTER(?tipoBien != dip:Bien) }}
            FILTER(CONTAINS(LCASE(?nombre), LCASE("{nombre_safe}")))
        }}
        LIMIT {limit}
        """

        bindings = self._sparql_query(query)
        results = []
        for b in bindings:
            results.append({
                "nombre": b.get("nombre", {}).get("value", ""),
                "bien": b.get("bien", {}).get("value", "N/D"),
                "tipo_bien": b.get("tipoBien", {}).get("value", "N/D"),
            })

        logger.info("InfoProbidad: %d bienes para '%s'.", len(results), nombre)
        return results

    # ═══════════════════════════════════════════════════════════
    # Buscar acciones/derechos en sociedades
    # ═══════════════════════════════════════════════════════════

    def buscar_acciones_derechos(self, nombre: str, limit: int = 100) -> list[dict[str, str]]:
        """
        Busca acciones o derechos en sociedades declarados por un funcionario.
        CLAVE para detectar conflictos de interés con proveedores del Estado.
        """
        nombre_safe = re.sub(r'[\\"\'\}\)\{\(<>;&|]', '', nombre).strip()
        if not nombre_safe:
            return []

        query = f"""{SPARQL_PREFIXES}
        SELECT DISTINCT ?nombre ?cargo ?institucion ?accion
        WHERE {{
            ?persona a dip:Persona ;
                     foaf:name ?nombre ;
                     dip:tieneAccionDerecho ?accionNode .
            ?accionNode rdfs:label ?accion .
            OPTIONAL {{ ?persona dip:poseeCargo ?cargoNode . ?cargoNode rdfs:label ?cargo . }}
            OPTIONAL {{ ?persona org:memberOf ?instNode . ?instNode foaf:name ?institucion . }}
            FILTER(CONTAINS(LCASE(?nombre), LCASE("{nombre_safe}")))
        }}
        LIMIT {limit}
        """

        bindings = self._sparql_query(query)
        results = []
        for b in bindings:
            results.append({
                "nombre": b.get("nombre", {}).get("value", ""),
                "cargo": b.get("cargo", {}).get("value", "N/D"),
                "institucion": b.get("institucion", {}).get("value", "N/D"),
                "accion_derecho": b.get("accion", {}).get("value", "N/D"),
            })

        logger.info("InfoProbidad: %d acciones/derechos para '%s'.", len(results), nombre)
        return results

    # ═══════════════════════════════════════════════════════════
    # Cruce forense: funcionario ↔ proveedor
    # ═══════════════════════════════════════════════════════════

    def cruzar_con_proveedor(self, rut_o_nombre_proveedor: str) -> list[dict[str, str]]:
        """
        Busca si algún funcionario público tiene actividades o acciones
        en sociedades cuyo nombre coincida con un proveedor del Estado.

        Este es el cruce más poderoso: detecta conflictos de interés directos.

        Args:
            rut_o_nombre_proveedor: Nombre de la empresa proveedora a buscar
                                    en las declaraciones de funcionarios.

        Returns:
            Lista de funcionarios con actividades/acciones vinculadas al proveedor.
        """
        nombre_safe = re.sub(r'[\\"\'\}\)\{\(<>;&|]', '', rut_o_nombre_proveedor).strip()
        if not nombre_safe or len(nombre_safe) < 3:
            return []

        # Buscar en actividades Y acciones/derechos
        query = f"""{SPARQL_PREFIXES}
        SELECT DISTINCT ?nombre ?cargo ?institucion ?vinculo ?tipoVinculo
        WHERE {{
            ?persona a dip:Persona ;
                     foaf:name ?nombre .
            OPTIONAL {{ ?persona dip:poseeCargo ?cargoNode . ?cargoNode rdfs:label ?cargo . }}
            OPTIONAL {{ ?persona org:memberOf ?instNode . ?instNode foaf:name ?institucion . }}
            {{
                ?persona dip:tieneActividad ?node .
                ?node rdfs:label ?vinculo .
                BIND("ACTIVIDAD" AS ?tipoVinculo)
            }} UNION {{
                ?persona dip:tieneAccionDerecho ?node .
                ?node rdfs:label ?vinculo .
                BIND("ACCION/DERECHO" AS ?tipoVinculo)
            }}
            FILTER(CONTAINS(LCASE(?vinculo), LCASE("{nombre_safe}")))
        }}
        LIMIT 50
        """

        bindings = self._sparql_query(query)
        results = []
        for b in bindings:
            results.append({
                "funcionario": b.get("nombre", {}).get("value", ""),
                "cargo": b.get("cargo", {}).get("value", "N/D"),
                "institucion": b.get("institucion", {}).get("value", "N/D"),
                "vinculo_declarado": b.get("vinculo", {}).get("value", "N/D"),
                "tipo_vinculo": b.get("tipoVinculo", {}).get("value", "N/D"),
            })

        if results:
            logger.warning(
                "🚨 InfoProbidad: %d funcionarios con vínculos a '%s'!",
                len(results), rut_o_nombre_proveedor,
            )
        else:
            logger.info("InfoProbidad: sin vínculos encontrados para '%s'.", rut_o_nombre_proveedor)

        return results

    # ═══════════════════════════════════════════════════════════
    # Guardar resultados en DB local
    # ═══════════════════════════════════════════════════════════

    def guardar_declarantes(self, declarantes: list[dict[str, str]]) -> int:
        """Guarda declarantes en la DB local. Retorna cantidad insertada."""
        if not declarantes:
            return 0

        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(CREATE_DECLARANTES_SQL)
                df = pd.DataFrame(declarantes)
                col_map = {
                    "nombre": "nombre",
                    "cargo": "cargo",
                    "institucion": "institucion",
                    "fecha_declaracion": "fecha_declaracion",
                    "tipo_declaracion": "tipo_declaracion",
                }
                df = df.rename(columns=col_map)
                df.to_sql("declarantes_probidad", conn, if_exists="append", index=False)
                count = len(df)
                logger.info("InfoProbidad: %d declarantes guardados en DB.", count)
                return count
        except (sqlite3.Error, Exception) as exc:
            logger.error("Error guardando en DB: %s", exc)
            return 0

    def guardar_actividades(self, actividades: list[dict[str, str]]) -> int:
        """Guarda actividades en la DB local. Retorna cantidad insertada."""
        if not actividades:
            return 0

        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(CREATE_ACTIVIDADES_SQL)
                df = pd.DataFrame(actividades)
                rename = {
                    "nombre": "nombre_declarante",
                    "actividad": "nombre_actividad",
                    "tipo_actividad": "tipo_actividad",
                }
                df = df.rename(columns=rename)
                cols = ["nombre_declarante", "cargo", "institucion",
                        "nombre_actividad", "tipo_actividad"]
                for c in cols:
                    if c not in df.columns:
                        df[c] = "N/D"
                df[cols].to_sql("actividades_probidad", conn, if_exists="append", index=False)
                count = len(df)
                logger.info("InfoProbidad: %d actividades guardadas en DB.", count)
                return count
        except (sqlite3.Error, Exception) as exc:
            logger.error("Error guardando actividades en DB: %s", exc)
            return 0

    # ═══════════════════════════════════════════════════════════
    # Consultar datos locales
    # ═══════════════════════════════════════════════════════════

    def declarantes_por_institucion(self, institucion: str) -> pd.DataFrame:
        """Busca declarantes guardados localmente por institución."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                return pd.read_sql_query(
                    """
                    SELECT * FROM declarantes_probidad
                    WHERE UPPER(institucion) LIKE ?
                    ORDER BY fecha_declaracion DESC
                    """,
                    conn,
                    params=[f"%{institucion.upper()}%"],
                )
        except (sqlite3.Error, pd.errors.DatabaseError):
            return pd.DataFrame()
