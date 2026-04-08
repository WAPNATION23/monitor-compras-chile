"""
InfoLobbyConnector — Conector de Ley del Lobby (InfoLobby.cl)
════════════════════════════════════════════════════════════
Descarga y procesa los datos abiertos de audiencias, viajes y donativos
publicados bajo la Ley N° 20.730 (Ley del Lobby).

Fuentes:
  • CSV directos de https://www.infolobby.cl (Catálogos de Datos Abiertos)
  • SPARQL endpoint: http://datos.infolobby.cl/sparql

Uso forense:
  El cruce de audiencias con compras públicas permite detectar:
  • Autoridad se reúne con representante de empresa → empresa gana licitación
  • Patrones de frecuencia anormal de audiencias previas a adjudicaciones
  • Donativos a autoridades que luego favorecen al donante en compras

Uso:
    from infolobby_connector import InfoLobbyConnector
    lobby = InfoLobbyConnector()

    # Descargar audiencias recientes
    audiencias = lobby.descargar_audiencias()

    # Buscar audiencias de una autoridad específica
    resultado = lobby.buscar_por_autoridad("subsecretario")
"""

from __future__ import annotations

import logging

import pandas as pd
import requests

from config import REQUEST_TIMEOUT

logger = logging.getLogger(__name__)

# ─────────────────── URLs de InfoLobby ─────────────────── #

# Catálogos CSV de datos abiertos (Ley del Lobby)
INFOLOBBY_BASE: str = "https://www.infolobby.cl"
SPARQL_ENDPOINT: str = "http://datos.infolobby.cl/sparql"

# URLs conocidas de catálogos CSV desde datos.gob.cl / infolobby
# Estas se actualizan periódicamente. Si fallan, se buscan dinámicamente.
CATALOGOS_CONOCIDOS: dict[str, str] = {
    "audiencias": "https://www.infolobby.cl/Datos/audienciasBuscar",
    "sujetos_pasivos": "https://www.infolobby.cl/Datos/sujetosPasivosDescargar",
    "sujetos_activos": "https://www.infolobby.cl/Datos/sujetosActivosDescargar",
    "viajes": "https://www.infolobby.cl/Datos/viajesDescargar",
    "donativos": "https://www.infolobby.cl/Datos/donativosDescargar",
}


class InfoLobbyConnector:
    """Conector para datos de la Ley del Lobby de Chile."""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "text/csv, application/json",
            "User-Agent": "MonitorComprasChile/1.0 (Auditoría Cívica)",
        })

    # ─────────────────── SPARQL query ─────────────────── #

    def query_sparql(self, sparql_query: str) -> pd.DataFrame:
        """
        Ejecuta una consulta SPARQL contra el endpoint de datos enlazados.

        Args:
            sparql_query: Consulta SPARQL.

        Returns:
            DataFrame con los resultados.
        """
        try:
            response = self.session.get(
                SPARQL_ENDPOINT,
                params={
                    "query": sparql_query,
                    "format": "json",
                },
                timeout=REQUEST_TIMEOUT * 2,  # SPARQL puede ser lento
            )
            response.raise_for_status()
            data = response.json()

            # Parsear resultado SPARQL JSON
            bindings = data.get("results", {}).get("bindings", [])
            if not bindings:
                logger.info("Consulta SPARQL sin resultados.")
                return pd.DataFrame()

            # Extraer valores de cada binding
            records = []
            for binding in bindings:
                record = {k: v.get("value", "") for k, v in binding.items()}
                records.append(record)

            df = pd.DataFrame(records)
            logger.info("SPARQL: %d resultados obtenidos.", len(df))
            return df

        except requests.exceptions.RequestException as exc:
            logger.error("Error en consulta SPARQL: %s", exc)
            return pd.DataFrame()

    # ─────────────────── Audiencias recientes ─────────────────── #

    def descargar_audiencias(
        self,
        fecha_desde: str | None = None,
        fecha_hasta: str | None = None,
        institucion: str | None = None,
    ) -> pd.DataFrame:
        """
        Descarga audiencias de la Ley del Lobby.

        Intenta primero via SPARQL, y si falla, intenta el catálogo CSV.

        Args:
            fecha_desde: Fecha inicio (YYYY-MM-DD).
            fecha_hasta: Fecha fin (YYYY-MM-DD).
            institucion: Filtrar por nombre de institución.

        Returns:
            DataFrame con las audiencias.
        """
        # Intentar vía SPARQL (datos estructurados)
        sparql = """
        PREFIX lobby: <http://datos.infolobby.cl/def#>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX dcterms: <http://purl.org/dc/terms/>

        SELECT ?audiencia ?fecha ?sujetoPasivo ?sujetoActivo ?institucion ?materia
        WHERE {
            ?audiencia a lobby:Audiencia ;
                       dcterms:date ?fecha ;
                       lobby:sujetoPasivo ?sp ;
                       lobby:sujetoActivo ?sa .
            ?sp foaf:name ?sujetoPasivo .
            ?sa foaf:name ?sujetoActivo .
            OPTIONAL { ?sp lobby:institucion ?inst . ?inst foaf:name ?institucion . }
            OPTIONAL { ?audiencia lobby:materia ?materia . }
        }
        ORDER BY DESC(?fecha)
        LIMIT 500
        """

        df = self.query_sparql(sparql)

        if not df.empty:
            # Aplicar filtros
            if fecha_desde:
                df = df[df["fecha"] >= fecha_desde]
            if fecha_hasta:
                df = df[df["fecha"] <= fecha_hasta]
            if institucion:
                mask = df["institucion"].str.contains(institucion, case=False, na=False)
                df = df[mask]

        return df

    # ─────────────────── Buscar por autoridad ─────────────────── #

    def buscar_por_autoridad(self, nombre: str, limit: int = 100) -> pd.DataFrame:
        """
        Busca audiencias donde participa una autoridad específica.

        Args:
            nombre: Nombre (parcial) de la autoridad.
            limit: Máximo de resultados.
        """
        # Sanitizar input para prevenir SPARQL injection
        nombre_safe = nombre.replace("\\", "\\\\").replace('"', '\\"').replace("'", "\\'")
        limit = min(max(int(limit), 1), 1000)
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
            FILTER(CONTAINS(LCASE(?sujetoPasivo), LCASE("{nombre_safe}")))
        }}
        ORDER BY DESC(?fecha)
        LIMIT {limit}
        """
        return self.query_sparql(sparql)

    # ─────────────────── Buscar lobbistas de una empresa ─────────────────── #

    def buscar_por_empresa(self, nombre_empresa: str, limit: int = 100) -> pd.DataFrame:
        """
        Busca audiencias donde un representante de una empresa específica
        se reunió con autoridades.

        Args:
            nombre_empresa: Nombre (parcial) de la empresa.
            limit: Máximo de resultados.
        """
        # Sanitizar input para prevenir SPARQL injection
        nombre_safe = nombre_empresa.replace("\\", "\\\\").replace('"', '\\"').replace("'", "\\'")
        limit = min(max(int(limit), 1), 1000)
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
            FILTER(CONTAINS(LCASE(?sujetoActivo), LCASE("{nombre_safe}")))
        }}
        ORDER BY DESC(?fecha)
        LIMIT {limit}
        """
        return self.query_sparql(sparql)

    # ─────────────────── Donativos ─────────────────── #

    def descargar_donativos(self, limit: int = 500) -> pd.DataFrame:
        """Descarga el registro de donativos recibidos por autoridades."""
        sparql = f"""
        PREFIX lobby: <http://datos.infolobby.cl/def#>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX dcterms: <http://purl.org/dc/terms/>

        SELECT ?fecha ?receptor ?donante ?descripcion ?monto
        WHERE {{
            ?donativo a lobby:Donativo ;
                      dcterms:date ?fecha ;
                      lobby:receptor ?r ;
                      lobby:donante ?d .
            ?r foaf:name ?receptor .
            ?d foaf:name ?donante .
            OPTIONAL {{ ?donativo lobby:descripcion ?descripcion . }}
            OPTIONAL {{ ?donativo lobby:monto ?monto . }}
        }}
        ORDER BY DESC(?fecha)
        LIMIT {limit}
        """
        return self.query_sparql(sparql)

    # ─────────────────── Viajes ─────────────────── #

    def descargar_viajes(self, limit: int = 500) -> pd.DataFrame:
        """Descarga el registro de viajes de autoridades financiados por terceros."""
        sparql = f"""
        PREFIX lobby: <http://datos.infolobby.cl/def#>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX dcterms: <http://purl.org/dc/terms/>

        SELECT ?fecha ?autoridad ?financiador ?destino ?motivo
        WHERE {{
            ?viaje a lobby:Viaje ;
                   dcterms:date ?fecha ;
                   lobby:autoridad ?auth ;
                   lobby:financiador ?fin .
            ?auth foaf:name ?autoridad .
            ?fin foaf:name ?financiador .
            OPTIONAL {{ ?viaje lobby:destino ?destino . }}
            OPTIONAL {{ ?viaje lobby:motivo ?motivo . }}
        }}
        ORDER BY DESC(?fecha)
        LIMIT {limit}
        """
        return self.query_sparql(sparql)
