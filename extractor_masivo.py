import logging
import time

from infolobby_connector import InfoLobbyConnector
import sqlite3

DB_PATH = "auditoria_estado.db"

logger = logging.getLogger(__name__)

def init_db_tables():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lobby_audiencias (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha TEXT,
                sujeto_pasivo TEXT,
                sujeto_activo TEXT,
                institucion TEXT,
                materia TEXT,
                UNIQUE(fecha, sujeto_pasivo, sujeto_activo)
            )
        """)
        conn.commit()

def vacuum_infolobby():
    logger.info("Iniciando Motor de Extracción Masiva (Modo Sigiloso)...")
    logger.info("Conectando al Endpoint SPARQL del Estado de Chile (InfoLobby)...")
    lobby = InfoLobbyConnector()

    guardados_totales = 0
    lote_size = 200
    objetivo_total = 2000

    logger.info("Extrayendo %d reuniones en lotes de %d...", objetivo_total, lote_size)

    with sqlite3.connect(DB_PATH) as conn:
        for offset in range(0, objetivo_total, lote_size):
            logger.info("Descargando bloque %d a %d...", offset, offset + lote_size)

            sparql_paginado = f"""
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
                }}
                ORDER BY DESC(?fecha)
                LIMIT {lote_size}
                OFFSET {offset}
            """

            df_lote = lobby.query_sparql(sparql_paginado)

            if df_lote.empty:
                logger.warning("Lote vacío o bloqueado. Pausando...")
                time.sleep(5)
                continue

            guardados_lote = 0
            for _, row in df_lote.iterrows():
                try:
                    conn.execute("""
                        INSERT INTO lobby_audiencias
                        (fecha, sujeto_pasivo, sujeto_activo, institucion, materia)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        str(row.get('fecha', '')),
                        str(row.get('sujetoPasivo', '')),
                        str(row.get('sujetoActivo', '')),
                        str(row.get('institucion', '')),
                        str(row.get('materia', ''))
                    ))
                    guardados_lote += 1
                except sqlite3.IntegrityError:
                    pass  # Ignorar duplicados

            conn.commit()
            guardados_totales += guardados_lote
            logger.info("%d nuevos inyectados. Esperando 3 segundos...", guardados_lote)
            time.sleep(3)

    logger.info("Extracción terminada. Se añadieron %d audiencias a la DB.", guardados_totales)

if __name__ == "__main__":
    init_db_tables()
    vacuum_infolobby()
