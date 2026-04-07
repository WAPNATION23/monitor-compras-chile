import argparse
import sys
import time
from infolobby_connector import InfoLobbyConnector
import sqlite3
from pathlib import Path
import os

DB_PATH = "auditoria_estado.db"

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
    print("🚀 Iniciando Motor de Extracción Masiva (Modo Sigiloso)...")
    print("📡 Conectando al Endpoint SPARQL del Estado de Chile (InfoLobby)...")
    lobby = InfoLobbyConnector()
    
    conn = sqlite3.connect(DB_PATH)
    guardados_totales = 0
    lote_size = 200
    objetivo_total = 2000
    
    print(f"⏳ Extrayendo {objetivo_total} reuniones en lotes de {lote_size} para evadir el Firewall (HTTP 403)...\\n")
    
    for offset in range(0, objetivo_total, lote_size):
        print(f"   [+] Descargando bloque {offset} a {offset + lote_size}...")
        
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
            print("⚠️ Lote vacío o bloqueado. Pausando ejecución para despistar sistema anti-bot...")
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
                pass # Ignorar duplicados
                
        conn.commit()
        guardados_totales += guardados_lote
        print(f"   ✔️ {guardados_lote} nuevos inyectados. Esperando 3 segundos...")
        time.sleep(3) # Delay crítico para no saturar al Estado
        
    conn.close()
    print(f"\\n✅ ¡Listo! Campaña de extracción terminada. Se añadieron {guardados_totales} audiencias a la DB.")
    print("🛡️ Módulo de InfoLobby completo. El Ojo del Pueblo se expande.")

if __name__ == "__main__":
    init_db_tables()
    vacuum_infolobby()
