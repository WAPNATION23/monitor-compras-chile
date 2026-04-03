"""
ServelExtractor — Motor de Inteligencia Electoral
════════════════════════════════════════════════════
Módulo especializado en Extracción y Web Scraping de Aportes a Campañas.

Lee datos publicados oficialmente (CSVs o tablas web de Transparencia Activa)
del SERVEL o fuentes públicas consolidadas (datos.gob.cl u observatorios) 
para poblar la tabla `aportes_servel` donde se registran los RUT de 
financistas políticos.

Uso forense:
  Permitir conectar (RUT o Nombre completo de "Proveedores" de Mercado Público) 
  con (RUT/Nombres de "Financistas de Campaña/Partido").
"""

from __future__ import annotations

import logging
import sqlite3
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from typing import Any

from config import DB_NAME, REQUEST_TIMEOUT

logger = logging.getLogger(__name__)

class ServelExtractor:
    """Extrae aportes electorales e información financiera de partidos/campañas."""

    def __init__(self, db_path: str | Path = DB_NAME) -> None:
        self.db_path = Path(db_path)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 Auditoria Forense Chile - ProyMonitor 1.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
        })
        self._init_db()

    def _init_db(self) -> None:
        """Crea la tabla `aportes_servel` si no existe."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS aportes_servel (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rut_aportante TEXT,
                    nombre_aportante TEXT,
                    rut_receptor TEXT,
                    nombre_receptor TEXT,
                    tipo_receptor TEXT,  -- ej. 'PARTIDO', 'CANDIDATO', 'INDEPENDIENTE'
                    monto_aporte REAL,
                    fecha_aporte TIMESTAMP,
                    eleccion_campaña TEXT,
                    UNIQUE(rut_aportante, rut_receptor, monto_aporte, fecha_aporte)
                )
                """
            )
            conn.commit()

    def procesar_csv_aportes(self, filepath_or_url: str) -> pd.DataFrame:
        """
        Lee datos del SERVEL (Consolidados o CSV de Ley de Transparencia)
        y lo normaliza independientemente si está en formato Csv o Excel.
        """
        logger.info(f"Procesando origen de aportes SERVEL: {filepath_or_url}")
        try:
            if filepath_or_url.endswith('.xlsx') or filepath_or_url.endswith('.xls'):
                df = pd.read_excel(filepath_or_url)
            else:
                try:
                    df = pd.read_csv(filepath_or_url, delimiter=";", encoding="utf-8")
                except:
                    # Alternativas de codificación/Separadores
                    df = pd.read_csv(filepath_or_url, delimiter=",", encoding="latin1")

            # Normalización rápida a minúsculas para encontrar las columnas clave
            df.columns = df.columns.str.lower().str.strip()
            
            # Mapas heurísticos de columnas típicas de las bases de SERVEL
            col_map = {
                'rut_aportante': next((c for c in df.columns if 'rut aportante' in c or 'rut_aportante' in c or 'rut donante' in c or 'rut_don' in c), None),
                'nombre_aportante': next((c for c in df.columns if 'nombre aportante' in c or 'nombre_aportante' in c or 'nombre_don' in c or 'aportante' in c), None),
                'rut_receptor': next((c for c in df.columns if 'rut receptor' in c or 'rut_receptor' in c or 'rut candidato' in c or 'rut partido' in c), None),
                'nombre_receptor': next((c for c in df.columns if 'nombre receptor' in c or 'nombre_receptor' in c or 'candidato' in c or 'partido' in c), None),
                'monto_aporte': next((c for c in df.columns if 'monto' in c or 'aporte' in c or 'valor' in c), None),
                'fecha_aporte': next((c for c in df.columns if 'fecha' in c), None),
                'eleccion_campaña': next((c for c in df.columns if 'eleccion' in c or 'proceso' in c or 'campaña' in c), None),
            }

            # Validar obligatorios básicos
            if not col_map['nombre_aportante'] or not col_map['nombre_receptor'] or not col_map['monto_aporte']:
                logger.error(f"Faltan columnas clave. Identificadas: {col_map}")
                return pd.DataFrame()

            # Estructurar resultado estándar
            df_norm = pd.DataFrame()
            df_norm['rut_aportante'] = df[col_map['rut_aportante']] if col_map['rut_aportante'] else "DESCONOCIDO"
            df_norm['nombre_aportante'] = df[col_map['nombre_aportante']]
            df_norm['rut_receptor'] = df[col_map['rut_receptor']] if col_map['rut_receptor'] else "DESCONOCIDO"
            df_norm['nombre_receptor'] = df[col_map['nombre_receptor']]
            df_norm['tipo_receptor'] = "NO DEFINIDO"  # Default
            df_norm['monto_aporte'] = pd.to_numeric(df[col_map['monto_aporte']].astype(str).str.replace(r'[\$,\.]', '', regex=True), errors='coerce').fillna(0)
            df_norm['fecha_aporte'] = pd.to_datetime(df[col_map['fecha_aporte']], format="mixed", errors='coerce') if col_map['fecha_aporte'] else pd.to_datetime('today')
            df_norm['eleccion_campaña'] = df[col_map['eleccion_campaña']] if col_map['eleccion_campaña'] else "HISTÓRICO"

            # Limpiar donaciones ilegítimas (vacías o 0)
            df_norm = df_norm[df_norm['monto_aporte'] > 0]
            
            self._save_to_db(df_norm)
            return df_norm

        except Exception as e:
            logger.error(f"Error parseando SERVEL aportes: {e}")
            return pd.DataFrame()

    def _save_to_db(self, df: pd.DataFrame) -> None:
        """Guarda los datos procesados en SQLite mitigando duplicados."""
        if df.empty:
            return
            
        conn = sqlite3.connect(self.db_path)
        guardados = 0
        duplicados = 0

        # Iterrows es lento pero es seguro para manejar UNIQUE constraint
        for _, row in df.iterrows():
            try:
                conn.execute(
                    """
                    INSERT INTO aportes_servel
                    (rut_aportante, nombre_aportante, rut_receptor, nombre_receptor, tipo_receptor, monto_aporte, fecha_aporte, eleccion_campaña)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(row['rut_aportante']).strip().upper() if pd.notna(row['rut_aportante']) else '?',
                        str(row['nombre_aportante']).strip().upper() if pd.notna(row['nombre_aportante']) else '?',
                        str(row['rut_receptor']).strip().upper() if pd.notna(row['rut_receptor']) else '?',
                        str(row['nombre_receptor']).strip().upper() if pd.notna(row['nombre_receptor']) else '?',
                        str(row['tipo_receptor']).strip().upper() if pd.notna(row['tipo_receptor']) else 'NO DEFINIDO',
                        float(row['monto_aporte']),
                        str(row['fecha_aporte']) if pd.notna(row['fecha_aporte']) else None,
                        str(row['eleccion_campaña']).strip().upper() if pd.notna(row['eleccion_campaña']) else 'GENERAL'
                    )
                )
                guardados += 1
            except sqlite3.IntegrityError:
                duplicados += 1
                pass

        conn.commit()
        conn.close()
        logger.info(f"Migrados a la DB Elector: {guardados} nuevos, {duplicados} repetidos.")
