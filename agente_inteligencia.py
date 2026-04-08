"""
Agente de Inteligencia - V2.5
Módulo Dual: Scraping del Diario Oficial y Motor de Visión Artificial (OCR).
"""
import sqlite3
import re
import logging
import fitz  # PyMuPDF

logger = logging.getLogger(__name__)

class AgenteInteligencia:
    def __init__(self, db_path="auditoria_estado.db"):
        self.db_path = db_path

    def _iniciar_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''CREATE TABLE IF NOT EXISTS sociedades (rut_empresa TEXT PRIMARY KEY, razon_social TEXT, capital REAL)''')
            conn.execute('''CREATE TABLE IF NOT EXISTS socios_empresa (id INTEGER PRIMARY KEY AUTOINCREMENT, rut_empresa TEXT, rut_socio TEXT, nombre_socio TEXT, porcentaje REAL, UNIQUE(rut_empresa, rut_socio))''')
            conn.execute('''CREATE TABLE IF NOT EXISTS resoluciones_exentas (id INTEGER PRIMARY KEY AUTOINCREMENT, rut_municipalidad TEXT, empresa_adjudicada TEXT, rut_empresa TEXT, monto_oculto REAL, justificacion TEXT, filepath TEXT)''')
            conn.commit()

    # ──────────────────────────────────────────────────────────────────
    # SPIDER: DIARIO OFICIAL (Extracción de Constituciones Societarias)
    # ──────────────────────────────────────────────────────────────────
    def rastrear_diario_oficial_hoy(self):
        """
        Conecta a los nodos de publicación chilenos para extraer las empresas
        creadas en los últimos 7 días y descifrar quiénes son los dueños reales.

        Estado: EXPERIMENTAL - requiere implementar scraper real del Diario Oficial.
        """
        self._iniciar_db()
        # TODO: Implementar scraper real de https://www.diariooficial.interior.gob.cl/publicaciones/
        logger.info("rastrear_diario_oficial_hoy: Sin implementación de scraper real aún.")
        return 0

    # ──────────────────────────────────────────────────────────────────
    # VISIÓN ARTIFICIAL (OCR): LECTOR DE RESOLUCIONES Y DECRETOS EN PDF
    # ──────────────────────────────────────────────────────────────────
    def extraer_datos_pdf_ocr(self, filepath: str) -> dict:
        """
        Usa PyMuPDF para leer resoluciones municipales escaneadas (que evaden
        el mercado público normal) para descubrir Tratos Directos por "Emergencia".
        """
        self._iniciar_db()
        texto_completo = ""
        try:
            with fitz.open(filepath) as doc:
                for page in doc:
                    texto_completo += page.get_text()

            # Inteligencia Artificial Clásica: Expresiones Regulares sobre Texto PDF
            monto_match = re.search(r'[\$]?\s?(\d{1,3}(?:\.\d{3})*(?:,\d+)?)\s?(?:pesos|clp)', texto_completo, re.IGNORECASE)
            rut_empresa_match = re.search(r'(\d{7,8}-[\dkK])', texto_completo)

            # Buscar el motivo del Trato Directo (emergencia, proveedor único, etc)
            motivo_emergencia = "Emergencia" if "emergencia" in texto_completo.lower() else "Desconocido"

            monto_detectado = 0
            if monto_match:
                monto_detectado = float(monto_match.group(1).replace(".", "").replace(",", "."))

            rut_detectado = rut_empresa_match.group(1) if rut_empresa_match else "No encontrado"

            resultado = {
                "rut_empresa_detectado": rut_detectado,
                "monto_oculto": monto_detectado,
                "motivo_trato_directo": motivo_emergencia,
                "texto_analizado": texto_completo[:500] + "..." # Sample
            }

            # Guardamos en la matrix
            if rut_detectado != "No encontrado" and monto_detectado > 0:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute("INSERT INTO resoluciones_exentas (rut_municipalidad, empresa_adjudicada, rut_empresa, monto_oculto, justificacion, filepath) VALUES (?, ?, ?, ?, ?, ?)",
                                 ("DEFAULT_MUNI", "EXTRAIDO_POR_OCR", rut_detectado, monto_detectado, motivo_emergencia, filepath))
                    conn.commit()

            return resultado

        except Exception as e:
            return {"error": str(e)}

if __name__ == "__main__":
    # Test Standalone
    agente = AgenteInteligencia()
    n = agente.rastrear_diario_oficial_hoy()
    print(f"Scrapeados y cruzados {n} boletines gigantes del Diario Oficial")
