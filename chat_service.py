"""
Servicio del asistente IA (DeepSeek) con inteligencia local.
Separado de dashboard.py para aislar la lógica de IA del UI.
"""

import logging
import os
import re
import sqlite3
from datetime import datetime

import requests
from dotenv import load_dotenv

from config import API_OC_URL, DB_NAME, REQUEST_TIMEOUT

logger = logging.getLogger(__name__)

load_dotenv()

DB_PATH = DB_NAME

_STOPWORDS = frozenset({
    "que", "para", "con", "los", "las", "del", "por", "una", "como", "este",
    "esta", "son", "hay", "puede", "tiene", "todos", "todas", "sobre", "donde",
    "cual", "cuando", "entre", "pero", "sin", "mas", "sus", "ese", "esa",
    "esos", "esas", "fue", "ser", "han", "era", "hoy", "dia", "ver", "quiero",
    "investigar", "investiga", "buscar", "arma", "expediente", "dime", "quien",
    "quienes", "cuales", "caso", "crear", "procede", "claro",
})


def _extract_keywords(text: str) -> list[str]:
    """Extrae palabras clave (3+ chars, sin stopwords) del texto. Máximo 10."""
    return [
        w
        for w in re.findall(r"[a-záéíóúñü]+", text.lower())
        if len(w) >= 3 and w not in _STOPWORDS
    ][:10]


def build_db_context(prompt: str) -> str:
    """Busca en la DB local y en la API de Mercado Público para armar contexto."""
    db_context = ""
    palabras = _extract_keywords(prompt)
    if not palabras:
        return db_context

    try:
        with sqlite3.connect(DB_PATH) as conn:
            # Proveedores
            params = [f"%{p}%" for p in palabras[:5]]
            conditions = " OR ".join(["nombre_proveedor LIKE ?" for _ in params])
            rows = conn.execute(
                f"""
                SELECT codigo_oc, nombre_proveedor, monto_total_item, nombre_comprador,
                       tipo_oc, categoria, fecha_creacion, rut_proveedor
                FROM ordenes_items
                WHERE {conditions}
                ORDER BY monto_total_item DESC
                LIMIT 10
                """,
                params,
            ).fetchall()

            if rows:
                db_context += f"\n### DATOS ENCONTRADOS EN BASE DE DATOS LOCAL ({len(rows)} resultados):\n"
                for r in rows:
                    db_context += (
                        f"- OC: {r[0]} | Proveedor: {r[1]} | Monto: ${r[2]:,.0f} CLP "
                        f"| Comprador: {r[3]} | Tipo: {r[4]} | Cat: {r[5]} | Fecha: {r[6]}\n"
                    )

                # Auto-buscar RUT en API para el proveedor top
                top_oc = rows[0][0]
                mp_ticket = os.getenv("MERCADO_PUBLICO_TICKET", "")
                if mp_ticket:
                    try:
                        api_url = f"{API_OC_URL}?codigo={top_oc}&ticket={mp_ticket}"
                        api_resp = requests.get(api_url, timeout=REQUEST_TIMEOUT)
                        if api_resp.status_code == 200:
                            listado = api_resp.json().get("Listado", [])
                            if listado:
                                oc_detail = listado[0]
                                prov = oc_detail.get("Proveedor", {})
                                fechas = oc_detail.get("Fechas", {})
                                db_context += f"\n### DETALLE API MERCADO PÚBLICO (OC {top_oc}):\n"
                                db_context += f"- Nombre completo: {prov.get('NombreContacto', 'N/A')}\n"
                                db_context += f"- RUT Sucursal/Empresa: {prov.get('RutSucursal', 'NO DISPONIBLE')}\n"
                                db_context += f"- Sucursal: {prov.get('NombreSucursal', 'N/A')}\n"
                                db_context += f"- Actividad: {prov.get('Actividad', 'N/A')}\n"
                                db_context += (
                                    f"- Dirección: {prov.get('Direccion', 'N/A')}, "
                                    f"{prov.get('Comuna', '')}, {prov.get('Region', '')}\n"
                                )
                                db_context += f"- Cargo contacto: {prov.get('CargoContacto', 'N/A')}\n"
                                db_context += f"- Fecha creación OC: {fechas.get('FechaCreacion', 'N/A')}\n"
                                db_context += f"- Fecha aceptación: {fechas.get('FechaAceptacion', 'N/A')}\n"
                                db_context += f"- Estado: {oc_detail.get('Estado', 'N/A')}\n"
                                db_context += f"- Monto total OC: ${oc_detail.get('Total', 0):,.0f} CLP\n"
                    except (requests.RequestException, KeyError, ValueError) as exc:
                        logger.warning("Error consultando API Mercado Público para OC %s: %s", top_oc, exc)

            # Compradores/organismos
            params_comp = [f"%{p}%" for p in palabras[:5]]
            conditions_comp = " OR ".join(["nombre_comprador LIKE ?" for _ in params_comp])
            rows_comp = conn.execute(
                f"""
                SELECT nombre_comprador, COUNT(*) as n, SUM(monto_total_item) as total
                FROM ordenes_items
                WHERE {conditions_comp}
                GROUP BY nombre_comprador
                ORDER BY total DESC
                LIMIT 5
                """,
                params_comp,
            ).fetchall()
            if rows_comp and rows_comp[0][1] > 0:
                db_context += "\n### ORGANISMOS COMPRADORES RELACIONADOS:\n"
                for r in rows_comp:
                    db_context += f"- {r[0]}: {r[1]} OCs, ${r[2]:,.0f} CLP total\n"
    except (sqlite3.Error, OSError) as exc:
        logger.error("Error consultando DB local: %s", exc)
        db_context = ""

    return db_context


def build_web_context(prompt: str) -> str:
    """Busca contexto web vía DuckDuckGo."""
    try:
        from duckduckgo_search import DDGS

        with DDGS() as ddgs:
            query_osint = f"{prompt} chile contraloria OR corrupcion OR fundaciones OR santiago"
            resultados = list(ddgs.text(query_osint, region="cl-es", safesearch="off", max_results=5))
            parts = []
            for r in resultados:
                parts.append(f"TITULO: {r.get('title')}\nTEXTO: {r.get('body')}\n")
            return "\n".join(parts)
    except Exception as exc:
        logger.warning("Error en búsqueda web: %s", exc)
        return "[No se pudo acceder a búsqueda web. Usando solo memoria interna.]"


def build_system_prompt(web_context: str, db_context: str) -> str:
    """Construye el system prompt para DeepSeek."""
    fecha_actual = datetime.now().strftime("%Y-%m-%d")
    return (
        "Eres el 'Cerebro Forense' de una plataforma anticorrupción llamada 'Ojo del Pueblo'. "
        "Tu objetivo es analizar datos financieros y políticos con rigor. Tu tono es directo, profesional y basado en evidencia (estilo analista OSINT). "
        f"MUY IMPORTANTE: Hoy es {fecha_actual} (Año 2026). Tu base de memoria llega solo hasta 2024, "
        "por lo que dependes de la Inyección Histórica y del Contexto Web de internet para hablar del presente. "
        "Si no sabes algo con certeza, dilo.\n"
        "\n\n### Contexto Histórico Inyectado (2026): ###\n"
        "- El actual Presidente de la República de Chile es José Antonio Kast.\n"
        "- El Caso Convenios / Fundaciones es el mayor escándalo reciente de desvío de fondos públicos vía subvenciones (Democracia Viva, En Ti, etc).\n"
        "\n### Contexto Web Extraído de Internet AHORA MISMO: ###\n"
        f"{web_context}\n"
        f"{db_context}\n"
        "##############################################################\n"
        "DIRECTRICES DE FORMATEO (OBLIGATORIO):\n"
        "1. Nunca hables como un robot amable, habla como un analista entregando un expediente clasificado.\n"
        "2. Estructura tu respuesta en este formato Markdown exacto:\n"
        "   - **PERFIL DE INTERÉS:** (Resumen de quién es, incluye RUT si está disponible en los datos)\n"
        "   - **HISTORIAL FINANCIERO:** (Qué contratos tiene, montos, fechas, tipo de compra)\n"
        "   - **ALERTAS DE VÍNCULOS:** (Nexos sospechosos, patrones anómalos)\n"
        "3. IMPORTANTE: Si tienes datos de la BASE DE DATOS LOCAL o de la API MERCADO PÚBLICO en tu contexto, ÚSALOS como fuente primaria. Esos datos son verificados y reales. Cita los códigos OC, RUTs, montos exactos y fechas.\n"
        "4. Si NO encuentras datos del objetivo en el contexto inyectado, dilo claramente y sugiere buscar con otro nombre o código OC.\n"
        "5. [HERRAMIENTA AUTONOMA] Si ya leiste el RUT en el contexto web o el usuario te lo dio (ej. '76.111.222-3'), puedes infiltrarte y descargar su historial de compras AHORA MISMO a tu memoria, añadiendo EXACTAMENTE la cadena secreta al final de tu respuesta: `[EJECUTAR_INFILTRACION: 76.111.222-3]`. Al usarla el panel interceptará tu orden, se auto-reiniciará y en la siguiente vuelta ya sabrás todo."
    )


def call_deepseek(messages: list[dict], web_context: str, db_context: str) -> str:
    """Envía la consulta a DeepSeek con reintentos. Retorna la respuesta o un mensaje de error."""
    api_key = os.getenv("DEEPSEEK_API_KEY", "")
    if not api_key:
        return "Error: No se encontró DEEPSEEK_API_KEY en el archivo .env. Configura tu clave para activar el asistente."

    system_prompt = build_system_prompt(web_context, db_context)

    payload = {
        "model": "deepseek-chat",
        "messages": [{"role": "system", "content": system_prompt}]
        + [{"role": m["role"], "content": m["content"]} for m in messages[-5:]],
        "temperature": 0.4,
    }

    import time as _time

    for intento in range(3):
        try:
            if intento > 0:
                _time.sleep(2)
            response = requests.post(
                "https://api.deepseek.com/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=60,
            )
            if response.status_code == 200:
                return response.json()["choices"][0]["message"]["content"]
            elif response.status_code == 429:
                continue
            else:
                return f"Error de API (código {response.status_code}). Intenta de nuevo."
        except requests.exceptions.Timeout:
            if intento < 2:
                continue
            return "El servidor de IA no responde (timeout tras 3 intentos). DeepSeek puede estar saturado. Intenta de nuevo en unos minutos."
        except Exception as e:
            return f"Error de conexión: {str(e)}"

    return "No se pudo obtener respuesta del servidor de IA tras 3 intentos."
