"""
Servicio del asistente IA (DeepSeek) con inteligencia forense.

Pipeline:
  1. Clasifica la intención del usuario (persona, proveedor, organismo, anomalía, general)
  2. Ejecuta herramientas forenses en paralelo según la intención
  3. Inyecta el contexto enriquecido al LLM
  4. Retorna respuesta con evidencia citada
"""

import logging
import os
import re
import sqlite3
import time
from datetime import datetime

import requests
from dotenv import load_dotenv

from config import API_OC_URL, DB_NAME, REQUEST_TIMEOUT

logger = logging.getLogger(__name__)

load_dotenv()

DB_PATH = DB_NAME

# ──────────────────────────────────────────────────────────────────────────
# TTL CACHE — evita recalcular queries forenses pesadas en cada click.
# Key: (function_name, *args). Value: (timestamp, result).
# ──────────────────────────────────────────────────────────────────────────
_CACHE: dict[tuple, tuple[float, object]] = {}
_CACHE_TTL_SECONDS = 600  # 10 min


def _cached(ttl: int = _CACHE_TTL_SECONDS):
    """Decorador de cache TTL sencillo (thread-unsafe pero suficiente para Streamlit)."""
    def deco(fn):
        def wrapper(*args, **kwargs):
            key = (fn.__name__, args, tuple(sorted(kwargs.items())))
            now = time.time()
            hit = _CACHE.get(key)
            if hit is not None and (now - hit[0]) < ttl:
                return hit[1]
            result = fn(*args, **kwargs)
            _CACHE[key] = (now, result)
            return result
        wrapper.__name__ = fn.__name__
        return wrapper
    return deco


@_cached()
def _cached_ranking_proveedores(top_n: int = 10):
    from cross_referencer import CrossReferencer
    return CrossReferencer(DB_PATH).ranking_proveedores_sospechosos(top_n=top_n)


@_cached()
def _cached_ranking_organismos():
    from cross_referencer import CrossReferencer
    return CrossReferencer(DB_PATH).ranking_riesgo_organismos()


@_cached()
def _cached_ratio_td():
    from cross_referencer import CrossReferencer
    return CrossReferencer(DB_PATH).ratio_tratos_directos()


@_cached()
def _cached_cruce_servel():
    from cross_referencer import CrossReferencer
    return CrossReferencer(DB_PATH).cruce_servel_compras()


@_cached()
def _cached_reporte_ejecutivo():
    from cross_referencer import CrossReferencer
    return CrossReferencer(DB_PATH).reporte_ejecutivo()

_STOPWORDS = frozenset({
    "que", "para", "con", "los", "las", "del", "por", "una", "como", "este",
    "esta", "son", "hay", "puede", "tiene", "todos", "todas", "sobre", "donde",
    "cual", "cuando", "entre", "pero", "sin", "mas", "sus", "ese", "esa",
    "esos", "esas", "fue", "ser", "han", "era", "hoy", "dia", "ver", "quiero",
    "investigar", "investiga", "buscar", "arma", "expediente", "dime", "quien",
    "quienes", "cuales", "caso", "crear", "procede", "claro", "proveedor",
    "rut", "organismo", "empresa", "persona", "cgr", "abusa", "anomalias",
    "anomalías", "vinculos", "vínculos", "politicos", "políticos",
    "fiscalizaciones", "aportes", "electorales", "trato", "directo", "sospechosos",
})

# ──────────────────────────────────────────────────────────────────────────
# INTENT CLASSIFIER
# ──────────────────────────────────────────────────────────────────────────
_RUT_PATTERN = re.compile(r"\d{1,2}\.?\d{3}\.?\d{3}-[\dkK]")

_INTENT_KEYWORDS = {
    "persona": [
        "persona", "político", "politico", "diputado", "senador", "alcalde",
        "funcionario", "ministro", "servel", "lobby", "probidad", "declaración",
    ],
    "proveedor": [
        "proveedor", "empresa", "rut", "sociedad", "fundación", "fundacion",
        "ong", "corporación", "corporacion", "contratista", "adjudicatario",
    ],
    "organismo": [
        "organismo", "ministerio", "servicio", "municipalidad", "gore",
        "hospital", "universidad", "comprador", "institución", "institucion",
    ],
    "anomalia": [
        "anomalía", "anomalia", "sospechoso", "fraude", "fraccionamiento",
        "vampiro", "fantasma", "sobreprecio", "irregularidad", "riesgo",
        "alerta", "concentración", "concentracion", "trato directo",
    ],
    "resumen": [
        "resumen", "dashboard", "general", "estadísticas", "estadisticas",
        "reporte", "ejecutivo", "panorama", "estado",
    ],
}


def classify_intent(prompt: str) -> list[str]:
    """Clasifica la intención del usuario. Retorna lista de intenciones."""
    lower = prompt.lower()
    intents = []
    if _RUT_PATTERN.search(prompt):
        intents.append("proveedor")
    for intent, keywords in _INTENT_KEYWORDS.items():
        if any(kw in lower for kw in keywords):
            intents.append(intent)
    return intents or ["general"]


# ──────────────────────────────────────────────────────────────────────────
# FORENSIC TOOLS — cada una retorna (label, context_str)
# ──────────────────────────────────────────────────────────────────────────

def _tool_person_search(prompt: str) -> tuple[str, str]:
    """Busca persona en las 7 fuentes oficiales vía AlertasPersonas."""
    try:
        from alertas_personas import AlertasPersonas
        ap = AlertasPersonas(DB_PATH)
        resultados = ap.buscar(prompt)
        if not resultados:
            return "Búsqueda de Persona", f"Sin resultados en 7 fuentes oficiales para: {prompt}"
        lines = [f"### BÚSQUEDA DE PERSONA — {len(resultados)} hallazgos ###"]
        for r in resultados[:20]:
            lines.append(
                f"- [{r.get('tipo', '?')}] {r.get('fecha', '?')} | "
                f"{r.get('descripcion', 'N/A')} | Fuente: {r.get('fuente', '?')}"
            )
        return "Búsqueda de Persona (7 fuentes)", "\n".join(lines)
    except Exception as exc:
        logger.warning("Error en búsqueda de persona: %s", exc)
        return "Búsqueda de Persona", f"[Error: {exc}]"


def _tool_anomaly_scan(prompt: str) -> tuple[str, str]:
    """Ejecuta detector de anomalías y cruza con personas."""
    try:
        lines = ["### ANÁLISIS FORENSE DE ANOMALÍAS ###"]

        # Top proveedores sospechosos (cacheado 10 min)
        df_susp = _cached_ranking_proveedores(top_n=10)
        if not df_susp.empty:
            lines.append("\n**TOP 10 PROVEEDORES SOSPECHOSOS (Score Compuesto):**")
            for _, row in df_susp.head(10).iterrows():
                lines.append(
                    f"- {row.get('nombre_proveedor', '?')} (RUT: {row.get('rut_proveedor', '?')}) | "
                    f"Score: {row.get('score_sospecha', 0):.1f} | "
                    f"Monto: ${row.get('monto_total', 0):,.0f} CLP"
                )

        # Organismos de mayor riesgo (cacheado)
        df_org = _cached_ranking_organismos()
        if not df_org.empty:
            lines.append("\n**TOP 5 ORGANISMOS DE MAYOR RIESGO:**")
            for _, row in df_org.head(5).iterrows():
                lines.append(
                    f"- {row.get('nombre_comprador', '?')} | "
                    f"Score Riesgo: {row.get('score_riesgo', 0):.1f} | "
                    f"OC: {row.get('n_ordenes', 0)} | "
                    f"Monto: ${row.get('monto_total', 0):,.0f} CLP"
                )

        # Abuso de trato directo (cacheado)
        df_td = _cached_ratio_td()
        if not df_td.empty:
            top_td = df_td[df_td["ratio_td"] > 80].head(5)
            if not top_td.empty:
                lines.append("\n**ORGANISMOS CON >80% TRATO DIRECTO:**")
                for _, row in top_td.iterrows():
                    lines.append(
                        f"- {row.get('nombre_comprador', '?')} | "
                        f"TD: {row.get('ratio_td', 0):.0f}% | "
                        f"N={row.get('n_trato_directo', 0)}/{row.get('n_total', 0)}"
                    )

        return "Scanner Forense", "\n".join(lines)
    except Exception as exc:
        logger.warning("Error en anomaly scan: %s", exc)
        return "Scanner Forense", f"[Error: {exc}]"


def _tool_cross_servel(prompt: str) -> tuple[str, str]:
    """Cruza aportes SERVEL vs. adjudicaciones."""
    try:
        df = _cached_cruce_servel()
        if df.empty:
            return "Cruce SERVEL", "Sin datos SERVEL cargados o sin coincidencias detectadas."
        lines = ["### CRUCE SERVEL — Aportes electorales vs. Adjudicaciones ###"]
        for _, row in df.head(10).iterrows():
            lines.append(
                f"- Aportante: {row.get('nombre_aportante', '?')} → "
                f"Partido/Candidato: {row.get('politico_o_partido', '?')} | "
                f"Inversión electoral: ${row.get('inversion_electoral', 0):,.0f} | "
                f"Retorno por licitaciones: ${row.get('retorno_licitaciones', 0):,.0f}"
            )
        return "Cruce SERVEL", "\n".join(lines)
    except Exception as exc:
        logger.warning("Error en cruce SERVEL: %s", exc)
        return "Cruce SERVEL", f"[Error: {exc}]"


def _tool_executive_report() -> tuple[str, str]:
    """Genera reporte ejecutivo de la base de datos."""
    try:
        report = _cached_reporte_ejecutivo()
        if not report:
            return "Reporte Ejecutivo", "Base de datos vacía."
        lines = ["### REPORTE EJECUTIVO ###"]
        for key, val in report.items():
            if isinstance(val, float):
                lines.append(f"- {key}: ${val:,.0f}" if val > 1000 else f"- {key}: {val:.2f}")
            else:
                lines.append(f"- {key}: {val}")
        return "Reporte Ejecutivo", "\n".join(lines)
    except Exception as exc:
        logger.warning("Error en reporte ejecutivo: %s", exc)
        return "Reporte Ejecutivo", f"[Error: {exc}]"


def _tool_fiscalizaciones_cgr(prompt: str) -> tuple[str, str]:
    """Busca organismos bajo fiscalización de la Contraloría."""
    try:
        import pandas as pd
        from contraloria_connector import ContraloriaConnector
        cgr = ContraloriaConnector(DB_PATH)
        df = cgr.cruzar_compradores_fiscalizados()
        if df.empty:
            return "Contraloría CGR", "Sin datos de fiscalizaciones cargados."
        # Filtrar por keywords si hay
        keywords = _extract_keywords(prompt)
        if keywords:
            mask = pd.Series(False, index=df.index)
            for kw in keywords:
                for col in ["nombre_comprador", "entidad_fiscalizada"]:
                    if col in df.columns:
                        mask |= df[col].str.lower().str.contains(kw, na=False)
            if mask.any():
                df = df[mask]
        lines = [f"### FISCALIZACIONES CGR — {len(df)} coincidencias ###"]
        for _, row in df.head(10).iterrows():
            lines.append(
                f"- {row.get('nombre_comprador', '?')} | "
                f"Fiscalizada: {row.get('entidad_fiscalizada', '?')} | "
                f"Gasto: ${row.get('gasto_total', 0):,.0f} CLP"
            )
        return "Contraloría CGR", "\n".join(lines)
    except ImportError:
        return "Contraloría CGR", "[Conector no disponible]"
    except Exception as exc:
        logger.warning("Error en CGR: %s", exc)
        return "Contraloría CGR", f"[Error: {exc}]"


def _tool_infoprobidad(prompt: str) -> tuple[str, str]:
    """Busca conflictos de interés en InfoProbidad."""
    try:
        from infoprobidad_connector import InfoProbidadConnector
        ip = InfoProbidadConnector(DB_PATH)
        df_susp = _cached_ranking_proveedores(top_n=5)
        if df_susp.empty:
            return "InfoProbidad", "Sin proveedores para cruzar."
        nombres = df_susp["nombre_proveedor"].tolist()
        df_conflictos = ip.cruzar_intereses_proveedores(nombres)
        if df_conflictos.empty:
            return "InfoProbidad", "Sin conflictos de interés detectados en top proveedores."
        lines = [f"### INFOPROBIDAD — {len(df_conflictos)} conflictos potenciales ###"]
        for _, row in df_conflictos.head(10).iterrows():
            lines.append(
                f"- Funcionario: {row.get('nombre_funcionario', '?')} | "
                f"Cargo: {row.get('cargo', '?')} | "
                f"Proveedor: {row.get('proveedor_match', '?')}"
            )
        return "InfoProbidad", "\n".join(lines)
    except ImportError:
        return "InfoProbidad", "[Conector no disponible]"
    except Exception as exc:
        logger.warning("Error en InfoProbidad: %s", exc)
        return "InfoProbidad", f"[Error: {exc}]"


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
    
    # 1. Prioridad: Buscar por RUT exacto si viene en el prompt
    m_rut = re.search(r"RUT\s+([\d\.\-Kk]+)", prompt, re.IGNORECASE)
    rut_detected = m_rut.group(1).replace(".", "").strip() if m_rut else None

    palabras = _extract_keywords(prompt)
    if not palabras and not rut_detected:
        return db_context

    try:
        with sqlite3.connect(DB_PATH) as conn:
            # Proveedores
            if rut_detected:
                rows = conn.execute(
                    """
                    SELECT codigo_oc, nombre_proveedor, monto_total_item, nombre_comprador,
                           tipo_oc, categoria, fecha_creacion, rut_proveedor
                    FROM ordenes_items
                    WHERE rut_proveedor = ?
                    ORDER BY monto_total_item DESC
                    LIMIT 20
                    """,
                    (rut_detected,)
                ).fetchall()
            else:
                params = [f"%{p}%" for p in palabras[:5]]
                conditions = " AND ".join(["nombre_proveedor LIKE ?" for _ in params])
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
    """Busca contexto web vía DuckDuckGo. Timeout estricto para no bloquear el UI."""
    # Permite desactivarla totalmente via env var (Streamlit Cloud a veces bloquea DDG)
    if os.getenv("DISABLE_WEB_SEARCH", "").lower() in ("1", "true", "yes"):
        return "[Búsqueda web desactivada por configuración.]"
    try:
        from duckduckgo_search import DDGS

        with DDGS(timeout=8) as ddgs:
            query_osint = f"{prompt} chile contraloria OR corrupcion OR fundaciones OR santiago"
            resultados = list(ddgs.text(query_osint, region="cl-es", safesearch="off", max_results=5))
            parts = []
            for r in resultados:
                parts.append(f"TITULO: {r.get('title')}\nTEXTO: {r.get('body')}\n")
            return "\n".join(parts)
    except Exception as exc:
        logger.warning("Error en búsqueda web: %s", exc)
        return "[No se pudo acceder a búsqueda web. Usando solo memoria interna.]"


# ──────────────────────────────────────────────────────────────────────────
# ORCHESTRATOR — selecciona y ejecuta herramientas según intención
# ──────────────────────────────────────────────────────────────────────────

# Mapa de intención → herramientas a ejecutar
_INTENT_TOOL_MAP: dict[str, list] = {
    "persona":  [_tool_person_search, _tool_cross_servel],
    "proveedor": [_tool_anomaly_scan, _tool_fiscalizaciones_cgr],
    "organismo": [_tool_anomaly_scan, _tool_fiscalizaciones_cgr, _tool_infoprobidad],
    "anomalia":  [_tool_anomaly_scan, _tool_cross_servel, _tool_infoprobidad],
    "resumen":   [_tool_executive_report],
    "general":   [_tool_executive_report],
}


def build_forensic_context(prompt: str) -> tuple[str, list[str]]:
    """
    Orquesta la ejecución de herramientas forenses según la intención.
    Retorna (contexto_forense, herramientas_usadas).
    """
    intents = classify_intent(prompt)
    tools_to_run = []
    seen = set()
    for intent in intents:
        for tool_fn in _INTENT_TOOL_MAP.get(intent, []):
            fn_name = tool_fn.__name__
            if fn_name not in seen:
                seen.add(fn_name)
                tools_to_run.append(tool_fn)

    if not tools_to_run:
        tools_to_run = [_tool_executive_report]

    results = []
    tools_used = []
    for tool_fn in tools_to_run:
        try:
            # _tool_executive_report no toma prompt
            if tool_fn is _tool_executive_report:
                label, ctx = tool_fn()
            else:
                label, ctx = tool_fn(prompt)
            tools_used.append(label)
            results.append(f"\n{ctx}")
        except Exception as exc:
            logger.warning("Error en herramienta %s: %s", tool_fn.__name__, exc)

    context = "\n".join(results)
    return context, tools_used


def build_system_prompt(web_context: str, db_context: str,
                        forensic_context: str = "") -> str:
    """Construye el system prompt para DeepSeek con inteligencia forense."""
    fecha_actual = datetime.now().strftime("%Y-%m-%d")
    return (
        "Eres el 'Cerebro Forense' de la plataforma anticorrupción 'Ojo del Pueblo'. "
        "Tu misión: analizar datos financieros y políticos con rigor forense. "
        "Tono: directo, profesional, basado en evidencia (analista OSINT senior).\n"
        f"Fecha de hoy: {fecha_actual}.\n"
        "\n══════════════════════════════════════════\n"
        "██ INTELIGENCIA FORENSE (Herramientas Automáticas) ██\n"
        "══════════════════════════════════════════\n"
        f"{forensic_context}\n"
        "\n══════════════════════════════════════════\n"
        "██ BASE DE DATOS LOCAL (Órdenes de Compra) ██\n"
        "══════════════════════════════════════════\n"
        f"{db_context}\n"
        "\n══════════════════════════════════════════\n"
        "██ CONTEXTO WEB EN TIEMPO REAL ██\n"
        "══════════════════════════════════════════\n"
        f"{web_context}\n"
        "\n##############################################################\n"
        "DIRECTRICES (OBLIGATORIO):\n"
        "1. Habla como un analista entregando un expediente clasificado. Cero frases genéricas.\n"
        "2. Si la INTELIGENCIA FORENSE contiene datos de las 7 fuentes oficiales (SERVEL, InfoLobby, "
        "Contraloría, InfoProbidad, Mercado Público), ÚSALOS como evidencia primaria.\n"
        "3. Estructura la respuesta así:\n"
        "   - **🔎 PERFIL DE INTERÉS:** (Quién es, RUT, cargo, vínculos)\n"
        "   - **💰 HISTORIAL FINANCIERO:** (Contratos, montos, fechas, tipo de compra, concentración)\n"
        "   - **🚨 ALERTAS Y ANOMALÍAS:** (Score de riesgo, patrones sospechosos, cruces SERVEL, conflictos de interés)\n"
        "   - **📋 RECOMENDACIÓN DE INVESTIGACIÓN:** (Qué profundizar, qué fuentes consultar)\n"
        "4. Cita datos exactos: códigos OC, RUTs, montos, fechas, scores de riesgo.\n"
        "5. Si NO encuentras datos del objetivo, dilo y sugiere búsquedas alternativas.\n"
        "6. [HERRAMIENTA AUTÓNOMA — INFILTRACIÓN] Si detectas un RUT (ej. '76.111.222-3'), "
        "puedes ordenar descargar su historial completo añadiendo al final de tu respuesta: "
        "`[EJECUTAR_INFILTRACION: 76.111.222-3]`\n"
    )


def call_deepseek(messages: list[dict], web_context: str, db_context: str,
                   forensic_context: str = "") -> str:
    """Envía la consulta a DeepSeek con reintentos. Retorna la respuesta o mensaje de error."""
    api_key = os.getenv("DEEPSEEK_API_KEY", "")
    if not api_key:
        return ("Error: No se encontró DEEPSEEK_API_KEY en el archivo .env. "
                "Configura tu clave para activar el asistente.")

    system_prompt = build_system_prompt(web_context, db_context, forensic_context)

    payload = {
        "model": "deepseek-chat",
        "messages": [{"role": "system", "content": system_prompt}]
        + [{"role": m["role"], "content": m["content"]} for m in messages[-8:]],
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
            return ("El servidor de IA no responde (timeout tras 3 intentos). "
                    "DeepSeek puede estar saturado. Intenta de nuevo en unos minutos.")
        except Exception as e:
            return f"Error de conexión: {str(e)}"

    return "No se pudo obtener respuesta del servidor de IA tras 3 intentos."
