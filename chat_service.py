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

from config import API_BUSCAR_PROVEEDOR_URL, API_OC_URL, DB_NAME, REQUEST_TIMEOUT

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

        # Abuso de trato directo (cacheado) — filtrado por significancia estadistica
        df_td = _cached_ratio_td()
        if not df_td.empty:
            # Solo organismos con volumen significativo (>=10 OC) para evitar ruido 1/1
            df_td_sig = df_td[df_td["n_total"] >= 10].copy()

            # Top 10 GLOBAL por ratio_td (organismos relevantes)
            top_td_global = df_td_sig.sort_values("ratio_td", ascending=False).head(10)
            if not top_td_global.empty:
                lines.append("\n**TOP 10 ORGANISMOS CON MAYOR RATIO DE TRATO DIRECTO (N>=10 OC):**")
                for _, row in top_td_global.iterrows():
                    monto_td = row.get('monto_td', 0) or 0
                    lines.append(
                        f"- {row.get('nombre_comprador', '?')} (RUT {row.get('rut_comprador', '?')}) | "
                        f"TD: {row.get('ratio_td', 0):.0f}% ({row.get('n_trato_directo', 0)}/{row.get('n_total', 0)} OC) | "
                        f"Monto TD: ${monto_td:,.0f} CLP ({row.get('pct_monto_td', 0):.0f}% del total)"
                    )

            # Top 10 MUNICIPALIDADES por ratio_td (filtro por nombre)
            mask_muni = df_td_sig["nombre_comprador"].astype(str).str.lower().str.contains(
                r"municipal|i\. muni|ilustre muni", regex=True, na=False
            )
            top_td_muni = df_td_sig[mask_muni].sort_values("ratio_td", ascending=False).head(10)
            if not top_td_muni.empty:
                lines.append("\n**TOP 10 MUNICIPALIDADES POR RATIO DE TRATO DIRECTO (N>=10 OC):**")
                for _, row in top_td_muni.iterrows():
                    monto_td = row.get('monto_td', 0) or 0
                    lines.append(
                        f"- {row.get('nombre_comprador', '?')} (RUT {row.get('rut_comprador', '?')}) | "
                        f"TD: {row.get('ratio_td', 0):.0f}% ({row.get('n_trato_directo', 0)}/{row.get('n_total', 0)} OC) | "
                        f"Monto TD: ${monto_td:,.0f} CLP"
                    )
            else:
                lines.append("\n**MUNICIPALIDADES:** Sin municipalidades con >=10 OC en la BD actual (la cobertura puede ser parcial — la BD prioriza grandes compradores).")

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


def _format_rut_with_dots(rut: str) -> str:
    """Convierte '76111222-3' -> '76.111.222-3' (formato esperado por API MP).

    Si el RUT ya viene con puntos, lo devuelve normalizado.
    """
    clean = rut.replace(".", "").replace(" ", "").strip()
    if "-" not in clean:
        return rut  # no podemos formatear sin DV
    body, dv = clean.rsplit("-", 1)
    # invertir, agrupar de a 3, reinvertir
    body_rev = body[::-1]
    chunks = [body_rev[i:i + 3] for i in range(0, len(body_rev), 3)]
    body_fmt = ".".join(chunks)[::-1]
    return f"{body_fmt}-{dv.upper()}"


def build_db_context(prompt: str) -> str:
    """Busca en la DB local y en la API de Mercado Público para armar contexto.

    Mercado Público es la fuente PRIMARIA y se consulta primero:
      1. Si hay RUT -> BuscarProveedor (autoritativo, datos de la empresa)
      2. Si hay match en BD -> detalle de la OC top via API
    Después se complementa con BD local (organismos, totales, etc.).
    """
    db_context = ""

    # 1. Prioridad: Buscar por RUT exacto si viene en el prompt
    m_rut = re.search(r"RUT\s+([\d\.\-Kk]+)", prompt, re.IGNORECASE)
    rut_detected = m_rut.group(1).replace(".", "").strip() if m_rut else None

    # Si no hay 'RUT' literal pero hay un patrón 7-9 dígitos+DV, también lo capturamos
    if not rut_detected:
        m_rut2 = re.search(r"\b(\d{1,2}\.?\d{3}\.?\d{3}-[\dKk])\b", prompt)
        if m_rut2:
            rut_detected = m_rut2.group(1).replace(".", "").strip()

    palabras = _extract_keywords(prompt)
    if not palabras and not rut_detected:
        return db_context

    # ── PASO 0: Consulta directa al endpoint oficial BuscarProveedor si hay RUT ──
    mp_ticket = os.getenv("MERCADO_PUBLICO_TICKET", "")
    if rut_detected and mp_ticket:
        try:
            # El endpoint espera RUT con puntos y guion: 76.111.222-3
            rut_fmt = _format_rut_with_dots(rut_detected)
            r = requests.get(
                API_BUSCAR_PROVEEDOR_URL,
                params={"rutempresaproveedor": rut_fmt, "ticket": mp_ticket},
                timeout=REQUEST_TIMEOUT,
            )
            if r.status_code == 200:
                data = r.json()
                listado = data.get("listaEmpresas") or data.get("Listado") or []
                if listado:
                    emp = listado[0] if isinstance(listado, list) else listado
                    db_context += (
                        "\n### MERCADO PUBLICO - FUENTE PRIMARIA OFICIAL (BuscarProveedor):\n"
                        f"- RUT consultado: {rut_fmt}\n"
                        f"- Nombre empresa: {emp.get('NombreEmpresa', 'N/A')}\n"
                        f"- Codigo interno MP: {emp.get('CodigoEmpresa', 'N/A')}\n"
                        f"- Estado: Registrado en plataforma oficial de compras del Estado.\n"
                    )
        except (requests.RequestException, ValueError, KeyError) as exc:
            logger.warning("Error en API BuscarProveedor para %s: %s", rut_detected, exc)

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
                # Busqueda por nombre: estrategia escalonada.
                # 1) Todas las palabras AND (estricto)  2) Top-2 keywords AND
                # 3) Keyword mas distintiva sola. Se queda con lo primero que devuelva algo.
                # Esto evita fallar cuando el usuario agrega contexto (ej. "antofagasta")
                # que no esta literal en el nombre del proveedor.
                rows = []
                # Generic / low-signal terms (industry verticals, paises, palabras vacias)
                _GENERIC = {
                    "empresa", "empresas", "compania", "compañia", "sociedad",
                    "chile", "ltda", "spa", "limitada", "sa", "eirl",
                    "transportes", "transporte", "comercial", "servicios",
                    "constructora", "constructor", "consultora", "consultores",
                    "inversiones", "inmobiliaria", "ingenieria", "ingenieros",
                    "distribuidora", "distribuidor", "importadora", "importaciones",
                    "exportadora", "comercio",
                }
                # Ciudades / regiones chilenas (alta frecuencia, baja distintividad)
                _GEO = {
                    "antofagasta", "santiago", "valparaiso", "valparaíso",
                    "concepcion", "concepción", "temuco", "iquique", "arica",
                    "rancagua", "talca", "chillan", "chillán", "punta", "arenas",
                    "puerto", "montt", "calama", "copiapo", "copiapó",
                    "coquimbo", "serena", "ovalle", "linares", "curico", "curicó",
                    "osorno", "valdivia", "talcahuano", "viña", "vina", "del", "mar",
                    "region", "región", "ciudad", "comuna", "norte", "sur",
                }
                # Score: largo + bonus si parece nombre propio (no esta en listas).
                # Penaliza palabras genericas o geograficas comunes.
                def _score(w: str) -> tuple[int, int]:
                    wl = w.lower()
                    if wl in _GENERIC:
                        return (-2, -len(w))  # casi descartado
                    if wl in _GEO:
                        return (-1, -len(w))  # solo si no hay nada mejor
                    return (1, len(w))  # priorizada

                keywords_sorted = sorted(set(palabras), key=_score, reverse=True)
                kws_useful = [k for k in keywords_sorted if _score(k)[0] >= 1]
                strategies: list[list[str]] = []
                if kws_useful:
                    if len(kws_useful) >= 2:
                        strategies.append(kws_useful[:3])      # 2-3 distintivas AND
                    strategies.append([kws_useful[0]])         # la mas distintiva sola
                # Fallback: si no hay nada distintivo, intenta con todas las palabras
                if not strategies and palabras:
                    strategies.append(palabras[:3])

                seen_strategies: set[tuple[str, ...]] = set()
                for strat in strategies:
                    key = tuple(strat)
                    if key in seen_strategies or not strat:
                        continue
                    seen_strategies.add(key)
                    params = [f"%{p}%" for p in strat]
                    cond_prov = " AND ".join(["nombre_proveedor LIKE ?" for _ in params])
                    rows = conn.execute(
                        f"""
                        SELECT codigo_oc, nombre_proveedor, monto_total_item, nombre_comprador,
                               tipo_oc, categoria, fecha_creacion, rut_proveedor
                        FROM ordenes_items
                        WHERE {cond_prov}
                        ORDER BY monto_total_item DESC
                        LIMIT 10
                        """,
                        params,
                    ).fetchall()
                    if rows:
                        break

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
    """Busca contexto web vía DuckDuckGo en MÚLTIPLES fuentes OSINT chilenas.

    Fuentes consultadas:
      - Dateas.com: publicaciones legales, avisos judiciales, diario oficial
      - Cooperativa.cl: avisos legales (pérdida de cheques, quiebras, etc.)
      - TodoLicitaciones / LicitaPyme / ChilePyme: licitaciones y proveedores
      - Fuentes periodísticas: CIPER, interferencia.cl, BioBioChile
      - Contraloria / corrupción / fundaciones: contexto general
    """
    # Permite desactivarla totalmente via env var (Streamlit Cloud a veces bloquea DDG)
    if os.getenv("DISABLE_WEB_SEARCH", "").lower() in ("1", "true", "yes"):
        return "[Búsqueda web desactivada por configuración.]"

    keywords = _extract_keywords(prompt)
    # Rebuild a clean search term from the original prompt (not stopword-filtered)
    # but cap it to avoid DDG rejecting overly long queries
    search_term = " ".join(prompt.split()[:12])

    # Define targeted search queries for each OSINT source cluster
    _OSINT_QUERIES = [
        # 0. Busqueda generica amplia (descubrimiento de RUT, web, redes)
        {
            "query": f"{search_term} chile rut",
            "label": "Busqueda general (descubrimiento de RUT y presencia web)",
            "max_results": 6,
        },
        # 0.1 Portal oficial de proveedores (Mercado Publico)
        {
            "query": f"{search_term} site:mercadopublico.cl OR site:chilecompra.cl",
            "label": "Mercado Publico / ChileCompra (proveedor oficial)",
            "max_results": 4,
        },
        # 0.2 Directorios de empresas chilenas (suelen traer RUT + giro + direccion)
        {
            "query": (
                f"{search_term} site:guiaempresaschile.cl OR site:rut.cl OR "
                "site:empresascl.com OR site:nuestrarut.cl OR site:datos.gob.cl"
            ),
            "label": "Directorios de empresas (RUT, giro, direccion)",
            "max_results": 5,
        },
        # 1. Fuentes judiciales / legales / Diario Oficial
        {
            "query": f"site:dateas.com {search_term} chile",
            "label": "Dateas.com (Publicaciones legales / Diario Oficial)",
            "max_results": 4,
        },
        # 2. Avisos legales en medios (pérdida de cheques, quiebras, etc.)
        {
            "query": f"{search_term} site:cooperativa.cl aviso legal OR publicacion judicial",
            "label": "Cooperativa.cl (Avisos legales)",
            "max_results": 3,
        },
        # 3. Licitaciones y proveedores
        {
            "query": f"{search_term} chile site:todolicitaciones.cl OR site:licitapyme.cl OR site:chilepyme.cl",
            "label": "Portales de Licitaciones (TodoLicitaciones / LicitaPyme / ChilePyme)",
            "max_results": 3,
        },
        # 4. Contexto periodístico / corrupción / contraloría
        {
            "query": f"{search_term} chile corrupcion OR contraloria OR fundaciones OR licitacion OR fraude",
            "label": "Fuentes periodisticas y judiciales",
            "max_results": 5,
        },
        # 5. Prensa regional (cubre denuncias locales que no llegan a medios nacionales)
        {
            "query": (
                f"{search_term} site:soychile.cl OR site:elnortero.cl OR "
                "site:laprensaaustral.cl OR site:elmostrador.cl OR site:diarioantofagasta.cl"
            ),
            "label": "Prensa regional chilena",
            "max_results": 4,
        },
    ]

    all_parts: list[str] = []
    sources_summary: list[str] = []  # status linea por fuente (para forzar transparencia)

    # ── Fallback: Bing HTML scrape para cuando DDG retorna 0 ──
    def _bing_fallback(query: str, max_results: int) -> list[dict[str, str]]:
        try:
            import requests as _req
            from urllib.parse import quote_plus
            url = f"https://www.bing.com/search?q={quote_plus(query)}&setlang=es&cc=cl"
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
                ),
                "Accept-Language": "es-CL,es;q=0.9,en;q=0.8",
            }
            r = _req.get(url, headers=headers, timeout=8)
            if r.status_code != 200:
                return []
            # Parser regex super liviano: titulo + url + snippet por bloque <li class="b_algo">
            import re as _re
            results = []
            blocks = _re.findall(
                r'<li class="b_algo"[^>]*>(.*?)</li>',
                r.text, _re.DOTALL,
            )
            for b in blocks[:max_results]:
                m_h = _re.search(r'<h2[^>]*>.*?<a [^>]*href="([^"]+)"[^>]*>(.*?)</a>', b, _re.DOTALL)
                m_s = _re.search(r'<p[^>]*>(.*?)</p>', b, _re.DOTALL)
                if not m_h:
                    continue
                href = m_h.group(1)
                title = _re.sub(r"<[^>]+>", "", m_h.group(2)).strip()
                body = _re.sub(r"<[^>]+>", "", m_s.group(1)).strip() if m_s else ""
                results.append({"href": href, "title": title, "body": body})
            return results
        except Exception as exc:  # noqa: BLE001
            logger.debug("Bing fallback fallo: %s", exc)
            return []

    # Estado del provider principal (para reportar tambien si DDG cayo entero)
    ddgs_ok = True
    try:
        from duckduckgo_search import DDGS
        _ddgs_ctx = DDGS(timeout=10)
    except Exception as exc:  # noqa: BLE001
        logger.warning("DDGS no disponible: %s", exc)
        ddgs_ok = False
        _ddgs_ctx = None

    try:
        if ddgs_ok and _ddgs_ctx is not None:
            ddgs = _ddgs_ctx.__enter__()
        else:
            ddgs = None

        for source in _OSINT_QUERIES:
            label = source["label"]
            results: list[dict[str, str]] = []
            origin = "—"
            err: str | None = None

            # Intento 1: DuckDuckGo
            if ddgs is not None:
                try:
                    results = list(ddgs.text(
                        source["query"],
                        region="cl-es",
                        safesearch="off",
                        max_results=source["max_results"],
                    ))
                    if results:
                        origin = "DDG"
                except Exception as exc:  # noqa: BLE001
                    err = f"DDG:{type(exc).__name__}"
                    logger.debug("DDG fail [%s]: %s", label, exc)

            # Intento 2: Bing scrape si DDG dio 0
            if not results:
                bing_results = _bing_fallback(source["query"], source["max_results"])
                if bing_results:
                    results = bing_results
                    origin = "Bing-fallback"

            # Loguea status en el summary (siempre, incluso si vacio)
            if results:
                sources_summary.append(f"[OK] {label} — {len(results)} hits via {origin}")
                all_parts.append(f"\n### {label} ###")
                for r in results:
                    title = r.get("title", "")
                    body = r.get("body", "")
                    href = r.get("href", "")
                    all_parts.append(
                        f"TITULO: {title}\nTEXTO: {body}\nURL: {href}\n"
                    )
            else:
                tag = err or "0 hits"
                sources_summary.append(f"[VACIO] {label} — {tag} (query: {source['query'][:80]})")
    finally:
        if ddgs_ok and _ddgs_ctx is not None:
            try:
                _ddgs_ctx.__exit__(None, None, None)
            except Exception:  # noqa: BLE001
                pass

    # Header con resumen de fuentes consultadas — fuerza a la IA a citar todo
    header_lines = [
        "### RESUMEN DE BUSQUEDAS WEB EJECUTADAS (TRANSPARENCIA OBLIGATORIA) ###",
        f"Termino base: '{search_term}'",
        f"Fuentes consultadas: {len(_OSINT_QUERIES)} clusters OSINT",
    ]
    header_lines.extend(sources_summary)
    if not all_parts:
        header_lines.append(
            "[ADVERTENCIA] Ninguna fuente retorno datos. NO concluyas 'no existe'; "
            "indica que la web estuvo limitada y propon busqueda manual por el usuario."
        )

    return "\n".join(header_lines) + ("\n" + "\n".join(all_parts) if all_parts else "")


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
        "Eres el 'Cerebro Forense' de la plataforma anticorrupcion 'Ojo del Pueblo'. "
        "Tu mision: analizar datos financieros y politicos con rigor forense. "
        "Tono: directo, profesional, basado en evidencia (analista OSINT senior).\n"
        f"Fecha de hoy: {fecha_actual}.\n"
        "\n======================================\n"
        "== INTELIGENCIA FORENSE (Herramientas Automaticas) ==\n"
        "======================================\n"
        f"{forensic_context}\n"
        "\n======================================\n"
        "== MERCADO PUBLICO + BD LOCAL (FUENTE PRIMARIA OFICIAL) ==\n"
        "======================================\n"
        "Esta seccion contiene datos cruzados de la API oficial de Mercado Publico "
        "(api.mercadopublico.cl) y la base de datos local con 54.000+ ordenes de "
        "compra extraidas de esa misma API. Es la EVIDENCIA PRIMARIA del expediente.\n"
        f"{db_context}\n"
        "\n======================================\n"
        "== CONTEXTO WEB OSINT (Fuentes complementarias en tiempo real) ==\n"
        "======================================\n"
        "Fuentes web consultadas (complementarias, NUNCA reemplazan a Mercado Publico): "
        "Dateas.com (publicaciones legales, Diario Oficial), "
        "Cooperativa.cl (avisos legales, perdida de cheques, quiebras), "
        "TodoLicitaciones / LicitaPyme / ChilePyme (licitaciones publicas), "
        "CIPER / BioBioChile / interferencia.cl (periodismo investigativo), "
        "ademas de las otras 6 fuentes oficiales (SERVEL, InfoLobby, Contraloria, "
        "InfoProbidad, datos.gob.cl, CGR SICA).\n\n"
        f"{web_context}\n"
        "\n##############################################################\n"
        "DIRECTRICES (OBLIGATORIO):\n"
        "1. Habla como un analista entregando un expediente clasificado. Cero frases genericas.\n"
        "2. FUENTE PRIMARIA = MERCADO PUBLICO + BD LOCAL. Empieza SIEMPRE el expediente "
        "citando datos exactos de Mercado Publico (codigos OC, RUTs, montos en CLP, "
        "fechas, organismos compradores). El resto de las fuentes son COMPLEMENTARIAS.\n"
        "3. Usa las fuentes web OSINT (Dateas, Cooperativa avisos legales, portales de "
        "licitaciones) como EVIDENCIA SECUNDARIA. Si encuentras un aviso legal "
        "(perdida de cheques, quiebra, constitucion de sociedad), CITALA con URL "
        "pero subordinada al historial de Mercado Publico.\n"
        "4. Estructura la respuesta asi:\n"
        "   - **PERFIL DE INTERES:** (Quien es, RUT, cargo, vinculos)\n"
        "   - **HISTORIAL EN MERCADO PUBLICO:** (Contratos, montos, fechas, tipo de compra, "
        "concentracion por organismo) - usa datos de la seccion FUENTE PRIMARIA\n"
        "   - **ALERTAS Y ANOMALIAS:** (Score de riesgo, patrones sospechosos, cruces SERVEL, conflictos de interes)\n"
        "   - **REGISTROS LEGALES/JUDICIALES:** (Publicaciones en Diario Oficial, avisos legales, "
        "perdida de documentos, quiebras, constituciones de sociedades — de Dateas y Cooperativa)\n"
        "   - **PRESENCIA EN LICITACIONES:** (Apariciones en TodoLicitaciones, LicitaPyme, ChilePyme)\n"
        "   - **RECOMENDACION DE INVESTIGACION:** (Que profundizar, que fuentes consultar)\n"
        "5. Cita datos exactos: codigos OC, RUTs, montos, fechas, scores de riesgo, URLs.\n"
        "6. Si NO encuentras datos del objetivo en Mercado Publico, dilo explicitamente "
        "y sugiere busquedas alternativas. La ausencia tambien es informacion.\n"
        "6.1. [RESOLUCION DE RUT — OBLIGATORIO] Si el usuario te da el nombre completo "
        "(o suficientes datos: nombre + cargo, nombre + organismo, nombre + region) "
        "y NO te entrega el RUT, tu PRIMERA tarea es DEDUCIRLO o ENCONTRARLO. "
        "Eres un modelo de razonamiento profundo: USALO. No respondas 'no tengo el RUT, "
        "porfa damelo' - eso es PEREZA y esta PROHIBIDO. En su lugar:\n"
        "   a) Revisa el CONTEXTO WEB OSINT mas arriba: Dateas, Diario Oficial, "
        "Cooperativa, portales de licitaciones suelen publicar el RUT junto al nombre.\n"
        "   b) Revisa el contexto de BD LOCAL: si la persona aparece como representante "
        "legal, socio o director de una empresa contratista, ese RUT esta ahi.\n"
        "   c) Cruza con SERVEL (aportes de campania): nombres completos de candidatos "
        "y aportantes vienen con RUT.\n"
        "   d) Cruza con InfoProbidad / Contraloria: declaraciones de patrimonio de "
        "funcionarios publicos publican nombre completo + RUT + cargo.\n"
        "   e) Si despues de revisar TODAS las fuentes anteriores sigue sin aparecer, "
        "indica explicitamente: 'RUT no identificado en las fuentes consultadas. "
        "Candidatos posibles a verificar: [lista de RUTs similares o de homonimos "
        "encontrados]'. NUNCA inventes un RUT, pero SI propon hipotesis basadas en "
        "coincidencias parciales del contexto (mismo organismo, misma region, mismo cargo).\n"
        "   f) Una vez que tengas o propongas un RUT, ejecuta automaticamente la "
        "infiltracion con `[EJECUTAR_INFILTRACION: <rut>]` para traer su historial.\n"
        "6.2. [BUSQUEDA DE EMPRESA POR NOMBRE — OBLIGATORIO] Si el usuario te da "
        "el nombre de una empresa SIN RUT (ej. 'Transportes Mathias de Antofagasta'), "
        "NO digas 'no la encuentro'. Despliega el siguiente protocolo:\n"
        "   a) Mira los DIRECTORIOS DE EMPRESAS del contexto web (guiaempresaschile, "
        "rut.cl, empresascl, nuestrarut, datos.gob.cl) — alli aparece RUT + giro + "
        "direccion.\n"
        "   b) Mira la seccion MERCADO PUBLICO / CHILECOMPRA del contexto web — si "
        "la empresa vende al Estado, su RUT esta publicado ahi.\n"
        "   c) Mira los DATOS DE BD LOCAL: ya se hicieron busquedas escalonadas por "
        "nombre (todas las palabras, top-2, palabra mas distintiva). Si hay match "
        "parcial, repotalo aunque la coincidencia no sea exacta.\n"
        "   d) Mira la PRENSA REGIONAL: en una empresa de Antofagasta, El Nortero o "
        "Diario Antofagasta suelen tener historicos.\n"
        "   e) Considera VARIANTES ortograficas del nombre (Mathias / Matias / "
        "Mathiass / Matias S.A. / Transportes M. ...) y razona sobre cual es la "
        "version oficial registrada en SII / Mercado Publico.\n"
        "   f) Aun si NO encuentras nada concreto, entrega un INFORME DE BUSQUEDA: "
        "que fuentes revisaste, que variantes intentaste, y que pistas hay (region, "
        "rubro probable, tamano probable, nombre similar de otra empresa del rubro). "
        "Decirle al usuario 'busca mas datos' SIN haberte exprimido tu razonamiento "
        "esta PROHIBIDO.\n"
        "   g) Si llegas a un RUT candidato, dispara `[EJECUTAR_INFILTRACION: <rut>]`.\n"
        "7. [HERRAMIENTA AUTONOMA — INFILTRACION] Si detectas un RUT (ej. '76.111.222-3'), "
        "puedes ordenar descargar su historial completo anadiendo al final de tu respuesta: "
        "`[EJECUTAR_INFILTRACION: 76.111.222-3]`\n"
        "8. [FOOTER OBLIGATORIO — TRANSPARENCIA DE FUENTES] Al final de TODA respuesta "
        "que implique investigar persona/empresa/organismo, agrega un bloque "
        "'### Fuentes Consultadas ###' donde enumeres CADA cluster OSINT del "
        "'RESUMEN DE BUSQUEDAS WEB EJECUTADAS' (te lo entrego arriba) marcando:\n"
        "   - ✅ si trajo datos utiles que citaste,\n"
        "   - ⚠️  si trajo datos pero no eran relevantes,\n"
        "   - ❌ si retorno 0 (indica si fue por bloqueo, query mala, o ausencia real).\n"
        "Esto NO es opcional: el usuario debe saber EXACTAMENTE que se reviso. Si te "
        "saltas el footer, la respuesta se considera INCOMPLETA. Ejemplo:\n"
        "   ### Fuentes Consultadas ###\n"
        "   - ✅ Busqueda general (3 hits, 1 RUT identificado)\n"
        "   - ❌ Mercado Publico / ChileCompra (0 hits — empresa probablemente no vende al Estado)\n"
        "   - ✅ Directorios de empresas (rut.cl: RUT 76.xxx.xxx-x)\n"
        "   - ❌ Dateas.com (0 hits)\n"
        "   - etc.\n"
        "9. [NO TE ENGANCHES EN 'VENDE AL ESTADO'] Una empresa puede existir y ser "
        "investigable aunque NO licite con el Estado. Si Mercado Publico no la tiene, "
        "NO concluyas 'no existe' — busca en directorios (rut.cl, guiaempresaschile), "
        "prensa regional, redes. Mercado Publico es solo UNA de varias fuentes.\n"
        "10. [PROHIBIDO PEDIR PERMISO — INVESTIGA PRIMERO, PREGUNTA DESPUES] NUNCA "
        "termines tu respuesta con frases tipo 'indiqueme si desea que proceda', "
        "'desea que profundice', 'puedo ejecutar si me autoriza'. ESTAS PROHIBIDAS. "
        "Tu rol es FORENSE PROACTIVO: si crees que falta data, ejecutas la herramienta "
        "ANTES de responder. Si la herramienta no puede ejecutarse desde aqui, "
        "explicalo y entrega TU MEJOR ANALISIS con la data que SI tienes — pero NO "
        "pidas permiso. Despues de tu analisis completo, opcionalmente puedes "
        "sugerir 'siguiente linea de investigacion: X', pero como propuesta concreta, "
        "no como peticion de permiso.\n"
        "11. [PROHIBIDO PLACEHOLDERS EN MARCADORES] El marcador "
        "`[EJECUTAR_INFILTRACION: ...]` SOLO acepta un RUT real con formato "
        "`12345678-9` o `76.111.222-3`. NUNCA escribas cosas como "
        "`[EJECUTAR_INFILTRACION: lista de municipalidades]` o `[EJECUTAR_INFILTRACION: <rut>]` "
        "literal — el sistema lo ignora. Si no tienes un RUT concreto, NO emitas el "
        "marcador; en su lugar lista los RUTs candidatos y pidele al usuario que "
        "clickee el que quiera infiltrar.\n"
        "12. [QUERIES MASIVAS — USA LA DATA QUE TE DI] Si el usuario pregunta 'top 5 "
        "municipalidades con peor ratio trato directo', YA tienes ese top en "
        "'ORGANISMOS CON >80% TRATO DIRECTO' o 'TOP 10 MUNICIPALIDADES POR RATIO DE "
        "TRATO DIRECTO' del contexto BD. NO digas 'no aparecen municipalidades' si la "
        "lista MUNI esta vacia — di EXACTAMENTE: 'En los datos disponibles "
        "(N>=10 OC) no figuran municipalidades en el top, lo cual sugiere que la "
        "cobertura de la BD prioriza otros tipos de organismo'. Y luego entrega el "
        "TOP GLOBAL real que SI tienes.\n"
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
        "model": "deepseek-v4-pro",
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
                timeout=90,
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

def generate_case_summary(messages: list[dict]) -> str:
    """Genera un dossier formal resumido de toda la conversacion IA.

    Filtra mensajes del sistema, manda toda la conversacion a DeepSeek con
    un system prompt especifico que pide un EXPEDIENTE FORENSE estructurado
    en markdown listo para imprimirse como PDF.

    Retorna texto markdown con secciones formales. Si no hay API key o falla,
    devuelve un fallback de texto plano armado localmente.
    """
    api_key = os.getenv("DEEPSEEK_API_KEY", "")

    # Filtrar mensajes utiles (sin system, sin mensajes-sistema internos)
    convo = [m for m in messages if m.get("role") in ("user", "assistant")
             and not (m.get("role") == "assistant" and m.get("content", "").startswith("**Cerebro Forense activado"))]

    if not convo:
        return "# Expediente vacio\n\nNo hay conversacion para resumir."

    if not api_key:
        # Fallback: concatenar bruto sin IA
        out = ["# EXPEDIENTE FORENSE - OJO DEL PUEBLO",
               f"_Generado el {datetime.now().strftime('%d/%m/%Y %H:%M')}_",
               "",
               "## Conversacion completa", ""]
        for m in convo:
            who = "INVESTIGADOR" if m["role"] == "user" else "CEREBRO FORENSE"
            out.append(f"### {who}")
            out.append(m["content"])
            out.append("")
        return "\n".join(out)

    fecha = datetime.now().strftime("%d de %B de %Y, %H:%M")
    system = (
        "Eres el redactor jefe del Cerebro Forense de 'Ojo del Pueblo'. "
        "Recibiras una conversacion completa entre un investigador y la IA forense. "
        "Tu tarea: producir un EXPEDIENTE FORENSE FORMAL en markdown listo para imprimir como PDF "
        "de prueba periodistica. Audiencia: periodistas, fiscales, ciudadanos.\n\n"
        "ESTRUCTURA OBLIGATORIA (usar exactamente estos headers):\n"
        "# EXPEDIENTE FORENSE - OJO DEL PUEBLO\n"
        f"_Generado el {fecha}_\n\n"
        "## 1. RESUMEN EJECUTIVO\n"
        "(3-5 lineas: que se investigo, que se encontro, nivel de gravedad)\n\n"
        "## 2. OBJETIVO DE LA INVESTIGACION\n"
        "(que pregunta inicial planteo el investigador)\n\n"
        "## 3. HALLAZGOS CLAVE\n"
        "(bullets con cifras exactas: RUTs, codigos OC, montos en CLP, fechas. "
        "Cada bullet termina con la fuente citada)\n\n"
        "## 4. CRUCES Y ANOMALIAS DETECTADAS\n"
        "(patrones sospechosos, aportes SERVEL, conflictos de interes, abuso TD)\n\n"
        "## 5. EVIDENCIA DE MERCADO PUBLICO\n"
        "(datos exactos de la API oficial: codigos OC, organismos, fechas, montos)\n\n"
        "## 6. FUENTES COMPLEMENTARIAS\n"
        "(URLs concretas de Dateas, Cooperativa, TodoLicitaciones u otros)\n\n"
        "## 7. RECOMENDACION DE PROFUNDIZACION\n"
        "(que linea seguir investigando, que documentos pedir por Transparencia)\n\n"
        "## 8. APENDICE - TRANSCRIPCION RESUMIDA\n"
        "(resume cada turno del chat en 2-3 lineas, no transcribas literal)\n\n"
        "REGLAS:\n"
        "- Cita SIEMPRE cifras exactas tal como aparecieron en la conversacion. NO inventes datos.\n"
        "- Si un dato no esta en la conversacion, escribe '(no disponible en la consulta actual)'.\n"
        "- Tono: profesional, sobrio, basado en evidencia. Sin opinionologia.\n"
        "- NO uses emojis. NO uses cajas unicode. Markdown plano.\n"
        "- Largo objetivo: 600-1200 palabras.\n"
    )

    user_payload = "CONVERSACION A RESUMIR:\n\n"
    for m in convo:
        who = "INVESTIGADOR" if m["role"] == "user" else "IA"
        user_payload += f"--- {who} ---\n{m['content']}\n\n"
    user_payload += "Genera ahora el EXPEDIENTE FORENSE completo."

    payload = {
        "model": "deepseek-v4-pro",
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user_payload},
        ],
        "temperature": 0.2,
    }

    try:
        response = requests.post(
            "https://api.deepseek.com/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload,
            timeout=120,
        )
        if response.status_code == 200:
            return response.json()["choices"][0]["message"]["content"]
        return f"# Error al generar expediente\n\nCodigo HTTP: {response.status_code}"
    except Exception as exc:  # noqa: BLE001
        logger.error("Error generate_case_summary: %s", exc)
        return f"# Error al generar expediente\n\n{type(exc).__name__}: {exc}"
