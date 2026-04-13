"""
Dashboard interactivo para "Ojo del Pueblo".
Construido con Streamlit.
"""

import logging
import html as html_mod
import os
import re
import sqlite3
import urllib.parse

import requests

import pandas as pd
import plotly.express as px
import streamlit as st
from datetime import datetime

from queries import (
    DB_PATH,
    load_data,
    init_feedback_db,
    save_feedback,
    format_clp,
    format_clp_full,
    load_licitaciones,
    get_rate_limit_usage,
    increment_rate_limit_usage,
)
from chat_service import (
    build_db_context, build_web_context, call_deepseek,
    classify_intent, build_forensic_context,
)
from config import DAILY_QUERY_LIMIT, OC_TIPO_TRATO_DIRECTO
from alertas_personas import AlertasPersonas

logger = logging.getLogger(__name__)

_LOGO_PATH = "logo_ojo_pueblo.png"

# ─────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN DE PÁGINA Y ESTILOS
# ─────────────────────────────────────────────────────────────────────────

_CUSTOM_CSS: str = """
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
    .stApp {
        background-color: #0B1120;
        color: #CBD5E1;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    div[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0F172A 0%, #0B1120 100%) !important;
        border-right: 1px solid rgba(51, 65, 85, 0.5);
    }
    h1, h2, h3, h4 {
        color: #F1F5F9 !important;
        font-family: 'Inter', sans-serif;
        font-weight: 700;
        letter-spacing: -0.025em;
    }
    h1 { font-size: 2rem; border-bottom: none; padding-bottom: 0; margin-bottom: 4px; }
    h3 { font-size: 1.15rem; margin-bottom: 4px; }
    h4 { font-size: 0.95rem; font-weight: 600; color: #94A3B8 !important; }
    [data-testid="stMetricValue"] {
        font-size: 1.6rem; font-weight: 800;
        color: #F8FAFC !important;
        background: none; -webkit-text-fill-color: #F8FAFC;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.7rem; color: #64748B !important;
        text-transform: uppercase; letter-spacing: 0.08em; font-weight: 500;
    }
    div[data-testid="metric-container"] {
        background: linear-gradient(135deg, rgba(15, 23, 42, 0.8) 0%, rgba(30, 41, 59, 0.4) 100%);
        border: 1px solid rgba(51, 65, 85, 0.5);
        padding: 16px 14px; border-radius: 12px;
        backdrop-filter: blur(8px);
        transition: border-color 0.2s ease;
    }
    div[data-testid="metric-container"]:hover { border-color: rgba(59, 130, 246, 0.5); }
    .stChatInput { border-color: #1E293B !important; }
    .chat-container {
        max-height: 520px; overflow-y: auto;
        padding: 20px 12px;
        border: 1px solid rgba(51, 65, 85, 0.4);
        border-radius: 16px;
        background: rgba(15, 23, 42, 0.5);
        margin-bottom: 12px; scroll-behavior: smooth;
    }
    .chat-container::-webkit-scrollbar { width: 5px; }
    .chat-container::-webkit-scrollbar-track { background: transparent; }
    .chat-container::-webkit-scrollbar-thumb { background: #334155; border-radius: 4px; }
    .chat-bubble {
        max-width: 78%; padding: 12px 16px;
        border-radius: 16px; margin-bottom: 8px;
        font-size: 0.88rem; line-height: 1.6; word-wrap: break-word;
    }
    .chat-bubble p { margin: 0 0 6px 0; }
    .chat-bubble p:last-child { margin-bottom: 0; }
    .chat-row-user { display: flex; justify-content: flex-end; }
    .chat-row-assistant { display: flex; justify-content: flex-start; }
    .bubble-user {
        background: linear-gradient(135deg, #2563EB, #1D4ED8);
        color: #E0E7FF; border-bottom-right-radius: 4px;
    }
    .bubble-assistant {
        background: rgba(30, 41, 59, 0.8);
        color: #CBD5E1; border: 1px solid rgba(51, 65, 85, 0.5);
        border-bottom-left-radius: 4px;
    }
    .chat-label {
        font-size: 0.68rem; color: #475569;
        margin-bottom: 2px; padding: 0 4px;
        font-weight: 500; text-transform: uppercase; letter-spacing: 0.04em;
    }
    .stDownloadButton > button {
        background: rgba(15, 23, 42, 0.6) !important; color: #94A3B8 !important;
        border: 1px solid rgba(51, 65, 85, 0.5) !important; font-size: 0.8rem;
        border-radius: 8px; transition: all 0.2s;
    }
    .stDownloadButton > button:hover {
        background: rgba(30, 41, 59, 0.8) !important; color: #F1F5F9 !important;
        border-color: rgba(59, 130, 246, 0.4) !important;
    }
    .btn-portal {
        display: inline-block; padding: 8px 16px; border-radius: 8px;
        background: rgba(15, 23, 42, 0.6); border: 1px solid rgba(51, 65, 85, 0.5);
        color: #94A3B8 !important; text-decoration: none; font-size: 0.82rem;
        font-weight: 500; transition: all 0.2s; margin-top: 4px;
    }
    .btn-portal:hover {
        background: rgba(30, 41, 59, 0.8); color: #F1F5F9 !important;
        border-color: rgba(59, 130, 246, 0.4);
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2px; background: transparent; padding-bottom: 0;
        border-bottom: 2px solid rgba(30, 41, 59, 0.6);
        flex-wrap: wrap; overflow-x: visible !important;
    }
    .stTabs [data-baseweb="tab-list"] button[role="tab"] { flex: 1 1 auto; min-width: 0; }
    .stTabs [data-baseweb="tab"] {
        background: transparent; border: none;
        border-bottom: 2px solid transparent;
        padding: 10px 16px; transition: all 0.2s ease; color: #64748B;
        font-weight: 600; border-radius: 0;
        font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.06em;
        white-space: nowrap;
    }
    .stTabs [data-baseweb="tab"]:hover { color: #94A3B8; }
    .stTabs [aria-selected="true"] {
        background: transparent !important; color: #3B82F6 !important;
        border-bottom: 2px solid #3B82F6 !important;
    }
    .stTabs [data-baseweb="tab-list"] > div[role="presentation"] { display: none !important; }
    [data-testid="stDataFrame"] {
        border-radius: 12px; border: 1px solid rgba(51, 65, 85, 0.4); overflow: hidden;
    }
    .stAlert { border-radius: 10px; }
    blockquote {
        background: rgba(30, 41, 59, 0.4) !important;
        border-left: 3px solid #3B82F6 !important;
        padding: 14px 18px !important; border-radius: 0 10px 10px 0;
        color: #94A3B8 !important; font-size: 0.85rem; margin-bottom: 20px;
    }
    blockquote p { margin: 0 !important; }
    .section-header {
        display: flex; align-items: center; gap: 10px;
        margin: 28px 0 12px 0;
    }
    .section-header .icon {
        width: 36px; height: 36px; border-radius: 10px;
        display: flex; align-items: center; justify-content: center;
        font-size: 1.1rem; flex-shrink: 0;
    }
    .section-header .icon.blue { background: rgba(59, 130, 246, 0.15); }
    .section-header .icon.red { background: rgba(239, 68, 68, 0.15); }
    .section-header .icon.amber { background: rgba(245, 158, 11, 0.15); }
    .section-header .icon.green { background: rgba(16, 185, 129, 0.15); }
    .section-header .icon.purple { background: rgba(139, 92, 246, 0.15); }
    .section-header h3 { margin: 0 !important; font-size: 1rem; }
    .section-header p { margin: 0; color: #64748B; font-size: 0.78rem; }
    .source-card {
        background: linear-gradient(135deg, rgba(15, 23, 42, 0.6) 0%, rgba(30, 41, 59, 0.3) 100%);
        border: 1px solid rgba(51, 65, 85, 0.4);
        border-radius: 12px; padding: 20px;
        transition: border-color 0.2s;
    }
    .source-card:hover { border-color: rgba(59, 130, 246, 0.4); }
    .source-card h4 { margin: 0 0 8px 0 !important; font-size: 0.9rem; color: #E2E8F0 !important; }
    .source-card p { margin: 0; color: #94A3B8; font-size: 0.82rem; line-height: 1.5; }
    .source-card .badge {
        display: inline-block; padding: 2px 8px; border-radius: 6px;
        font-size: 0.7rem; font-weight: 600; margin-bottom: 8px;
    }
    .source-card .badge.live { background: rgba(239, 68, 68, 0.15); color: #F87171; }
    .source-card .badge.data { background: rgba(59, 130, 246, 0.15); color: #60A5FA; }
    .source-card .badge.press { background: rgba(245, 158, 11, 0.15); color: #FBBF24; }
    .footer-legal {
        font-size: 0.78rem; color: #475569; text-align: justify;
        border: 1px solid rgba(51, 65, 85, 0.3); padding: 20px;
        border-radius: 12px; background: rgba(15, 23, 42, 0.4); line-height: 1.6;
    }
    .footer-legal strong { color: #64748B; }
    .share-btn {
        display: inline-flex; align-items: center; gap: 6px;
        padding: 8px 16px; border-radius: 8px;
        background: rgba(15, 23, 42, 0.6); border: 1px solid rgba(51, 65, 85, 0.4);
        color: #94A3B8 !important; text-decoration: none; font-size: 0.82rem;
        font-weight: 500; transition: all 0.2s; width: 100%; justify-content: center;
    }
    .share-btn:hover {
        background: rgba(30, 41, 59, 0.8); color: #F1F5F9 !important;
        border-color: rgba(59, 130, 246, 0.4); text-decoration: none;
    }
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: #1E293B; border-radius: 4px; }
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
    header[data-testid="stHeader"] { background: transparent; }
    /* ── Forensic Radar Cards ── */
    .forensic-radar {
        display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
        gap: 12px; margin: 16px 0 24px 0;
    }
    .radar-card {
        padding: 16px 18px; border-radius: 12px;
        border-left: 4px solid; position: relative; overflow: hidden;
    }
    .radar-card.critical {
        background: linear-gradient(135deg, rgba(239,68,68,0.08) 0%, rgba(15,23,42,0.6) 100%);
        border-color: #EF4444;
    }
    .radar-card.warning {
        background: linear-gradient(135deg, rgba(245,158,11,0.08) 0%, rgba(15,23,42,0.6) 100%);
        border-color: #F59E0B;
    }
    .radar-card.info {
        background: linear-gradient(135deg, rgba(59,130,246,0.08) 0%, rgba(15,23,42,0.6) 100%);
        border-color: #3B82F6;
    }
    .radar-card.clean {
        background: linear-gradient(135deg, rgba(16,185,129,0.08) 0%, rgba(15,23,42,0.6) 100%);
        border-color: #10B981;
    }
    .radar-card .radar-number {
        font-size: 2rem; font-weight: 800; line-height: 1;
        margin-bottom: 4px;
    }
    .radar-card.critical .radar-number { color: #F87171; }
    .radar-card.warning .radar-number { color: #FBBF24; }
    .radar-card.info .radar-number { color: #60A5FA; }
    .radar-card.clean .radar-number { color: #34D399; }
    .radar-card .radar-label {
        font-size: 0.72rem; color: #64748B; text-transform: uppercase;
        letter-spacing: 0.06em; font-weight: 600;
    }
    .radar-card .radar-detail {
        font-size: 0.78rem; color: #94A3B8; margin-top: 6px; line-height: 1.4;
    }
    /* ── Lead Priority Cards ── */
    .lead-card {
        background: linear-gradient(135deg, rgba(15,23,42,0.7) 0%, rgba(30,41,59,0.3) 100%);
        border: 1px solid rgba(51,65,85,0.4); border-radius: 12px;
        padding: 16px 20px; margin-bottom: 10px;
        transition: border-color 0.2s;
    }
    .lead-card:hover { border-color: rgba(239,68,68,0.5); }
    .lead-card .lead-rank {
        display: inline-block; width: 28px; height: 28px; border-radius: 8px;
        text-align: center; line-height: 28px; font-weight: 800; font-size: 0.85rem;
        margin-right: 10px; flex-shrink: 0;
    }
    .lead-card .lead-rank.r1 { background: rgba(239,68,68,0.2); color: #F87171; }
    .lead-card .lead-rank.r2 { background: rgba(245,158,11,0.2); color: #FBBF24; }
    .lead-card .lead-rank.r3 { background: rgba(59,130,246,0.2); color: #60A5FA; }
    .lead-card .lead-name {
        font-size: 0.92rem; font-weight: 700; color: #E2E8F0;
    }
    .lead-card .lead-rut {
        font-size: 0.75rem; color: #64748B; font-family: monospace;
    }
    .lead-card .lead-flags {
        display: flex; flex-wrap: wrap; gap: 4px; margin-top: 8px;
    }
    .lead-card .lead-flag {
        display: inline-block; padding: 2px 8px; border-radius: 6px;
        font-size: 0.68rem; font-weight: 600;
    }
    .lead-flag.red { background: rgba(239,68,68,0.15); color: #F87171; }
    .lead-flag.amber { background: rgba(245,158,11,0.15); color: #FBBF24; }
    .lead-flag.blue { background: rgba(59,130,246,0.15); color: #60A5FA; }
</style>
"""
st.set_page_config(
    page_title="Ojo del Pueblo",
    page_icon=_LOGO_PATH if os.path.exists(_LOGO_PATH) else "O",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Inyectar CSS profesional (Estilo Centro de Comando / OSINT)
st.markdown(_CUSTOM_CSS, unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────
# CONFIGURACIONES Y CONSTANTES
# ─────────────────────────────────────────────────────────────────────────
EMOJIS_RIESGO = {
    "MUNICIPALIDAD": "🏛️",
    "FUERZAS ARMADAS/ORDEN": "🚓",
    "ALERTA FUNDACIONES/TRATO DIRECTO": "🚨💰",
    "MOP/OBRAS": "🚧",
    "GENERAL": "📄"
}

# Paleta profesional de análisis
COLOR_DISCRETE_MAP = {
    "MUNICIPALIDAD": "#3B82F6",
    "FUERZAS ARMADAS/ORDEN": "#10B981",
    "ALERTA FUNDACIONES/TRATO DIRECTO": "#EF4444",
    "MOP/OBRAS": "#F59E0B",
    "GENERAL": "#6366F1"
}

_CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter, sans-serif", size=12, color="#CBD5E1"),
    margin=dict(l=10, r=80, t=20, b=40),
    xaxis=dict(
        gridcolor="rgba(51,65,85,0.3)", zerolinecolor="rgba(51,65,85,0.3)",
        tickfont=dict(size=11, color="#94A3B8"),
    ),
    yaxis=dict(
        gridcolor="rgba(51,65,85,0.3)", zerolinecolor="rgba(51,65,85,0.3)",
        tickfont=dict(size=11, color="#94A3B8"),
    ),
    hoverlabel=dict(
        bgcolor="#1E293B", font_size=12, font_color="#F1F5F9",
        bordercolor="#334155",
    ),
)

# ─────────────────────────────────────────────────────────────────────────
# INTERFAZ: CENTRO DE MONITOREO
# ─────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def _cached_load():
    return load_data()


def _render_empty_state():
    """Pantalla de bienvenida cuando no hay datos cargados."""
    st.markdown("""
    <div style="text-align:center; padding: 40px 20px;">
        <h2 style="color:#4FC3F7;">Bienvenido a Ojo del Pueblo</h2>
        <p style="color:#aaa; font-size:1.1em; max-width:600px; margin:auto;">
            La base de datos está vacía. Para empezar a fiscalizar, necesitas
            extraer datos <b>reales</b> desde la API de Mercado Público (ChileCompra).
        </p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### Extraer datos ahora")
        st.caption("Descarga órdenes de compra de ayer directamente desde aquí.")
        max_oc = st.selectbox("Cantidad de OC a extraer", [100, 500, 1000, 5000], index=0)
        if st.button("Extraer datos de Mercado Público", type="primary", use_container_width=True):
            _run_extraction(max_oc)

    with col2:
        st.markdown("#### Configuración necesaria")
        ticket = os.getenv("MERCADO_PUBLICO_TICKET", "")
        deepseek = os.getenv("DEEPSEEK_API_KEY", "")

        if ticket:
            st.success("MERCADO_PUBLICO_TICKET configurado")
        else:
            st.error("MERCADO_PUBLICO_TICKET no configurado")

        if deepseek:
            st.success("DEEPSEEK_API_KEY configurado")
        else:
            st.warning("DEEPSEEK_API_KEY no configurado (chat IA desactivado)")

        if not ticket:
            st.markdown("""
            **En Streamlit Cloud:**
            1. Ve a **Settings → Secrets**
            2. Agrega:
            ```toml
            MERCADO_PUBLICO_TICKET = "tu_ticket"
            DEEPSEEK_API_KEY = "tu_clave"
            ```
            3. Recarga la app
            """)


def _run_extraction(max_oc: int):
    """Ejecuta extracción ligera desde el dashboard."""
    from datetime import date, timedelta
    ticket = os.getenv("MERCADO_PUBLICO_TICKET", "")

    if not ticket:
        st.error("No se puede extraer sin MERCADO_PUBLICO_TICKET. Configúralo primero.")
        return

    try:
        from extractor import MercadoPublicoExtractor
        from processor import DataProcessor
        from detector import AnomalyDetector

        fecha = date.today() - timedelta(days=1)
        with st.status(f"Extrayendo OC del {fecha.strftime('%d/%m/%Y')}...", expanded=True) as status:
            st.write("Conectando con API de Mercado Público...")
            extractor = MercadoPublicoExtractor()
            ordenes = extractor.extract(fecha, max_oc=max_oc)

            if not ordenes:
                status.update(label="Sin datos para esa fecha", state="error")
                st.warning("La API no devolvió órdenes. Puede que el ticket haya expirado.")
                return

            st.write(f"{len(ordenes)} órdenes descargadas. Procesando...")
            processor = DataProcessor()
            df, inserted = processor.process_and_store(ordenes)

            st.write(f"{inserted} ítems almacenados. Analizando anomalías...")
            detector = AnomalyDetector()
            anomalies = detector.detect(method="serenata")
            n_anomalies = len(anomalies) if anomalies is not None else 0

            status.update(label=f"Listo: {len(ordenes)} OC, {inserted} ítems, {n_anomalies} anomalías", state="complete")

        st.success("Datos cargados exitosamente. Recargando dashboard...")
        _cached_load.clear()
        st.rerun()

    except Exception as exc:
        logger.error("Error en extracción desde dashboard: %s", exc)
        st.error(f"Error durante la extracción: {exc}")


def _investigate_buttons(entities: list[tuple[str, str]], prefix: str, entity_type: str = "proveedor"):
    """Render quick-investigate buttons that pre-fill the IA chat.

    Args:
        entities: list of (name, rut) tuples.
        prefix: unique key prefix to avoid Streamlit duplicate keys.
        entity_type: 'proveedor' or 'organismo'.
    """
    cols = st.columns(min(len(entities), 5))
    for i, (name, rut) in enumerate(entities[:5]):
        with cols[i]:
            label = name[:22] + "…" if len(name) > 22 else name
            if st.button(f"🔍 {label}", key=f"inv_{prefix}_{i}", use_container_width=True):
                if entity_type == "proveedor":
                    query = f"Investiga al proveedor {name} (RUT {rut}): ¿tiene anomalías, vínculos políticos, fiscalizaciones o aportes electorales?"
                else:
                    query = f"Investiga al organismo {name} (RUT {rut}): ¿abusa del trato directo, tiene fiscalizaciones de CGR o proveedores sospechosos?"
                st.session_state["_pending_query"] = query
                st.rerun()


def _render_tab_general(df_filtrado, total_gasto, total_oc, total_proveedores, total_compradores, pct_td, n_trato_directo):
    # Explicación clara del panel
    st.markdown("""
    > Este panel muestra **órdenes de compra** emitidas por organismos del Estado de Chile,
    > obtenidas desde la API de Mercado Público (ChileCompra). Usa los filtros laterales para explorar.
    """)

    # KPIs Superiores
    kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

    total_gasto = df_filtrado['monto_total_item'].sum() if not df_filtrado.empty else 0
    total_oc = df_filtrado['codigo_oc'].nunique() if not df_filtrado.empty else 0
    total_proveedores = df_filtrado['nombre_proveedor'].nunique() if not df_filtrado.empty else 0
    total_compradores = df_filtrado['nombre_comprador'].nunique() if not df_filtrado.empty else 0

    # Calcular % trato directo
    n_trato_directo = 0
    if not df_filtrado.empty:
        n_trato_directo = df_filtrado[df_filtrado['tipo_oc'].isin(OC_TIPO_TRATO_DIRECTO)]['codigo_oc'].nunique()
    pct_td = (n_trato_directo / total_oc * 100) if total_oc > 0 else 0

    with kpi1:
        st.metric("Gasto Escaneado", format_clp(total_gasto), help="Suma de todas las OC cargadas")
    with kpi2:
        st.metric("Ordenes de Compra", f"{total_oc:,}", help="Cantidad de OC únicas")
    with kpi3:
        st.metric("Sin Licitación", f"{pct_td:.0f}%", help=f"{n_trato_directo} OC por trato directo")
    with kpi4:
        st.metric("Proveedores", f"{total_proveedores:,}", help="Empresas/personas que venden al Estado")
    with kpi5:
        st.metric("Organismos", f"{total_compradores:,}", help="Entidades del Estado que compran")

    # ── RADAR FORENSE: Alertas automáticas ──
    if not df_filtrado.empty:
        # Calcular métricas forenses
        # 1. Proveedores multigiro (venden en 3+ categorías distintas)
        prov_cats = df_filtrado.groupby('rut_proveedor')['categoria_riesgo'].nunique()
        n_multigiro = int((prov_cats >= 3).sum())

        # 2. Proveedores que venden a 5+ organismos distintos
        prov_orgs = df_filtrado.groupby('rut_proveedor')['rut_comprador'].nunique()
        n_multi_org = int((prov_orgs >= 5).sum())

        # 3. OC de alto valor (> $100M CLP) por trato directo
        df_td_only = df_filtrado[df_filtrado['tipo_oc'].isin(OC_TIPO_TRATO_DIRECTO)]
        oc_montos = df_td_only.groupby('codigo_oc')['monto_total_item'].sum()
        n_td_alto_valor = int((oc_montos > 100_000_000).sum())

        # 4. Concentración: % del gasto en top 5 proveedores
        gasto_por_prov = df_filtrado.groupby('rut_proveedor')['monto_total_item'].sum()
        top5_gasto = gasto_por_prov.nlargest(5).sum()
        pct_top5 = (top5_gasto / total_gasto * 100) if total_gasto > 0 else 0

        # Determinar severity
        sev_td = "critical" if pct_td > 60 else ("warning" if pct_td > 40 else "info")
        sev_alto = "critical" if n_td_alto_valor > 5 else ("warning" if n_td_alto_valor > 0 else "clean")
        sev_multi = "warning" if n_multigiro > 10 else ("info" if n_multigiro > 0 else "clean")
        sev_conc = "critical" if pct_top5 > 50 else ("warning" if pct_top5 > 30 else "info")

        st.markdown(
            f'<div class="forensic-radar">'
            f'  <div class="radar-card {sev_td}">'
            f'    <div class="radar-number">{pct_td:.0f}%</div>'
            f'    <div class="radar-label">Sin Licitación Pública</div>'
            f'    <div class="radar-detail">{n_trato_directo:,} OC sin proceso competitivo</div>'
            f'  </div>'
            f'  <div class="radar-card {sev_alto}">'
            f'    <div class="radar-number">{n_td_alto_valor}</div>'
            f'    <div class="radar-label">Trato Directo &gt;$100M</div>'
            f'    <div class="radar-detail">OC de alto valor sin licitación</div>'
            f'  </div>'
            f'  <div class="radar-card {sev_multi}">'
            f'    <div class="radar-number">{n_multigiro}</div>'
            f'    <div class="radar-label">Proveedores Multigiro</div>'
            f'    <div class="radar-detail">Venden en 3+ categorías distintas</div>'
            f'  </div>'
            f'  <div class="radar-card {sev_conc}">'
            f'    <div class="radar-number">{pct_top5:.0f}%</div>'
            f'    <div class="radar-label">Concentración Top 5</div>'
            f'    <div class="radar-detail">{format_clp(top5_gasto)} en solo 5 proveedores</div>'
            f'  </div>'
            f'</div>',
            unsafe_allow_html=True,
        )

        # ── PRIORIDADES DE INVESTIGACIÓN ──
        st.markdown(
            '<div class="section-header">'
            '<div class="icon red">🎯</div>'
            '<div><h3>Prioridades de Investigación</h3>'
            '<p>Leads generados automáticamente según score de riesgo compuesto</p></div>'
            '</div>',
            unsafe_allow_html=True,
        )

        # Build priority leads from data
        leads = []
        prov_stats = df_filtrado.groupby(['rut_proveedor', 'nombre_proveedor']).agg(
            monto=('monto_total_item', 'sum'),
            n_oc=('codigo_oc', 'nunique'),
            n_organismos=('rut_comprador', 'nunique'),
            n_categorias=('categoria_riesgo', 'nunique'),
        ).reset_index()

        # Add trato directo count per provider
        td_por_prov = df_td_only.groupby('rut_proveedor')['codigo_oc'].nunique().reset_index()
        td_por_prov.columns = ['rut_proveedor', 'n_td']
        prov_stats = prov_stats.merge(td_por_prov, on='rut_proveedor', how='left')
        prov_stats['n_td'] = prov_stats['n_td'].fillna(0).astype(int)
        prov_stats['pct_td'] = (prov_stats['n_td'] / prov_stats['n_oc'] * 100).fillna(0)

        # Score: weighted sum of risk factors
        prov_stats['score'] = (
            (prov_stats['pct_td'] / 100) * 30 +
            (prov_stats['n_organismos'].clip(upper=10) / 10) * 25 +
            (prov_stats['n_categorias'].clip(upper=5) / 5) * 20 +
            (prov_stats['monto'].rank(pct=True)) * 25
        )
        top_leads = prov_stats.nlargest(3, 'score')

        rank_classes = ['r1', 'r2', 'r3']
        lead_html_parts = []
        for idx, (_, lead) in enumerate(top_leads.iterrows()):
            flags = []
            if lead['pct_td'] > 70:
                flags.append('<span class="lead-flag red">🚨 +70% Trato Directo</span>')
            elif lead['pct_td'] > 40:
                flags.append('<span class="lead-flag amber">⚠️ +40% Trato Directo</span>')
            if lead['n_organismos'] >= 5:
                flags.append('<span class="lead-flag amber">🕸️ Multi-organismo</span>')
            if lead['n_categorias'] >= 3:
                flags.append('<span class="lead-flag blue">🔄 Multigiro</span>')
            if lead['monto'] > 500_000_000:
                flags.append(f'<span class="lead-flag red">💰 {format_clp(lead["monto"])}</span>')
            else:
                flags.append(f'<span class="lead-flag blue">💰 {format_clp(lead["monto"])}</span>')

            lead_html_parts.append(
                f'<div class="lead-card">'
                f'  <div style="display:flex;align-items:center;">'
                f'    <span class="lead-rank {rank_classes[idx]}">{idx+1}</span>'
                f'    <div>'
                f'      <div class="lead-name">{html_mod.escape(str(lead["nombre_proveedor"]))}</div>'
                f'      <div class="lead-rut">{html_mod.escape(str(lead["rut_proveedor"]))} · '
                f'{lead["n_oc"]} OC · {lead["n_organismos"]} organismos</div>'
                f'    </div>'
                f'  </div>'
                f'  <div class="lead-flags">{"".join(flags)}</div>'
                f'</div>'
            )

        st.markdown("\n".join(lead_html_parts), unsafe_allow_html=True)

        # Investigate buttons for the top 3 leads
        inv_leads = list(zip(top_leads['nombre_proveedor'], top_leads['rut_proveedor']))
        _investigate_buttons(inv_leads, "lead", "proveedor")

    st.markdown("<br>", unsafe_allow_html=True)

    # Fila 1: Top proveedores + Gasto por organismo
    col_g1, col_g2 = st.columns(2)

    with col_g1:
        st.markdown("#### Top 10 proveedores por monto adjudicado")
        if not df_filtrado.empty:
            top_prov = df_filtrado.groupby('nombre_proveedor')['monto_total_item'].sum().reset_index()
            top_prov = top_prov.nlargest(10, 'monto_total_item').sort_values('monto_total_item', ascending=True)
            top_prov['monto_label'] = top_prov['monto_total_item'].apply(format_clp)
            top_prov['nombre_corto'] = top_prov['nombre_proveedor'].str[:35]
            top_prov['hover_txt'] = (
                top_prov['nombre_proveedor'] + '<br>'
                + top_prov['monto_total_item'].apply(format_clp_full)
            )

            fig_bar = px.bar(
                top_prov, x='monto_total_item', y='nombre_corto', orientation='h',
                labels={'monto_total_item': 'Total ($CLP)', 'nombre_corto': ''},
                text='monto_label', template="plotly_dark",
                color_discrete_sequence=["#3B82F6"],
                custom_data=['hover_txt'],
            )
            fig_bar.update_layout(**_CHART_LAYOUT, height=420)
            fig_bar.update_traces(
                textposition='outside', textfont_size=11, textfont_color="#CBD5E1",
                hovertemplate='%{customdata[0]}<extra></extra>',
            )
            fig_bar.update_xaxes(tickformat='~s', title_text='')
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("Sin datos suficientes.")

    with col_g2:
        st.markdown("#### Top 10 organismos por gasto")
        if not df_filtrado.empty:
            top_comp = df_filtrado.groupby('nombre_comprador')['monto_total_item'].sum().reset_index()
            top_comp = top_comp.nlargest(10, 'monto_total_item')
            top_comp['nombre_corto'] = top_comp['nombre_comprador'].str[:40]
            top_comp['hover_txt'] = (
                top_comp['nombre_comprador'] + '<br>'
                + top_comp['monto_total_item'].apply(format_clp_full)
            )

            fig_pie = px.pie(
                top_comp, values='monto_total_item', names='nombre_corto',
                hole=0.45,
                color_discrete_sequence=["#3B82F6", "#6366F1", "#8B5CF6", "#A78BFA",
                                         "#10B981", "#14B8A6", "#F59E0B", "#F97316",
                                         "#EF4444", "#EC4899"],
                custom_data=['hover_txt'],
            )
            fig_pie.update_traces(
                textposition='inside', textinfo='percent', textfont_size=11,
                hovertemplate='%{customdata[0]}<extra></extra>',
            )
            fig_pie.update_layout(
                **_CHART_LAYOUT,
                showlegend=True, height=420,
                legend=dict(
                    font=dict(size=10, color="#CBD5E1"), bgcolor="rgba(0,0,0,0)",
                    orientation="v", yanchor="middle", y=0.5,
                ),
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.info("Sin datos.")

    # Fila 2: Tipo de compra + Timeline
    col_g3, col_g4 = st.columns(2)

    with col_g3:
        st.markdown("#### Distribución por tipo de compra")
        st.caption("D1/C1 = Trato Directo · CM = Convenio Marco · AG = Compra Ágil · LP = Licitación")
        if not df_filtrado.empty:
            tipo_data = df_filtrado.groupby('tipo_oc').agg(
                n_oc=('codigo_oc', 'nunique'),
                monto=('monto_total_item', 'sum')
            ).reset_index()
            tipo_data['tipo_label'] = tipo_data['tipo_oc'].map({
                'D1': 'Trato Directo (D1)', 'C1': 'Trato Directo (C1)',
                'F3': 'Trato Directo (F3)', 'G1': 'Trato Directo (G1)',
                'FG': 'Trato Directo (FG)', 'CM': 'Convenio Marco (CM)',
                'SE': 'Sin Envío (SE)', 'AG': 'Compra Ágil (AG)',
                'MC': 'Compra Ágil (MC)', 'R1': 'Compra Ágil (R1)',
                'LP': 'Licitación (LP)', 'CO': 'Convenio (CO)',
                'RC': 'Resolución (RC)',
            }).fillna(tipo_data['tipo_oc'])
            tipo_data['monto_label'] = tipo_data['monto'].apply(format_clp)

            fig_tipo = px.bar(
                tipo_data.sort_values('monto', ascending=True),
                x='monto', y='tipo_label', orientation='h',
                text='monto_label', template="plotly_dark",
                labels={'monto': 'Monto Total', 'tipo_label': ''},
                color='monto', color_continuous_scale=['#1E293B', '#3B82F6']
            )
            fig_tipo.update_layout(
                **_CHART_LAYOUT, showlegend=False, coloraxis_showscale=False,
                height=400,
            )
            fig_tipo.update_traces(
                textposition='outside', textfont_size=11, textfont_color="#CBD5E1",
                hovertemplate='%{y}<br>%{text}<extra></extra>',
            )
            fig_tipo.update_xaxes(tickformat='~s', title_text='')
            st.plotly_chart(fig_tipo, use_container_width=True)
        else:
            st.info("Sin datos.")

    with col_g4:
        st.markdown("#### Evolución del gasto diario")
        if not df_filtrado.empty:
            df_time = df_filtrado.dropna(subset=['fecha_creacion']).copy()
            if not df_time.empty:
                gasto_diario = df_time.groupby(df_time['fecha_creacion'].dt.date)['monto_total_item'].sum().reset_index()
                gasto_diario.columns = ['fecha', 'monto']
                fig_line = px.area(
                    gasto_diario, x='fecha', y='monto',
                    labels={'fecha': 'Fecha', 'monto': 'Gasto del día ($CLP)'},
                    color_discrete_sequence=['#3B82F6']
                )
                fig_line.update_layout(**_CHART_LAYOUT, height=400)
                fig_line.update_traces(
                    line=dict(width=2.5), fillcolor="rgba(59,130,246,0.12)",
                    hovertemplate='%{x|%d/%m/%Y}<br>$%{y:,.0f} CLP<extra></extra>',
                )
                fig_line.update_yaxes(tickformat='~s', title_text='')
                fig_line.update_xaxes(tickformat='%d/%m/%y', title_text='')
                st.plotly_chart(fig_line, use_container_width=True)
            else:
                st.info("No hay fechas válidas.")
        else:
            st.info("Sin datos.")

    # Fila 3: Alertas rápidas
    st.markdown("#### Órdenes de compra de mayor monto")
    if not df_filtrado.empty:
        top5 = df_filtrado.nlargest(5, 'monto_total_item')[['codigo_oc', 'nombre_proveedor', 'rut_proveedor', 'nombre_comprador', 'monto_total_item', 'tipo_oc']].copy()
        top5['monto_total_item'] = top5['monto_total_item'].apply(format_clp)
        top5.columns = ['Código OC', 'Proveedor', 'RUT Proveedor', 'Organismo', 'Monto', 'Tipo']
        st.dataframe(top5, hide_index=True, use_container_width=True)

    # ══════════════════════════════════════════════════════════════════════════
    # PESTAÑA 2: CRUCE DE DATOS FORENSES (CrossReferencer)



def _render_tab_cruces(df_filtrado, total_proveedores, total_compradores, n_trato_directo, filtro_global):
    st.markdown(
        '<div class="section-header">'
        '<div class="icon blue">🔬</div>'
        '<div><h3>Cruces Forenses</h3>'
        '<p>Detección de patrones de riesgo mediante cruce de bases de datos públicas</p></div>'
        '</div>',
        unsafe_allow_html=True,
    )

    try:
        from cross_referencer import CrossReferencer

        xref = CrossReferencer(DB_PATH)

        # ── Reporte Ejecutivo ──
        reporte = xref.reporte_ejecutivo()
        if reporte:
            with st.expander("📊 Reporte Ejecutivo — Resumen de la Base de Datos", expanded=False):
                re1, re2, re3, re4 = st.columns(4)
                re1.metric("Total OC", f"{reporte.get('total_ordenes', 0):,}")
                re2.metric("Monto Total", format_clp_full(reporte.get('monto_total_clp', 0)))
                re3.metric("Proveedores", f"{reporte.get('total_proveedores', 0):,}")
                re4.metric("Compradores", f"{reporte.get('total_compradores', 0):,}")

                col_cat, col_tipo = st.columns(2)
                with col_cat:
                    cats = reporte.get('categorias_riesgo', {})
                    if cats:
                        st.markdown("**Distribución por categoría de riesgo:**")
                        for cat, n in sorted(cats.items(), key=lambda x: x[1], reverse=True):
                            st.write(f"- {EMOJIS_RIESGO.get(cat, '📄')} {cat}: **{n}** registros")
                with col_tipo:
                    tipos = reporte.get('tipos_oc', {})
                    if tipos:
                        st.markdown("**Tipos de orden de compra:**")
                        for tipo, n in sorted(tipos.items(), key=lambda x: x[1], reverse=True):
                            st.write(f"- {tipo or 'Sin tipo'}: **{n}**")

        c1, c2 = st.columns(2)

        with c1:
            st.markdown("#### Ranking de Proveedores Sospechosos")
            st.caption("Score compuesto: sobreprecio, concentración, y multigiro.")
            df_sosp = xref.ranking_proveedores_sospechosos(top_n=100)
            if filtro_global:
                q = filtro_global.lower()
                df_sosp = df_sosp[df_sosp['nombre_proveedor'].str.lower().str.contains(q, na=False) |
                                  df_sosp['rut_proveedor'].str.lower().str.contains(q, na=False)]

            if not df_sosp.empty:
                df_sosp = df_sosp.head(10)
                # Botón descarga del ranking
                csv_sosp = df_sosp.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "📥 Descargar ranking", csv_sosp,
                    file_name=f"proveedores_sospechosos_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv", key="dl_sosp"
                )
                df_sosp['monto_total'] = df_sosp['monto_total'].apply(format_clp_full)
                st.dataframe(df_sosp, hide_index=True, use_container_width=True)
                # Quick-investigate buttons for top suspicious providers
                inv_entities = list(zip(df_sosp['nombre_proveedor'], df_sosp['rut_proveedor']))
                _investigate_buttons(inv_entities, "sosp", "proveedor")
            else:
                st.info("Sin anomalías encontradas bajo este filtro.")

            st.markdown("#### Organismos de Mayor Riesgo")
            st.caption("Score basado en tratos directos excesivos y anomalías.")
            df_riesgo = xref.ranking_riesgo_organismos()

            if filtro_global:
                q = filtro_global.lower()
                df_riesgo = df_riesgo[df_riesgo['nombre_comprador'].str.lower().str.contains(q, na=False) |
                                      df_riesgo['rut_comprador'].str.lower().str.contains(q, na=False)]

            if not df_riesgo.empty:
                df_riesgo_show = df_riesgo.head(10)
                df_riesgo_show['monto_total'] = df_riesgo_show['monto_total'].apply(format_clp_full)
                st.dataframe(df_riesgo_show, hide_index=True, use_container_width=True)
                inv_orgs = list(zip(df_riesgo.head(10)['nombre_comprador'], df_riesgo.head(10)['rut_comprador']))
                _investigate_buttons(inv_orgs, "riesgo_org", "organismo")
            else:
                st.info("Sin datos para analizar.")

        with c2:
            st.markdown("#### Abuso de Trato Directo")
            st.caption("Organismos con mayor porcentaje de trato directo sobre total de OC.")
            df_td = xref.ratio_tratos_directos()

            if filtro_global:
                q = filtro_global.lower()
                df_td = df_td[df_td['nombre_comprador'].str.lower().str.contains(q, na=False) |
                              df_td['rut_comprador'].str.lower().str.contains(q, na=False)]

            if not df_td.empty:
                df_td['monto_total'] = df_td['monto_total'].apply(format_clp_full)
                df_td['monto_td'] = df_td['monto_td'].apply(format_clp_full)
                td_cols = ['nombre_comprador', 'rut_comprador', 'ratio_td', 'n_trato_directo', 'n_total', 'monto_td', 'monto_total']
                td_cols = [c for c in td_cols if c in df_td.columns]
                st.dataframe(df_td.head(10)[td_cols], hide_index=True, use_container_width=True)
            else:
                st.info("Sin datos bajo este filtro.")

            st.markdown("#### Concentración de Capital")
            st.caption("Proveedores que acumulan el mayor porcentaje del gasto público.")
            df_conc = xref.concentracion_capital(top_n=50)

            if filtro_global:
                q = filtro_global.lower()
                df_conc = df_conc[df_conc['nombre_proveedor'].str.lower().str.contains(q, na=False) |
                                  df_conc['rut_proveedor'].str.lower().str.contains(q, na=False)]

            if not df_conc.empty:
                df_conc = df_conc.head(5)
                df_conc['total_adjudicado'] = df_conc['total_adjudicado'].apply(format_clp_full)
                conc_cols = ['nombre_proveedor', 'rut_proveedor', 'total_adjudicado', 'pct_del_total', 'n_ordenes']
                conc_cols = [c for c in conc_cols if c in df_conc.columns]
                st.dataframe(df_conc[conc_cols], hide_index=True, use_container_width=True)
                if 'rut_proveedor' in df_conc.columns:
                    inv_conc = list(zip(df_conc['nombre_proveedor'], df_conc['rut_proveedor']))
                    _investigate_buttons(inv_conc, "conc", "proveedor")
            else:
                st.info("Sin datos bajo este filtro.")

        st.markdown("---")
        st.markdown(
            '<div class="section-header">'
            '<div class="icon red">🏛️</div>'
            '<div><h3>Aportes SERVEL vs. Adjudicaciones</h3>'
            '<p>Correlación entre financistas de campañas y empresas con trato directo</p></div>'
            '</div>',
            unsafe_allow_html=True,
        )
        df_servel_cruzado = xref.cruce_servel_compras()

        if filtro_global and not df_servel_cruzado.empty:
            q = filtro_global.lower()
            df_servel_cruzado = df_servel_cruzado[
                df_servel_cruzado['nombre_aportante'].str.lower().str.contains(q, na=False) |
                df_servel_cruzado['rut_proveedor_aportante'].str.lower().str.contains(q, na=False) |
                df_servel_cruzado['politico_o_partido'].str.lower().str.contains(q, na=False)
            ]

        if not df_servel_cruzado.empty:
            df_servel_cruzado['inversion_electoral'] = df_servel_cruzado['inversion_electoral'].apply(format_clp_full)
            df_servel_cruzado['retorno_licitaciones'] = df_servel_cruzado['retorno_licitaciones'].apply(format_clp_full)
            st.dataframe(df_servel_cruzado, hide_index=True, use_container_width=True)
        else:
            st.info("🛡️ Nivel 0 de corrupción detectado o matriz limpia bajo este filtro. (Asegúrate de haber procesado SERVEL).")

        st.markdown("---")
        st.markdown("#### Malla Societaria — Beneficiarios Finales")
        st.caption("Personas naturales detrás de empresas que ganan contratos públicos.")
        df_malla = xref.cruce_malla_societaria()

        if filtro_global and not df_malla.empty:
            q = filtro_global.lower()
            df_malla = df_malla[
                df_malla['CABECILLA_OCULTO'].str.lower().str.contains(q, na=False) |
                df_malla['EMPRESA_PANTALLA'].str.lower().str.contains(q, na=False) |
                df_malla['RUT_CABECILLA'].str.lower().str.contains(q, na=False)
            ]

        if not df_malla.empty:
            df_malla['MONTO_EXTRAIDO'] = df_malla['MONTO_EXTRAIDO'].apply(format_clp_full)
            st.dataframe(df_malla, hide_index=True, use_container_width=True)
        else:
            st.info("No hay datos de dueños reales vinculados al Mercado Público en la base local aún.")

        # ── Cruce 8: Anomalías → Personas (SERVEL + Lobby) ──
        st.markdown("---")
        st.markdown(
            '<div class="section-header">'
            '<div class="icon purple">🕵️</div>'
            '<div><h3>Anomalías ↔ Vínculos Políticos</h3>'
            '<p>Proveedores flaggeados que además aparecen como donantes (SERVEL) o en audiencias de lobby</p></div>'
            '</div>',
            unsafe_allow_html=True,
        )
        with st.spinner("Ejecutando detector + cruce de personas..."):
            try:
                df_cruce_personas = xref.cruce_anomalias_personas()
                if filtro_global and not df_cruce_personas.empty:
                    q = filtro_global.lower()
                    df_cruce_personas = df_cruce_personas[
                        df_cruce_personas['nombre_proveedor'].str.lower().str.contains(q, na=False) |
                        df_cruce_personas['rut_proveedor'].str.lower().str.contains(q, na=False)
                    ]
                if not df_cruce_personas.empty:
                    df_cruce_personas['monto_anomalo'] = df_cruce_personas['monto_anomalo'].apply(format_clp_full)
                    if 'donacion_total_servel' in df_cruce_personas.columns:
                        df_cruce_personas['donacion_total_servel'] = df_cruce_personas['donacion_total_servel'].apply(format_clp_full)
                    st.dataframe(df_cruce_personas, hide_index=True, use_container_width=True)
                    st.warning(
                        f"⚠️ {len(df_cruce_personas)} proveedores con anomalías detectadas "
                        "Y vínculos políticos confirmados."
                    )
                else:
                    st.info("🛡️ Ningún proveedor anómalo tiene vínculos SERVEL/Lobby detectados (o las fuentes no están cargadas).")
            except Exception as exc:
                logger.warning("Error en cruce anomalías→personas: %s", exc)
                st.info("Cruce anomalías→personas no disponible. El detector puede tardar con datasets grandes.")

        # ── Cruce: Compradores bajo Fiscalización CGR ──
        st.markdown(
            '<div class="section-header">'
            '<div class="icon amber">🔎</div>'
            '<div><h3>Compradores bajo Fiscalización CGR</h3>'
            '<p>Organismos compradores actualmente bajo fiscalización de la Contraloría</p></div>'
            '</div>',
            unsafe_allow_html=True,
        )
        try:
            from contraloria_connector import ContraloriaConnector
            cgr = ContraloriaConnector(DB_PATH)
            df_fisc = cgr.cruzar_compradores_fiscalizados()
            if not df_fisc.empty:
                if filtro_global:
                    q = filtro_global.lower()
                    df_fisc = df_fisc[
                        df_fisc['nombre_comprador'].str.lower().str.contains(q, na=False) |
                        df_fisc['entidad_fiscalizada'].str.lower().str.contains(q, na=False)
                    ]
                if not df_fisc.empty:
                    df_fisc['gasto_total'] = df_fisc['gasto_total'].apply(format_clp_full)
                    st.dataframe(df_fisc, hide_index=True, use_container_width=True)
                    st.warning(
                        f"⚠️ {df_fisc['nombre_comprador'].nunique()} organismos compradores "
                        "están bajo fiscalización activa de la CGR."
                    )
                else:
                    st.info("🛡️ Sin coincidencias con el filtro actual.")
            else:
                st.info("🛡️ Sin datos de fiscalizaciones cargados. Ejecuta el conector de Contraloría primero.")
        except ImportError:
            st.info("Conector de Contraloría no disponible.")
        except Exception as exc:
            logger.warning("Error en cruce compradores-fiscalizaciones: %s", exc)
            st.info("Cruce compradores↔fiscalizaciones no disponible.")

        # ── Cruce: Funcionarios con intereses en proveedores (InfoProbidad) ──
        st.markdown(
            '<div class="section-header">'
            '<div class="icon green">🏛️</div>'
            '<div><h3>Conflictos de Interés — InfoProbidad</h3>'
            '<p>Funcionarios que declararon actividades o participación en proveedores del Estado</p></div>'
            '</div>',
            unsafe_allow_html=True,
        )
        try:
            from infoprobidad_connector import InfoProbidadConnector
            ip = InfoProbidadConnector(DB_PATH)

            # Obtener top proveedores sospechosos para cruzar
            with sqlite3.connect(DB_PATH) as conn_ip:
                top_proveedores = pd.read_sql_query(
                    """
                    SELECT DISTINCT nombre_proveedor,
                           SUM(monto_total_item) as gasto_total
                    FROM ordenes_items
                    WHERE estado != '9' AND nombre_proveedor IS NOT NULL
                    GROUP BY nombre_proveedor
                    ORDER BY gasto_total DESC
                    LIMIT 20
                    """,
                    conn_ip,
                )

            if not top_proveedores.empty:
                conflictos = []
                with st.spinner("Consultando InfoProbidad (SPARQL)..."):
                    for _, prov in top_proveedores.iterrows():
                        nombre_prov = prov["nombre_proveedor"]
                        if nombre_prov and len(nombre_prov) > 5:
                            # Usar solo la parte más significativa del nombre
                            palabras = [
                                p for p in nombre_prov.split()
                                if p.upper() not in {"S.A.", "SPA", "SpA", "LTDA", "LTDA.",
                                                     "E.I.R.L.", "S.A", "EIRL", "Y", "DE",
                                                     "DEL", "LA", "LOS", "LAS"}
                                and len(p) > 2
                            ]
                            if palabras:
                                busqueda = " ".join(palabras[:2])
                                cruces = ip.cruzar_con_proveedor(busqueda)
                                for c in cruces:
                                    c["proveedor_buscado"] = nombre_prov
                                    c["gasto_total_proveedor"] = prov["gasto_total"]
                                    conflictos.append(c)

                if conflictos:
                    df_conflictos = pd.DataFrame(conflictos)
                    df_conflictos['gasto_total_proveedor'] = df_conflictos['gasto_total_proveedor'].apply(format_clp_full)
                    st.dataframe(df_conflictos, hide_index=True, use_container_width=True)
                    st.error(
                        f"🚨 {len(conflictos)} posibles conflictos de interés detectados. "
                        "Funcionarios con vínculos declarados a proveedores del Estado."
                    )
                else:
                    st.info("🛡️ Sin conflictos de interés detectados en los top 20 proveedores.")
            else:
                st.info("Sin proveedores en la base de datos para cruzar.")
        except ImportError:
            st.info("Conector de InfoProbidad no disponible.")
        except Exception as exc:
            logger.warning("Error en cruce InfoProbidad: %s", exc)
            st.info("Cruce funcionarios↔proveedores (InfoProbidad) no disponible.")

    except (OSError, pd.errors.DatabaseError, sqlite3.Error) as e:
        logger.error("Error en cruces forenses: %s", e)
        st.error(f"Error cargando cruces forenses: {e}")
        st.info("Esto puede ocurrir si la base de datos está vacía o corrupta. Ejecuta `python main.py` para recargar datos.")

    # ══════════════════════════════════════════════════════════════════════════
    # PESTAÑA 3: REGISTRO FORENSE RAW



def _render_tab_datos(df_filtrado, filtro_global):
    st.markdown(
        '<div class="section-header">'
        '<div class="icon blue">📋</div>'
        '<div><h3>Registro Completo</h3>'
        f'<p>{len(df_filtrado):,} registros cargados</p></div>'
        '</div>',
        unsafe_allow_html=True,
    )

    # Sub-tabs para OC y Licitaciones
    sub_oc, sub_lic = st.tabs(["Órdenes de Compra", "Licitaciones Públicas"])

    with sub_oc:
        if not df_filtrado.empty:
            mostrar_col = [
                'codigo_oc', 'tipo_oc', 'categoria_riesgo', 'nombre_comprador',
                'rut_comprador', 'nombre_proveedor', 'rut_proveedor',
                'nombre_producto', 'cantidad',
                'precio_unitario', 'monto_total_item', 'estado', 'fecha_creacion'
            ]
            dt_display = df_filtrado[mostrar_col].copy()
            if pd.api.types.is_datetime64_any_dtype(dt_display['fecha_creacion']):
                dt_display['fecha_creacion'] = dt_display['fecha_creacion'].dt.strftime('%Y-%m-%d')

            # Botón de descarga
            csv_oc = dt_display.to_csv(index=False).encode('utf-8')
            st.download_button(
                "📥 Descargar datos OC (CSV)", csv_oc,
                file_name=f"ordenes_compra_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )

            st.dataframe(
                dt_display,
                use_container_width=True,
                height=500,
                hide_index=True,
                column_config={
                    "codigo_oc": st.column_config.TextColumn("Código OC"),
                    "tipo_oc": st.column_config.TextColumn("Tipo"),
                    "categoria_riesgo": st.column_config.TextColumn("Nivel Riesgo"),
                    "nombre_comprador": st.column_config.TextColumn("Entidad de Gobierno"),
                    "rut_comprador": st.column_config.TextColumn("RUT Comprador"),
                    "nombre_proveedor": st.column_config.TextColumn("Proveedor / Empresa"),
                    "rut_proveedor": st.column_config.TextColumn("RUT Proveedor"),
                    "nombre_producto": st.column_config.TextColumn("Ítem Adquirido", width="medium"),
                    "cantidad": st.column_config.NumberColumn("Ud.", format="%d"),
                    "precio_unitario": st.column_config.NumberColumn("Precio Ud.", format="$%d"),
                    "monto_total_item": st.column_config.ProgressColumn(
                        "Monto ($CLP)",
                        format="$%f",
                        min_value=0,
                        max_value=max(dt_display['monto_total_item'].max(), 1)
                    ),
                    "estado": st.column_config.TextColumn("Estado"),
                    "fecha_creacion": st.column_config.DateColumn("Fecha Emisión")
                }
            )
        else:
            st.warning("No hay registros con los filtros actuales.")

    with sub_lic:
        try:
            df_lic = load_licitaciones(limit=5000)
            if not df_lic.empty:
                if filtro_global:
                    q = filtro_global.lower()
                    mask = pd.Series([False] * len(df_lic))
                    for col in df_lic.select_dtypes(include='object').columns:
                        mask = mask | df_lic[col].str.lower().str.contains(q, na=False)
                    df_lic = df_lic[mask]

                st.caption(f"{len(df_lic)} licitaciones encontradas")

                csv_lic = df_lic.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "📥 Descargar licitaciones (CSV)", csv_lic,
                    file_name=f"licitaciones_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
                st.dataframe(df_lic, use_container_width=True, height=500, hide_index=True)
            else:
                st.info("No hay licitaciones cargadas aún.")
        except (OSError, pd.errors.DatabaseError) as exc:
            st.info(f"Tabla de licitaciones no disponible: {exc}")

    # ══════════════════════════════════════════════════════════════════════════
    # PESTAÑA 4: FUENTES OFICIALES Y MEDIOS



def _render_tab_fuentes(df_filtrado):
    st.markdown(
        '<div class="section-header">'
        '<div class="icon blue">📡</div>'
        '<div><h3>Fuentes y Medios</h3>'
        '<p>Acceso directo a portales gubernamentales y periodismo de investigación</p></div>'
        '</div>',
        unsafe_allow_html=True,
    )

    col_m1, col_m2, col_m3 = st.columns(3)

    with col_m1:
        st.markdown(
            '<div class="source-card">'
            '<span class="badge live">EN VIVO</span>'
            '<h4>TV Senado</h4>'
            '<p>Transmisión directa desde la Sala y Comisiones del Senado.</p>'
            '</div>',
            unsafe_allow_html=True,
        )
        st.markdown("<a href='https://tv.senado.cl/' target='_blank' class='btn-portal'>Ir a TV Senado</a>", unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown(
            '<div class="source-card">'
            '<span class="badge live">EN VIVO</span>'
            '<h4>Cámara de Diputados</h4>'
            '<p>Comisiones Investigadoras en directo.</p>'
            '</div>',
            unsafe_allow_html=True,
        )
        st.markdown("<a href='https://webtv.camara.cl/' target='_blank' class='btn-portal'>Ir a Cámara TV</a>", unsafe_allow_html=True)

    with col_m2:
        st.markdown(
            '<div class="source-card">'
            '<span class="badge live">EN VIVO</span>'
            '<h4>Antofagasta TV</h4>'
            '<p>Foco territorial en GORE y municipios de la región.</p>'
            '</div>',
            unsafe_allow_html=True,
        )
        st.markdown("<a href='https://www.youtube.com/@antofagastatv30/live' target='_blank' class='btn-portal'>Ver ATV</a>", unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown(
            '<div class="source-card">'
            '<span class="badge live">EN VIVO</span>'
            '<h4>TVN 24 Horas</h4>'
            '<p>Cobertura nacional abierta.</p>'
            '</div>',
            unsafe_allow_html=True,
        )
        st.markdown("<a href='https://www.24horas.cl/envivo' target='_blank' class='btn-portal'>Ver 24H</a>", unsafe_allow_html=True)

    with col_m3:
        st.markdown(
            '<div class="source-card">'
            '<span class="badge press">INVESTIGACIÓN</span>'
            '<h4>CIPER Chile</h4>'
            '<p>Centro de Investigación Periodística independiente.</p>'
            '</div>',
            unsafe_allow_html=True,
        )
        st.markdown("<a href='https://www.ciperchile.cl/' target='_blank' class='btn-portal'>Leer CIPER</a>", unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown(
            '<div class="source-card">'
            '<span class="badge press">MEDIOS</span>'
            '<h4>BioBío TV</h4>'
            '<p>Alertas preventivas por radio y televisión.</p>'
            '</div>',
            unsafe_allow_html=True,
        )
        st.markdown("<a href='https://www.biobiochile.cl/bbtv' target='_blank' class='btn-portal'>Ver BioBío</a>", unsafe_allow_html=True)

    # ── Nuevas fuentes de datos integradas ──
    st.markdown("---")
    st.markdown(
        '<div class="section-header">'
        '<div class="icon green">🗂️</div>'
        '<div><h3>Fuentes Integradas al Motor de Cruce</h3>'
        '<p>Se consultan automáticamente en cruces forenses y búsqueda de personas</p></div>'
        '</div>',
        unsafe_allow_html=True,
    )

    col_f1, col_f2, col_f3 = st.columns(3)

    with col_f1:
        st.markdown(
            '<div class="source-card">'
            '<span class="badge data">DATOS ABIERTOS</span>'
            '<h4>InfoProbidad</h4>'
            '<p>116,000+ declaraciones de patrimonio e intereses de funcionarios públicos: '
            'cargo, institución, actividades, bienes, acciones en sociedades.</p>'
            '</div>',
            unsafe_allow_html=True,
        )
        st.markdown("<a href='https://www.infoprobidad.cl' target='_blank' class='btn-portal'>Ir a InfoProbidad</a>", unsafe_allow_html=True)

    with col_f2:
        st.markdown(
            '<div class="source-card">'
            '<span class="badge data">DATOS ABIERTOS</span>'
            '<h4>Contraloría (SICA)</h4>'
            '<p>2,000+ fiscalizaciones activas: región, sector, entidad, tipo, '
            'materia de investigación.</p>'
            '</div>',
            unsafe_allow_html=True,
        )
        st.markdown("<a href='https://www.contraloria.cl/web/cgr/fiscalizaciones-en-curso' target='_blank' class='btn-portal'>Ver Fiscalizaciones</a>", unsafe_allow_html=True)

    with col_f3:
        st.markdown(
            '<div class="source-card">'
            '<span class="badge data">DATOS ABIERTOS</span>'
            '<h4>DIPRES (datos.gob.cl)</h4>'
            '<p>Presupuestos por institución, dotación de personal, '
            'gastos en honorarios y contrataciones.</p>'
            '</div>',
            unsafe_allow_html=True,
        )
        st.markdown("<a href='https://www.dipres.gob.cl' target='_blank' class='btn-portal'>Ir a DIPRES</a>", unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════════
    # PESTAÑA 5: EN LA MIRA — Alertas de Personas de Interés Público



def _render_tab_mira(df_filtrado):
    st.markdown(
        '<div class="section-header">'
        '<div class="icon purple">🔍</div>'
        '<div><h3>Personas en la Mira</h3>'
        '<p>Búsqueda en 7 fuentes oficiales: InfoLobby, datos.gob.cl, Contraloría, SERVEL, '
        'Mercado Público, InfoProbidad y Fiscalizaciones CGR</p></div>'
        '</div>',
        unsafe_allow_html=True,
    )

    col_buscar, col_opciones = st.columns([3, 1])
    with col_buscar:
        mira_nombre = st.text_input(
            "Nombre de la persona",
            placeholder="Ej: Camila Flores, Juan Pérez...",
            key="mira_nombre_input",
        )
    with col_opciones:
        mira_incluir_compras = st.checkbox("Incluir Mercado Público", value=False, key="mira_compras")

    mira_buscar = st.button("🔎 Buscar alertas", key="mira_buscar_btn", type="primary")

    if mira_buscar and mira_nombre.strip():
        motor = AlertasPersonas()
        with st.spinner(f"Consultando fuentes oficiales para '{mira_nombre}'..."):
            alertas = motor.buscar(mira_nombre.strip(), incluir_compras=mira_incluir_compras)

        if not alertas:
            st.info(f"No se encontraron alertas públicas para **{mira_nombre}**.")
        else:
            # Resumen superior
            fuentes_encontradas = sorted({a["fuente"] for a in alertas})
            tipos_encontrados = sorted({a["tipo_alerta"] for a in alertas})

            st.success(f"**{len(alertas)}** alerta(s) encontrada(s) en **{len(fuentes_encontradas)}** fuente(s).")

            # Métricas rápidas
            cols_metricas = st.columns(min(len(tipos_encontrados), 5))
            for i, tipo in enumerate(tipos_encontrados[:5]):
                count = sum(1 for a in alertas if a["tipo_alerta"] == tipo)
                color_map = {
                    "LOBBY": "🤝", "SANCIÓN": "⚖️", "DICTAMEN": "📜",
                    "COMPRA_PUBLICA": "🛒", "APORTE_ELECTORAL": "🗳️",
                }
                icono = color_map.get(tipo, "📋")
                cols_metricas[i].metric(f"{icono} {tipo}", count)

            st.markdown("---")

            # Tarjetas de alerta estilo Microsoft
            border_colors = {
                "LOBBY": "#3B82F6",
                "SANCIÓN": "#EF4444",
                "DICTAMEN": "#8B5CF6",
                "COMPRA_PUBLICA": "#10B981",
                "APORTE_ELECTORAL": "#F59E0B",
            }

            for alerta in alertas:
                bcolor = border_colors.get(alerta["tipo_alerta"], "#475569")
                fecha_display = html_mod.escape(alerta["fecha"]) if alerta["fecha"] != "sin fecha" else "—"
                safe_tipo = html_mod.escape(alerta.get("tipo_alerta", ""))
                safe_fuente = html_mod.escape(alerta.get("fuente", ""))
                safe_desc = html_mod.escape(alerta.get("descripcion", ""))
                raw_url = alerta.get("url", "")
                # Solo permitir URLs http/https
                safe_url = html_mod.escape(raw_url) if raw_url and raw_url.startswith(("http://", "https://")) else ""
                url_link = (
                    f"<a href='{safe_url}' target='_blank' rel='noopener noreferrer' "
                    f"style='color:{bcolor};text-decoration:none;font-size:0.8rem;font-weight:500;'>Ver fuente ↗</a>"
                    if safe_url else ""
                )
                card_html = f"""
                <div style="
                    border-left: 3px solid {bcolor};
                    background: linear-gradient(135deg, rgba(15,23,42,0.6) 0%, rgba(30,41,59,0.3) 100%);
                    padding: 14px 18px;
                    margin-bottom: 8px;
                    border-radius: 10px;
                    border: 1px solid rgba(51,65,85,0.3);
                    border-left: 3px solid {bcolor};
                ">
                    <div style="display:flex;justify-content:space-between;align-items:center;">
                        <span style="color:{bcolor};font-weight:600;font-size:0.78rem;letter-spacing:0.04em;text-transform:uppercase;">
                            {safe_tipo}
                        </span>
                        <span style="color:#475569;font-size:0.75rem;">{fecha_display}</span>
                    </div>
                    <div style="color:#64748B;font-size:0.75rem;margin-top:2px;font-weight:500;">
                        {safe_fuente}
                    </div>
                    <div style="color:#CBD5E1;margin-top:8px;font-size:0.85rem;line-height:1.5;">
                        {safe_desc}
                    </div>
                    <div style="margin-top:8px;">{url_link}</div>
                </div>
                """
                st.markdown(card_html, unsafe_allow_html=True)

    elif mira_buscar:
        st.warning("Ingresa un nombre para buscar.")

    # ══════════════════════════════════════════════════════════════════════════
    # PESTAÑA 6: INTELIGENCIA CIVIL (REPORTES Y LOBBY)



def _render_tab_denuncias(df_filtrado):
    st.markdown(
        '<div class="section-header">'
        '<div class="icon amber">📡</div>'
        '<div><h3>Radar Legislativo</h3>'
        '<p>Monitoreo de proyectos de ley que modifiquen normas de compras o subvenciones</p></div>'
        '</div>',
        unsafe_allow_html=True,
    )

    col_leg1, col_leg2, col_leg3 = st.columns(3)
    with col_leg1:
        st.markdown(
            '<div class="source-card">'
            '<span class="badge data">LEGISLACIÓN</span>'
            '<h4>Ley de Compras Públicas</h4>'
            '<p>Estado de proyectos de ley relacionados con compras del Estado.</p>'
            '</div>',
            unsafe_allow_html=True,
        )
        st.markdown("<a href='https://www.senado.cl/appsenado/templates/tramitacion/index.html' target='_blank' class='btn-portal'>Buscar en Senado</a>", unsafe_allow_html=True)
    with col_leg2:
        st.markdown(
            '<div class="source-card">'
            '<span class="badge data">LEGISLACIÓN</span>'
            '<h4>Subvenciones y Transferencias</h4>'
            '<p>Cambios normativos en subvenciones públicas.</p>'
            '</div>',
            unsafe_allow_html=True,
        )
        st.markdown("<a href='https://www.camara.cl/legislacion/ProyectosDeLey/proyectos_ley.aspx' target='_blank' class='btn-portal'>Buscar en Cámara</a>", unsafe_allow_html=True)
    with col_leg3:
        st.markdown(
            '<div class="source-card">'
            '<span class="badge data">LEGISLACIÓN</span>'
            '<h4>Transparencia y Probidad</h4>'
            '<p>Proyectos de ley sobre transparencia del Estado.</p>'
            '</div>',
            unsafe_allow_html=True,
        )
        st.markdown("<a href='https://www.bcn.cl/leychile' target='_blank' class='btn-portal'>Buscar en BCN</a>", unsafe_allow_html=True)

    st.markdown("<div style='margin:20px 0; border-top:1px solid rgba(51,65,85,0.3);'></div>", unsafe_allow_html=True)

    st.markdown(
        '<div class="section-header">'
        '<div class="icon red">📨</div>'
        '<div><h3>Reportar un Hallazgo</h3>'
        '<p>Aporta datos de posibles irregularidades para cruce con bases de datos públicas</p></div>'
        '</div>',
        unsafe_allow_html=True,
    )

    with st.form("feedback_form", clear_on_submit=True):
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            f_tipo = st.selectbox(
                "Categoría del Hallazgo / Alerta",
                [
                 "Desfalco / Sobreprecio Sistemático",
                 "Proyecto de Ley Oculto / Lobby Directo",
                 "Tráfico de Influencias / Favores Políticos",
                 "Fraccionamiento (Licitación evadida)",
                 "Horario Vampiro (De Madrugada)",
                 "Empresa de Cartón / Giro dudoso",
                 ]
            )
        with col_f2:
            f_dato = st.text_input("Dato Clave (OC, RUT, Apellido o Boletín Ley)")

        f_comentario = st.text_area("Descripción del hallazgo:", height=120)

        submit_btn = st.form_submit_button("Enviar Reporte", type="primary")

        if submit_btn:
            if f_dato.strip() == "":
                st.error("❌ Debes incluir al menos un dato clave (OC, RUT o nombre).")
            else:
                save_feedback(f_tipo, f_dato, f_comentario)
                st.success(f"✅ Reporte sobre '{f_dato}' registrado correctamente.")

    # ══════════════════════════════════════════════════════════════════════════
    # PESTAÑA 6: ASISTENTE IA (CHATBOT FORENSE)



def _render_tab_ia(df_filtrado, prompt=None):
    st.markdown(
        '<div class="section-header">'
        '<div class="icon blue">\U0001f9e0</div>'
        '<div><h3>Cerebro Forense — Asistente de Investigación</h3>'
        '<p>IA con acceso a 7 fuentes oficiales: Mercado Público, SERVEL, InfoLobby, '
        'Contraloría, InfoProbidad, DIPRES y datos.gob. Cruce automático en cada consulta.</p></div>'
        '</div>',
        unsafe_allow_html=True,
    )

    if "ia_messages" not in st.session_state:
        st.session_state.ia_messages = [
            {"role": "assistant", "content":
             "**\U0001f9e0 Cerebro Forense activado.**\n\n"
             "Tengo acceso directo a la base de datos de órdenes de compra, "
             "aportes SERVEL, registros de lobby, declaraciones de probidad, "
             "fiscalizaciones de la Contraloría y más.\n\n"
             "Puedo investigar **personas**, **empresas**, **organismos** o "
             "ejecutar **análisis de anomalías** completos. ¿Qué necesitas?"}
        ]
    if "ia_tools_used" not in st.session_state:
        st.session_state.ia_tools_used = {}

    # ── Queries sugeridas ──
    st.markdown(
        '<div style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:16px;">',
        unsafe_allow_html=True,
    )
    _SUGGESTED_QUERIES = [
        ("\U0001f4ca Reporte ejecutivo", "Dame un reporte ejecutivo completo de la base de datos"),
        ("\U0001f6a8 Top sospechosos", "¿Cuáles son los proveedores más sospechosos y por qué?"),
        ("\U0001f5f3\ufe0f Cruce SERVEL", "¿Hay aportantes electorales que después ganan licitaciones?"),
        ("\U0001f3db\ufe0f Trato directo", "¿Qué organismos abusan del trato directo?"),
        ("\U0001f575\ufe0f Anomalías", "Analiza todas las anomalías detectadas en la base de datos"),
    ]
    col_chips = st.columns(len(_SUGGESTED_QUERIES))
    for i, (label, query) in enumerate(_SUGGESTED_QUERIES):
        with col_chips[i]:
            if st.button(label, key=f"chip_{i}", use_container_width=True):
                st.session_state["_pending_query"] = query
                st.rerun()

    # ── Definir renderer de burbujas ──
    def _render_chat_bubbles(messages: list[dict]) -> str:
        """Genera HTML de burbujas de chat con indicadores de herramientas."""
        html_parts = []
        for idx, msg in enumerate(messages):
            if msg["role"] == "system":
                continue
            is_user = msg["role"] == "user"
            row_class = "chat-row-user" if is_user else "chat-row-assistant"
            bubble_class = "bubble-user" if is_user else "bubble-assistant"
            label = "Tú" if is_user else "\U0001f9e0 Cerebro Forense"
            align = "text-align:right;" if is_user else "text-align:left;"
            safe_content = html_mod.escape(msg["content"])
            safe_content = safe_content.replace("\n", "<br>")
            safe_content = re.sub(
                r"\*\*(.+?)\*\*", r"<strong>\1</strong>", safe_content
            )
            # Indicador de herramientas usadas para respuestas del asistente
            tools_badge = ""
            if not is_user and idx in st.session_state.ia_tools_used:
                tools_list = st.session_state.ia_tools_used[idx]
                badges = "".join(
                    f'<span style="display:inline-block;background:rgba(37,99,235,0.12);'
                    f'color:#2563eb;font-size:11px;padding:2px 8px;border-radius:10px;'
                    f'margin:2px 3px 0 0;">\u26a1 {t}</span>'
                    for t in tools_list
                )
                tools_badge = f'<div style="margin-top:8px;">{badges}</div>'

            html_parts.append(
                f'<div class="chat-label" style="{align}">{label}</div>'
                f'<div class="{row_class}">'
                f'<div class="chat-bubble {bubble_class}">{safe_content}{tools_badge}</div>'
                f'</div>'
            )
        return "\n".join(html_parts)

    # ── Placeholder para el chat (se llena después del procesamiento) ──
    chat_placeholder = st.empty()

    # ── Rate limit ──
    if "api_calls" not in st.session_state:
        st.session_state.api_calls = 0

    try:
        headers = st.context.headers
        user_ip = headers.get("X-Forwarded-For", headers.get("Host", "local")).split(",")[0].strip()
    except (AttributeError, KeyError):
        user_ip = "local"

    today_str = datetime.now().strftime("%Y-%m-%d")
    daily_used = get_rate_limit_usage(user_ip, today_str)

    daily_limit = DAILY_QUERY_LIMIT
    remaining = max(0, daily_limit - daily_used)

    # ── Procesar prompt ANTES de renderizar chat ──
    pending = st.session_state.pop("_pending_query", None)
    effective_prompt = pending or prompt

    if effective_prompt:
        if remaining <= 0:
            st.error(f"\U0001f6d1 Límite diario alcanzado ({daily_limit} consultas). Vuelve mañana.")
        else:
            st.session_state.api_calls += 1
            increment_rate_limit_usage(user_ip, today_str)
            st.session_state.ia_messages.append({"role": "user", "content": effective_prompt})

            api_key = os.getenv("DEEPSEEK_API_KEY", "")
            if not api_key:
                revelacion = "\u26a0\ufe0f Asistente IA no disponible: falta la clave DEEPSEEK_API_KEY en .env"
                tools_used = []
            else:
                try:
                    intents = classify_intent(effective_prompt)
                    intent_labels = {"persona": "\U0001f464 Persona", "proveedor": "\U0001f3e2 Proveedor",
                                     "organismo": "\U0001f3db\ufe0f Organismo", "anomalia": "\U0001f6a8 Anomalía",
                                     "resumen": "\U0001f4ca Resumen", "general": "\U0001f50e General"}
                    detected = ", ".join(intent_labels.get(i, i) for i in intents)
                    st.toast(f"Intención detectada: {detected}", icon="\U0001f3af")

                    st.toast("Ejecutando herramientas forenses...", icon="\u26a1")
                    forensic_context, tools_used = build_forensic_context(effective_prompt)

                    st.toast("Escaneando base de datos local...", icon="\U0001f50d")
                    db_context = build_db_context(effective_prompt)
                    tools_used.append("DB Local")

                    st.toast("Buscando información en la web...", icon="\U0001f310")
                    web_context = build_web_context(effective_prompt)
                    tools_used.append("Web OSINT")

                    st.toast("Analizando con Cerebro Forense...", icon="\U0001f9e0")
                    revelacion = call_deepseek(
                        st.session_state.ia_messages, web_context, db_context,
                        forensic_context
                    )
                except (requests.RequestException, KeyError, ValueError) as ia_exc:
                    revelacion = f"Error al consultar el asistente IA: {ia_exc}"
                    tools_used = []
                    logger.error("Error en chat IA: %s", ia_exc)

            infil_match = re.search(r"\[EJECUTAR_INFILTRACION:\s*([\d\.\-Kk]+)\]", revelacion)
            clean_response = revelacion.replace(infil_match.group(0) if infil_match else "", "")

            st.session_state.ia_messages.append({"role": "assistant", "content": clean_response})
            msg_idx = len(st.session_state.ia_messages) - 1
            if tools_used:
                st.session_state.ia_tools_used[msg_idx] = tools_used

            if infil_match:
                rut_detectado = infil_match.group(1)
                st.warning(f"\u26a1 DESCARGA AUTOMÁTICA DE HISTORIAL PARA RUT: {rut_detectado}")
                with st.spinner("Consultando registros públicos de Mercado Público vía API..."):
                    from infiltrador_ia import infiltrar_rut
                    target_rut = rut_detectado.replace(".", "").strip()
                    if re.fullmatch(r"\d{7,8}-[\dkK]", target_rut):
                        infiltrar_rut(target_rut)
                        st.success("\u2705 Descarga histórica exitosa en la DB local.")
                        st.session_state.ia_messages.append({
                            "role": "system",
                            "content": f"SISTEMA: Infiltración para {rut_detectado} completada. Historial inyectado en DB."
                        })

            # Solo forzar rerun si la consulta vino de un botón Investigar
            # (el usuario está en otra pestaña y necesita ver el expander)
            if pending:
                st.session_state["_show_ia_response"] = True
                st.rerun()

    # ── Renderizar chat (ahora incluye mensajes nuevos si se procesó un prompt) ──
    chat_html = _render_chat_bubbles(st.session_state.ia_messages)
    chat_placeholder.markdown(
        f'<div class="chat-container" id="chat-scroll">{chat_html}</div>'
        '<script>var c=document.getElementById("chat-scroll");'
        'if(c)c.scrollTop=c.scrollHeight;</script>',
        unsafe_allow_html=True,
    )

    st.caption(f"\U0001f50b Consultas restantes hoy: **{remaining}**/{daily_limit}")


def main():
    init_feedback_db()

    # ENCABEZADO PRINCIPAL
    col_t1, col_t2, col_t3 = st.columns([0.06, 0.64, 0.3])
    with col_t1:
        if os.path.exists(_LOGO_PATH):
            st.image(_LOGO_PATH, use_container_width=True)
    with col_t2:
        st.title("Ojo del Pueblo")
        st.caption("Plataforma de fiscalización ciudadana — Compras públicas del Estado de Chile")
    with col_t3:
        st.markdown(
            f"<div style='text-align:right; padding-top:12px;'>"
            f"<span style='color:#475569; font-size:0.75rem;'>Última actualización</span><br>"
            f"<span style='color:#94A3B8; font-size:0.85rem; font-weight:600;'>"
            f"{datetime.now().strftime('%d/%m/%Y %H:%M')}</span>"
            f"</div>",
            unsafe_allow_html=True,
        )

    # VERIFICACIÓN DE BD
    if not os.path.exists(DB_PATH):
        st.error("🚫 Base de datos no encontrada.")
        st.info(
            "Ejecuta el pipeline para crear la base de datos:\n\n"
            "```bash\npython main.py\n```"
        )
        st.stop()

    # CARGA DE DATOS
    try:
        df = _cached_load()
    except (OSError, pd.errors.DatabaseError) as exc:
        logger.error("Error al cargar datos: %s", exc)
        st.error(f"Error al conectar con la base de datos: {exc}")
        st.info("Verifica que `auditoria_estado.db` no esté corrupta. Puedes restaurar un backup con:\n\n```bash\npython backup.py --list\npython backup.py --restore backups/<archivo>.db\n```")
        st.stop()

    if df.empty:
        _render_empty_state()
        return

    # ─────────────────────────────────────────────────────────────────────────
    # PANEL DE CONTROL LATERAL (FILTROS FORENSES)
    # ─────────────────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown(
            "<p style='color:#475569; font-size:0.7rem; text-transform:uppercase; "
            "letter-spacing:0.08em; font-weight:600; margin-bottom:4px;'>Búsqueda global</p>",
            unsafe_allow_html=True,
        )
        filtro_global = st.text_input(
            "Buscar", placeholder="RUT, nombre, empresa o código OC",
            key="filtro_global", label_visibility="collapsed",
        )

        st.markdown("<div style='margin:16px 0 8px 0; border-top:1px solid rgba(51,65,85,0.3);'></div>", unsafe_allow_html=True)
        st.markdown(
            "<p style='color:#475569; font-size:0.7rem; text-transform:uppercase; "
            "letter-spacing:0.08em; font-weight:600; margin-bottom:4px;'>Filtros</p>",
            unsafe_allow_html=True,
        )

        # Filtro por tipo de OC
        tipos_disp = sorted(df['tipo_oc'].dropna().unique().tolist())
        _TIPO_LABELS = {"TD": "TD — Trato Directo", "SE": "SE — Sin Especificar",
                        "AG": "AG — Compra Ágil", "CM": "CM — Convenio Marco",
                        "CC": "CC — Contrato", "CT": "CT — Contratación"}
        filtro_tipo = st.multiselect(
            "Tipo de OC",
            options=tipos_disp,
            format_func=lambda x: _TIPO_LABELS.get(x, x),
            default=[],
        )

        # Filtro por monto mínimo
        filtro_monto_min = st.number_input(
            "Monto mínimo ($CLP)", min_value=0, value=0, step=1_000_000,
            help="Filtrar OC con monto total mayor a este valor",
        )

        # Filtro por organismo (top 20 compradores)
        top_compradores = (
            df.groupby('nombre_comprador')['monto_total_item'].sum()
            .nlargest(30).index.tolist()
        )
        filtro_comprador = st.multiselect("Organismo comprador", options=top_compradores, default=[])

        categorias_disp = df['categoria_riesgo'].dropna().unique().tolist()
        filtro_cat = st.multiselect("Categoría de riesgo", options=categorias_disp, default=[])

        fechas_validas = df['fecha_creacion'].dropna()
        if not fechas_validas.empty:
            fecha_min = fechas_validas.min().date()
            fecha_max = fechas_validas.max().date()
            filtro_fecha = st.date_input(
                "Rango de fechas",
                value=(fecha_min, fecha_max),
                min_value=fecha_min,
                max_value=fecha_max,
            )
        else:
            filtro_fecha = None

        st.markdown("<div style='margin:12px 0; border-top:1px solid rgba(51,65,85,0.3);'></div>", unsafe_allow_html=True)

    # APLICAR FILTROS
    df_filtrado = df.copy()

    if filtro_global:
        query = re.escape(filtro_global.lower())
        mask = (
            df_filtrado['nombre_comprador'].str.lower().str.contains(query, na=False)
            | df_filtrado['nombre_proveedor'].str.lower().str.contains(query, na=False)
            | df_filtrado['codigo_oc'].str.lower().str.contains(query, na=False)
            | df_filtrado['rut_proveedor'].str.lower().str.contains(query, na=False)
        )
        if 'rut_comprador' in df_filtrado.columns:
            mask = mask | df_filtrado['rut_comprador'].str.lower().str.contains(query, na=False)
        if 'nombre_producto' in df_filtrado.columns:
            mask = mask | df_filtrado['nombre_producto'].str.lower().str.contains(query, na=False)
        df_filtrado = df_filtrado[mask]

    if filtro_tipo:
        df_filtrado = df_filtrado[df_filtrado['tipo_oc'].isin(filtro_tipo)]
    if filtro_monto_min > 0:
        oc_montos = df_filtrado.groupby('codigo_oc')['monto_total_item'].sum()
        oc_ok = oc_montos[oc_montos >= filtro_monto_min].index
        df_filtrado = df_filtrado[df_filtrado['codigo_oc'].isin(oc_ok)]
    if filtro_comprador:
        df_filtrado = df_filtrado[df_filtrado['nombre_comprador'].isin(filtro_comprador)]
    if filtro_cat:
        df_filtrado = df_filtrado[df_filtrado['categoria_riesgo'].isin(filtro_cat)]

    if filtro_fecha and isinstance(filtro_fecha, tuple) and len(filtro_fecha) == 2:
        desde, hasta = filtro_fecha
        ts_desde = pd.Timestamp(desde)
        ts_hasta = pd.Timestamp(hasta) + pd.Timedelta(days=1)
        mask_has_date = df_filtrado['fecha_creacion'].notna()
        mask_in_range = (
            (df_filtrado['fecha_creacion'] >= ts_desde)
            & (df_filtrado['fecha_creacion'] < ts_hasta)
        )
        df_filtrado = df_filtrado[
            (~mask_has_date) |
            (mask_has_date & mask_in_range)
        ]



    # ─────────────────────────────────────────────────────────────────────────
    # MÉTRICAS GLOBALES (usadas por múltiples tabs + share footer)
    # ─────────────────────────────────────────────────────────────────────────
    total_gasto = df_filtrado['monto_total_item'].sum() if not df_filtrado.empty else 0
    total_oc = df_filtrado['codigo_oc'].nunique() if not df_filtrado.empty else 0
    total_proveedores = df_filtrado['nombre_proveedor'].nunique() if not df_filtrado.empty else 0
    total_compradores = df_filtrado['nombre_comprador'].nunique() if not df_filtrado.empty else 0
    n_trato_directo = 0
    if not df_filtrado.empty:
        n_trato_directo = df_filtrado[df_filtrado['tipo_oc'].isin(OC_TIPO_TRATO_DIRECTO)]['codigo_oc'].nunique()
    pct_td = (n_trato_directo / total_oc * 100) if total_oc > 0 else 0

    # ─────────────────────────────────────────────────────────────────────────
    # ENRUTAMIENTO POR PESTAÑAS (Limpieza Visual)
    # ─────────────────────────────────────────────────────────────────────────

    tab_names = [
        "Panel General",
        "Cruces Forenses",
        "Datos Crudos",
        "Fuentes",
        "🔍 En la Mira",
        "Denuncias",
        "🧠 Asistente IA",
    ]

    # Mostrar última respuesta de la IA (visible desde cualquier pestaña)
    if st.session_state.pop("_show_ia_response", False):
        _ia_msgs = st.session_state.get("ia_messages", [])
        if _ia_msgs and _ia_msgs[-1]["role"] == "assistant":
            with st.expander("🧠 Respuesta del Cerebro Forense", expanded=True):
                st.markdown(_ia_msgs[-1]["content"])
                st.caption("💡 Puedes ver el historial completo en la pestaña **🧠 Asistente IA**.")

    tab_estadisticas, tab_cruce, tab_registro, tab_medios, tab_mira, tab_analistas, tab_ia = st.tabs(tab_names)



    with tab_estadisticas:
        _render_tab_general(df_filtrado, total_gasto, total_oc, total_proveedores, total_compradores, pct_td, n_trato_directo)

    with tab_cruce:
        _render_tab_cruces(df_filtrado, total_proveedores, total_compradores, n_trato_directo, filtro_global)

    with tab_registro:
        _render_tab_datos(df_filtrado, filtro_global)

    with tab_medios:
        _render_tab_fuentes(df_filtrado)

    with tab_mira:
        _render_tab_mira(df_filtrado)

    with tab_analistas:
        _render_tab_denuncias(df_filtrado)

    with tab_ia:
        ia_prompt = st.chat_input("¿Qué quieres investigar? (persona, empresa, organismo, anomalías...)")
        _render_tab_ia(df_filtrado, prompt=ia_prompt)

    # ─────────────────────────────────────────────────────────────────────────
    # COMPARTIR EN REDES SOCIALES
    # ─────────────────────────────────────────────────────────────────────────
    st.markdown("<div style='margin:32px 0 12px 0; border-top:1px solid rgba(51,65,85,0.3);'></div>", unsafe_allow_html=True)

    share_text = f"Encontré datos interesantes en Ojo del Pueblo: {total_oc:,} órdenes de compra por {format_clp(total_gasto)} bajo fiscalización ciudadana."
    encoded_text = urllib.parse.quote(share_text)

    col_share1, col_share2, col_share3 = st.columns(3)
    with col_share1:
        st.markdown(f"<a href='https://twitter.com/intent/tweet?text={encoded_text}' target='_blank' class='share-btn'>𝕏 Compartir en X</a>", unsafe_allow_html=True)
    with col_share2:
        st.markdown(f"<a href='https://wa.me/?text={encoded_text}' target='_blank' class='share-btn'>💬 WhatsApp</a>", unsafe_allow_html=True)
    with col_share3:
        st.markdown(f"<a href='https://www.facebook.com/sharer/sharer.php?quote={encoded_text}' target='_blank' class='share-btn'>📘 Facebook</a>", unsafe_allow_html=True)

    # ─────────────────────────────────────────────────────────────────────────
    # DISCLAIMER LEGAL
    # ─────────────────────────────────────────────────────────────────────────
    st.markdown("<div style='margin:20px 0 12px 0; border-top:1px solid rgba(51,65,85,0.3);'></div>", unsafe_allow_html=True)
    st.markdown("""
    <div class="footer-legal">
    <strong>DESCARGO DE RESPONSABILIDAD</strong><br>
    <em>Ojo del Pueblo</em> es una herramienta algorítmica de análisis de datos abiertos y transparencia cívica. Las alertas, anomalías y cruces presentados son resultado de <strong>análisis matemáticos automatizados</strong> sobre bases de datos públicas gubernamentales (Mercado Público, SERVEL, InfoLobby, InfoProbidad, Contraloría, DIPRES).
    <br><br>
    La presencia de alertas <strong>no constituye una imputación ni acusación formal</strong>. Todo análisis persigue fines investigativos, educativos y de control ciudadano. Prevalece la <strong>presunción de inocencia</strong> de cualquier persona o entidad. Si detecta datos erróneos, informe en la pestaña de Denuncias.
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()