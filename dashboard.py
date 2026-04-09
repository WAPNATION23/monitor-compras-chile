"""
Dashboard interactivo para "Ojo del Pueblo".
Construido con Streamlit.
"""

import logging
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
from chat_service import build_db_context, build_web_context, call_deepseek
from config import DAILY_QUERY_LIMIT, OC_TIPO_TRATO_DIRECTO
from alertas_personas import AlertasPersonas

logger = logging.getLogger(__name__)

_LOGO_PATH = "logo_ojo_pueblo.png"

# ─────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN DE PÁGINA Y ESTILOS
# ─────────────────────────────────────────────────────────────────────────

_CUSTOM_CSS: str = """
<style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&display=swap');
    .stApp {
        background-color: #05080E;
        background-image:
            linear-gradient(rgba(14, 21, 35, 0.4) 1px, transparent 1px),
            linear-gradient(90deg, rgba(14, 21, 35, 0.4) 1px, transparent 1px);
        background-size: 30px 30px;
        color: #D1D5DB;
        font-family: 'Space Grotesk', sans-serif;
    }
    .css-1r6slb0, .css-12oz5g7, div[data-testid="stSidebar"] {
        background-color: rgba(9, 13, 20, 0.95) !important;
        border-right: 1px solid #1E293B;
        backdrop-filter: blur(10px);
    }
    h1, h2, h3, h4 {
        color: #F8FAFC !important;
        font-family: 'Space Grotesk', sans-serif;
        font-weight: 600;
        letter-spacing: -0.02em;
    }
    h1 {
        border-bottom: 2px solid #2563EB;
        padding-bottom: 12px;
        margin-bottom: 30px;
        font-size: 2.2rem;
        text-shadow: 0px 0px 15px rgba(37, 99, 235, 0.2);
    }
    [data-testid="stMetricValue"] {
        font-size: 1.5rem; font-weight: 700; color: #FFFFFF !important;
        background: -webkit-linear-gradient(45deg, #60A5FA, #FFFFFF);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.75rem; color: #94A3B8 !important;
        text-transform: uppercase; letter-spacing: 0.05em;
    }
    div[data-testid="metric-container"] {
        background: rgba(15, 23, 42, 0.6); border: 1px solid #1E293B;
        padding: 12px 10px; border-radius: 8px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    }
    .stChatInput { border-color: #334155 !important; }
    .stDownloadButton > button {
        background-color: #1E293B !important; color: #94A3B8 !important;
        border: 1px solid #334155 !important; font-size: 0.8rem;
    }
    .stDownloadButton > button:hover {
        background-color: #334155 !important; color: #F8FAFC !important;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 4px; background-color: transparent; padding-bottom: 0px;
        border-bottom: 1px solid #334155; flex-wrap: wrap; overflow-x: visible !important;
    }
    .stTabs [data-baseweb="tab-list"] button[role="tab"] { flex: 1 1 auto; min-width: 0; }
    .stTabs [data-baseweb="tab"] {
        background-color: #0F172A; border: 1px solid #334155; border-bottom: none;
        padding: 8px 12px; transition: all 0.3s ease; color: #94A3B8;
        font-weight: 600; border-radius: 6px 6px 0 0;
        font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.03em; white-space: nowrap;
    }
    .stTabs [aria-selected="true"] {
        background-color: #1E293B !important; color: #38BDF8 !important;
        border-color: #38BDF8; border-width: 2px 2px 0px 2px;
    }
    .stTabs [data-baseweb="tab-list"] > div[role="presentation"] { display: none !important; }
    [data-testid="stDataFrame"] {
        border-radius: 8px; border: 1px solid #1E293B; overflow: hidden;
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1);
    }
    .stAlert { border-radius: 6px; border: 1px solid #7F1D1D; background: rgba(127, 29, 29, 0.1) !important; }
    blockquote {
        background: rgba(37, 99, 235, 0.1) !important;
        border-left: 3px solid #3B82F6 !important;
        padding: 12px 16px !important; border-radius: 0 8px 8px 0;
        color: #94A3B8 !important; font-size: 0.85rem; margin-bottom: 20px;
    }
    blockquote p { margin: 0 !important; }
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

# ─────────────────────────────────────────────────────────────────────────
# INTERFAZ: CENTRO DE MONITOREO
# ─────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def _cached_load():
    return load_data()

def main():
    init_feedback_db()

    # ENCABEZADO PRINCIPAL (Compacto)
    col_t1, col_t2 = st.columns([0.1, 0.9])
    with col_t1:
        if os.path.exists(_LOGO_PATH):
            st.image(_LOGO_PATH, use_container_width=True)
    with col_t2:
        st.title("Ojo del Pueblo")
        st.markdown("*Fiscalización ciudadana de compras del Estado de Chile en tiempo real.*")
        st.caption(f"Datos de Mercado Público (API ChileCompra) | Actualizado: {datetime.now().strftime('%d/%m/%Y %H:%M')}")

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
        st.error("Base de datos vacía o no inicializada.")
        st.info("Ejecuta `python main.py` en la terminal para recargar los datos.")
        return

    # ─────────────────────────────────────────────────────────────────────────
    # PANEL DE CONTROL LATERAL (FILTROS FORENSES)
    # ─────────────────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("### Búsqueda")
        filtro_global = st.text_input("RUT, nombre, empresa o código OC", placeholder="Ej: 76.111.000-1")

        st.markdown("---")
        st.markdown("### Filtros")

        st.markdown("**Zona geográfica**")
        radar_antofagasta = st.checkbox("Solo Antofagasta")
        radar_region = st.text_input("Otra región (ej. Biobío):", "")

        st.markdown("**Categoría de riesgo**")
        categorias_disp = df['categoria_riesgo'].dropna().unique().tolist()
        filtro_cat = st.multiselect("Seleccionar", options=categorias_disp, default=[])

        st.markdown("**Rango de fechas**")
        fechas_validas = df['fecha_creacion'].dropna()
        if not fechas_validas.empty:
            fecha_min = fechas_validas.min().date()
            fecha_max = fechas_validas.max().date()
            filtro_fecha = st.date_input(
                "Desde / Hasta",
                value=(fecha_min, fecha_max),
                min_value=fecha_min,
                max_value=fecha_max,
            )
        else:
            filtro_fecha = None

        st.markdown("---")

    # APLICAR FILTROS
    df_filtrado = df.copy()

    if filtro_global:
        query = filtro_global.lower()
        df_filtrado = df_filtrado[
            df_filtrado['nombre_comprador'].str.lower().str.contains(query, na=False) |
            df_filtrado['nombre_proveedor'].str.lower().str.contains(query, na=False) |
            df_filtrado['codigo_oc'].str.lower().str.contains(query, na=False) |
            df_filtrado['rut_proveedor'].str.lower().str.contains(query, na=False) |
            df_filtrado['rut_comprador'].str.lower().str.contains(query, na=False)
        ]

    if radar_antofagasta:
        df_filtrado = df_filtrado[df_filtrado['nombre_comprador'].str.lower().str.contains("antofagasta", na=False)]
    if radar_region:
        df_filtrado = df_filtrado[df_filtrado['nombre_comprador'].str.lower().str.contains(radar_region.lower(), na=False)]
    if filtro_cat:
        df_filtrado = df_filtrado[df_filtrado['categoria_riesgo'].isin(filtro_cat)]

    if filtro_fecha and isinstance(filtro_fecha, tuple) and len(filtro_fecha) == 2:
        desde, hasta = filtro_fecha
        ts_desde = pd.Timestamp(desde)
        ts_hasta = pd.Timestamp(hasta) + pd.Timedelta(days=1)
        mask_fecha = df_filtrado['fecha_creacion'].notna()
        df_filtrado = df_filtrado[
            ~mask_fecha |
            ((df_filtrado['fecha_creacion'] >= ts_desde) &
             (df_filtrado['fecha_creacion'] < ts_hasta))
        ]



    # ─────────────────────────────────────────────────────────────────────────
    # ENRUTAMIENTO POR PESTAÑAS (Limpieza Visual)
    # ─────────────────────────────────────────────────────────────────────────
    tab_estadisticas, tab_cruce, tab_registro, tab_medios, tab_mira, tab_analistas, tab_ia = st.tabs([
        "Panel General",
        "Cruces Forenses",
        "Datos Crudos",
        "Fuentes",
        "🔍 En la Mira",
        "Denuncias",
        "Asistente IA"
    ])

    # ══════════════════════════════════════════════════════════════════════════
    # PESTAÑA 1: PANEL TÁCTICO
    # ══════════════════════════════════════════════════════════════════════════
    with tab_estadisticas:
        # Explicación clara del panel
        st.markdown("""
        > **Qué estás viendo:** Este panel muestra las **órdenes de compra** emitidas por organismos del Estado de Chile,
        > obtenidas en tiempo real desde la API de Mercado Público (ChileCompra). El "Gasto Escaneado" es la suma total
        > de todas las órdenes de compra cargadas en esta plataforma. Usa los filtros del panel izquierdo para explorar.
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

        st.markdown("<br>", unsafe_allow_html=True)

        # Fila 1: Top proveedores + Gasto por organismo
        col_g1, col_g2 = st.columns(2)

        with col_g1:
            st.markdown("#### Quién recibe más plata del Estado")
            st.caption("Top 10 proveedores por monto total adjudicado")
            if not df_filtrado.empty:
                top_prov = df_filtrado.groupby('nombre_proveedor')['monto_total_item'].sum().reset_index()
                top_prov = top_prov.nlargest(10, 'monto_total_item').sort_values('monto_total_item', ascending=True)
                top_prov['monto_label'] = top_prov['monto_total_item'].apply(format_clp)

                fig_bar = px.bar(
                    top_prov, x='monto_total_item', y='nombre_proveedor', orientation='h',
                    labels={'monto_total_item': 'Total ($CLP)', 'nombre_proveedor': ''},
                    text='monto_label', template="plotly_dark",
                    color_discrete_sequence=["#ff3366"]
                )
                fig_bar.update_layout(margin={"l": 0, "r": 0, "t": 10, "b": 0}, font={"family": "Inter", "size": 11})
                fig_bar.update_traces(textposition='outside')
                st.plotly_chart(fig_bar, use_container_width=True)
            else:
                st.info("Sin datos suficientes.")

        with col_g2:
            st.markdown("#### Quién gasta más del Estado")
            st.caption("Top 10 organismos públicos por monto de compras")
            if not df_filtrado.empty:
                top_comp = df_filtrado.groupby('nombre_comprador')['monto_total_item'].sum().reset_index()
                top_comp = top_comp.nlargest(10, 'monto_total_item')

                fig_pie = px.pie(
                    top_comp, values='monto_total_item', names='nombre_comprador',
                    template="plotly_dark", hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Set2
                )
                fig_pie.update_traces(textposition='inside', textinfo='percent+label', textfont_size=10)
                fig_pie.update_layout(margin={"l": 0, "r": 0, "t": 10, "b": 0}, showlegend=False, height=400)
                st.plotly_chart(fig_pie, use_container_width=True)
            else:
                st.info("Sin datos.")

        # Fila 2: Tipo de compra + Timeline
        col_g3, col_g4 = st.columns(2)

        with col_g3:
            st.markdown("#### Cómo compran")
            st.caption("Tipo de orden: D1/C1 = Trato Directo, CM = Convenio Marco, AG = Compra Ágil, LP = Licitación")
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
                    color='monto', color_continuous_scale=['#1E293B', '#EF4444']
                )
                fig_tipo.update_layout(margin={"l": 0, "r": 0, "t": 10, "b": 0}, showlegend=False, coloraxis_showscale=False)
                fig_tipo.update_traces(textposition='outside')
                st.plotly_chart(fig_tipo, use_container_width=True)
            else:
                st.info("Sin datos.")

        with col_g4:
            st.markdown("#### Cuándo compran")
            st.caption("Evolución del gasto diario")
            if not df_filtrado.empty:
                df_time = df_filtrado.dropna(subset=['fecha_creacion']).copy()
                if not df_time.empty:
                    gasto_diario = df_time.groupby(df_time['fecha_creacion'].dt.date)['monto_total_item'].sum().reset_index()
                    gasto_diario.columns = ['fecha', 'monto']
                    fig_line = px.area(
                        gasto_diario, x='fecha', y='monto',
                        template="plotly_dark",
                        labels={'fecha': 'Fecha', 'monto': 'Gasto del día ($CLP)'},
                        color_discrete_sequence=['#3B82F6']
                    )
                    fig_line.update_layout(height=350, margin={"l": 0, "r": 0, "t": 10, "b": 0})
                    st.plotly_chart(fig_line, use_container_width=True)
                else:
                    st.info("No hay fechas válidas.")
            else:
                st.info("Sin datos.")

        # Fila 3: Alertas rápidas
        st.markdown("#### Alertas destacadas")
        if not df_filtrado.empty:
            top5 = df_filtrado.nlargest(5, 'monto_total_item')[['codigo_oc', 'nombre_proveedor', 'nombre_comprador', 'monto_total_item', 'tipo_oc']].copy()
            top5['monto_total_item'] = top5['monto_total_item'].apply(format_clp)
            top5.columns = ['Código OC', 'Proveedor', 'Organismo', 'Monto', 'Tipo']
            st.dataframe(top5, hide_index=True, use_container_width=True)

    # ══════════════════════════════════════════════════════════════════════════
    # PESTAÑA 2: CRUCE DE DATOS FORENSES (CrossReferencer)
    # ══════════════════════════════════════════════════════════════════════════
    with tab_cruce:
        st.markdown("### Análisis de Cruces y Riesgo")
        st.caption("Detección de patrones sistemáticos mediante cruces de bases de datos públicas.")

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
                st.markdown("#### 🚩 Ranking de Proveedores Sospechosos")
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
                else:
                    st.info("Sin anomalías encontradas bajo este filtro.")

                st.markdown("#### 🏢 Organismos de Mayor Riesgo")
                st.caption("Score basado en tratos directos excesivos y anomalías.")
                df_riesgo = xref.ranking_riesgo_organismos()

                if filtro_global:
                    q = filtro_global.lower()
                    df_riesgo = df_riesgo[df_riesgo['nombre_comprador'].str.lower().str.contains(q, na=False) |
                                          df_riesgo['rut_comprador'].str.lower().str.contains(q, na=False)]

                if not df_riesgo.empty:
                    df_riesgo['monto_total'] = df_riesgo['monto_total'].apply(format_clp_full)
                    st.dataframe(df_riesgo.head(10), hide_index=True, use_container_width=True)
                else:
                    st.info("Sin datos para analizar.")

            with c2:
                st.markdown("#### 🚨 Abuso de Trato Directo")
                st.caption("Organismos con mayor porcentaje de trato directo sobre total de OC.")
                df_td = xref.ratio_tratos_directos()

                if filtro_global:
                    q = filtro_global.lower()
                    df_td = df_td[df_td['nombre_comprador'].str.lower().str.contains(q, na=False) |
                                  df_td['rut_comprador'].str.lower().str.contains(q, na=False)]

                if not df_td.empty:
                    df_td['monto_total'] = df_td['monto_total'].apply(format_clp_full)
                    df_td['monto_td'] = df_td['monto_td'].apply(format_clp_full)
                    st.dataframe(df_td.head(10)[['nombre_comprador', 'ratio_td', 'n_trato_directo', 'n_total']], hide_index=True, use_container_width=True)
                else:
                    st.info("Sin datos bajo este filtro.")

                st.markdown("#### 💰 Concentración de Capital (Ley de Pareto)")
                st.caption("Proveedores que acumulan el mayor porcentaje del gasto público.")
                df_conc = xref.concentracion_capital(top_n=50)

                if filtro_global:
                    q = filtro_global.lower()
                    df_conc = df_conc[df_conc['nombre_proveedor'].str.lower().str.contains(q, na=False) |
                                      df_conc['rut_proveedor'].str.lower().str.contains(q, na=False)]

                if not df_conc.empty:
                    df_conc = df_conc.head(5)
                    df_conc['total_adjudicado'] = df_conc['total_adjudicado'].apply(format_clp_full)
                    st.dataframe(df_conc[['nombre_proveedor', 'total_adjudicado', 'pct_del_total']], hide_index=True, use_container_width=True)
                else:
                    st.info("Sin datos bajo este filtro.")

            st.markdown("---")
            st.markdown("### 🏛️ Cruce de Datos Políticos: Aportes SERVEL vs. Adjudicaciones")
            st.caption("🔍 Correlación detectada entre financistas de campañas políticas (Donantes) y empresas que ganan adjudicaciones u Obtienen Trato Directo.")
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
            st.markdown("#### 🏗️ Malla Societaria: Beneficiarios Finales")
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

        except (OSError, pd.errors.DatabaseError, sqlite3.Error) as e:
            logger.error("Error en cruces forenses: %s", e)
            st.error(f"Error cargando cruces forenses: {e}")
            st.info("Esto puede ocurrir si la base de datos está vacía o corrupta. Ejecuta `python main.py` para recargar datos.")

    # ══════════════════════════════════════════════════════════════════════════
    # PESTAÑA 3: REGISTRO FORENSE RAW
    # ══════════════════════════════════════════════════════════════════════════
    with tab_registro:
        st.markdown(f"### Registro Completo ({len(df_filtrado)} registros)")

        # Sub-tabs para OC y Licitaciones
        sub_oc, sub_lic = st.tabs(["Órdenes de Compra", "Licitaciones Públicas"])

        with sub_oc:
            if not df_filtrado.empty:
                mostrar_col = [
                    'codigo_oc', 'categoria_riesgo', 'nombre_comprador',
                    'nombre_proveedor', 'nombre_producto', 'cantidad',
                    'precio_unitario', 'monto_total_item', 'fecha_creacion'
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
                        "categoria_riesgo": st.column_config.TextColumn("Nivel Riesgo"),
                        "nombre_comprador": st.column_config.TextColumn("Entidad de Gobierno"),
                        "nombre_proveedor": st.column_config.TextColumn("Proveedor / Empresa"),
                        "nombre_producto": st.column_config.TextColumn("Ítem Adquirido", width="medium"),
                        "cantidad": st.column_config.NumberColumn("Ud.", format="%d"),
                        "precio_unitario": st.column_config.NumberColumn("Precio Ud.", format="$%d"),
                        "monto_total_item": st.column_config.ProgressColumn(
                            "Monto ($CLP)",
                            format="$%f",
                            min_value=0,
                            max_value=max(dt_display['monto_total_item'].max(), 1)
                        ),
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
    # ══════════════════════════════════════════════════════════════════════════
    with tab_medios:
        st.markdown("### Fuentes Oficiales y Medios")
        st.caption("Acceso directo a portales gubernamentales y periodismo de investigación para contrastar hallazgos.")

        col_m1, col_m2, col_m3 = st.columns(3)

        with col_m1:
            st.markdown("#### Sector Legislativo")
            st.info("**TV Senado (Tramitación)**\n\nDirecto desde la Sala y Comisiones.")
            st.markdown("<a href='https://tv.senado.cl/' target='_blank' class='btn-portal'>🔴 Ir a TV Senado (Vivo)</a>", unsafe_allow_html=True)

            st.info("**Cámara de Diputados**\n\nComisiones Investigadoras.")
            st.markdown("<a href='https://webtv.camara.cl/' target='_blank' class='btn-portal'>🔴 Ir a Cámara TV (Vivo)</a>", unsafe_allow_html=True)

        with col_m2:
            st.markdown("#### Noticieros y Alertas")
            st.info("**Antofagasta TV**\n\nFoco territorial del GORE y Munis (YouTube Live nativo).")
            st.markdown("<a href='https://www.youtube.com/@antofagastatv30/live' target='_blank' class='btn-portal'>🔴 Ir a ATV (Caso Convenios)</a>", unsafe_allow_html=True)

            st.info("**TVN 24 Horas**\n\nCobertura Nacional Abierta.")
            st.markdown("<a href='https://www.24horas.cl/envivo' target='_blank' class='btn-portal'>🔴 Ir a 24H Central</a>", unsafe_allow_html=True)

        with col_m3:
            st.markdown("#### Periodismo Forense")
            st.info("**CIPER Chile**\n\nCentro de Investigación Periodística.")
            st.markdown("<a href='https://www.ciperchile.cl/' target='_blank' class='btn-portal'>📰 Leer CIPER Reportajes</a>", unsafe_allow_html=True)

            st.info("**BioBio TV**\n\nAlertas preventivas por radio y TV.")
            st.markdown("<a href='https://www.biobiochile.cl/bbtv' target='_blank' class='btn-portal'>📡 Sintonizar BioBio TV</a>", unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════════
    # PESTAÑA 5: EN LA MIRA — Alertas de Personas de Interés Público
    # ══════════════════════════════════════════════════════════════════════════
    with tab_mira:
        st.markdown("### 🔍 Personas en la Mira")
        st.caption(
            "Búsqueda en fuentes oficiales del Estado de Chile: InfoLobby, datos.gob.cl, "
            "Contraloría, SERVEL y Mercado Público. Solo datos reales y verificables."
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
                    "LOBBY": "#0078d4",
                    "SANCIÓN": "#d13438",
                    "DICTAMEN": "#8764b8",
                    "COMPRA_PUBLICA": "#107c10",
                    "APORTE_ELECTORAL": "#ff8c00",
                }

                for alerta in alertas:
                    bcolor = border_colors.get(alerta["tipo_alerta"], "#666666")
                    fecha_display = alerta["fecha"] if alerta["fecha"] != "sin fecha" else "—"
                    url_link = (
                        f"<a href='{alerta['url']}' target='_blank' "
                        f"style='color:{bcolor};text-decoration:none;'>Ver fuente ↗</a>"
                        if alerta["url"] else ""
                    )
                    card_html = f"""
                    <div style="
                        border-left: 4px solid {bcolor};
                        background: #1e1e1e;
                        padding: 12px 16px;
                        margin-bottom: 8px;
                        border-radius: 4px;
                    ">
                        <div style="display:flex;justify-content:space-between;align-items:center;">
                            <span style="color:{bcolor};font-weight:600;font-size:0.85em;">
                                {alerta['tipo_alerta']}
                            </span>
                            <span style="color:#888;font-size:0.8em;">{fecha_display}</span>
                        </div>
                        <div style="color:#ccc;font-size:0.8em;margin-top:2px;">
                            {alerta['fuente']}
                        </div>
                        <div style="color:#eee;margin-top:6px;font-size:0.9em;">
                            {alerta['descripcion']}
                        </div>
                        <div style="margin-top:6px;">{url_link}</div>
                    </div>
                    """
                    st.markdown(card_html, unsafe_allow_html=True)

        elif mira_buscar:
            st.warning("Ingresa un nombre para buscar.")

    # ══════════════════════════════════════════════════════════════════════════
    # PESTAÑA 6: INTELIGENCIA CIVIL (REPORTES Y LOBBY)
    # ══════════════════════════════════════════════════════════════════════════
    with tab_analistas:
        st.markdown("### Radar Legislativo Oficial")
        st.warning("Monitoreo de proyectos de ley que modifiquen normas de compras o subvenciones.")

        col_leg1, col_leg2, col_leg3 = st.columns(3)
        with col_leg1:
            st.info("📉 **Ley de Compras Públicas**\n\nRevisa el estado de proyectos de ley relacionados.\n\n[Buscar en Senado.cl](https://www.senado.cl/appsenado/templates/tramitacion/index.html)")
        with col_leg2:
            st.error("🚨 **Subvenciones y Transferencias**\n\nMonitorea cambios normativos en subvenciones públicas.\n\n[Buscar en Cámara](https://www.camara.cl/legislacion/ProyectosDeLey/proyectos_702702702702702ley.aspx)")
        with col_leg3:
            st.warning("🚧 **Transparencia y Probidad**\n\nSigue los proyectos de ley sobre transparencia.\n\n[Buscar en BCN](https://www.bcn.cl/leychile)")

        st.markdown("---")

        st.markdown("### Reportar un Hallazgo")
        st.info("Aporta datos de posibles irregularidades para que el sistema los cruce con las bases de datos públicas.")

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

            f_comentario = st.text_area("Análisis Forense (Descripción del modus operandi / Impacto):", height=120)

            submit_btn = st.form_submit_button("📨 Enviar Reporte")

            if submit_btn:
                if f_dato.strip() == "":
                    st.error("❌ Debes incluir al menos un dato clave (OC, RUT o nombre).")
                else:
                    save_feedback(f_tipo, f_dato, f_comentario)
                    st.success(f"✅ Reporte sobre '{f_dato}' registrado correctamente.")

    # ══════════════════════════════════════════════════════════════════════════
    # PESTAÑA 6: ASISTENTE IA (CHATBOT FORENSE)
    # ══════════════════════════════════════════════════════════════════════════
    with tab_ia:
        st.markdown("### Asistente de Investigación")
        st.caption("Consulta perfiles de políticos, empresas o fundaciones. El asistente busca en la web y cruza con la base de datos.")

        if "ia_messages" not in st.session_state:
            st.session_state.ia_messages = [
                {"role": "assistant", "content": "Sistema listo. Puedo investigar perfiles de empresas, políticos o instituciones cruzando datos públicos. ¿Qué necesitas analizar?"}
            ]

        for message in st.session_state.ia_messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

        if "api_calls" not in st.session_state:
            st.session_state.api_calls = 0

        # Rate limit por IP: 20 consultas por día
        try:
            headers = st.context.headers
            user_ip = headers.get("X-Forwarded-For", headers.get("Host", "local")).split(",")[0].strip()
        except (AttributeError, KeyError):
            user_ip = "local"

        today_str = datetime.now().strftime("%Y-%m-%d")
        daily_used = get_rate_limit_usage(user_ip, today_str)

        daily_limit = DAILY_QUERY_LIMIT
        remaining = max(0, daily_limit - daily_used)
        st.caption(f"Consultas restantes hoy: {remaining}/{daily_limit}")

        if prompt := st.chat_input("¿Qué quieres investigar?"):
            if remaining <= 0:
                st.error(f"🛑 Límite diario alcanzado ({daily_limit} consultas por día). Vuelve mañana.")
            else:
                st.session_state.api_calls += 1
                increment_rate_limit_usage(user_ip, today_str)
                st.chat_message("user").markdown(prompt)
                st.session_state.ia_messages.append({"role": "user", "content": prompt})

                with st.chat_message("assistant"):
                    with st.spinner("Procesando consulta..."):
                        api_key = os.getenv("DEEPSEEK_API_KEY", "")
                        if not api_key:
                            revelacion = "⚠️ Asistente IA no disponible: falta la clave DEEPSEEK_API_KEY en el archivo .env."
                            st.warning(revelacion)
                        else:
                            try:
                                # 0. Inteligencia local (DB + API)
                                st.toast("Escaneando base de datos local...", icon="🔍")
                                db_context = build_db_context(prompt)

                                # 1. Búsqueda web
                                st.toast("Buscando información actualizada en la web...", icon="🌐")
                                web_context = build_web_context(prompt)

                                # 2. Llamar a DeepSeek
                                revelacion = call_deepseek(
                                    st.session_state.ia_messages, web_context, db_context
                                )
                            except (requests.RequestException, KeyError, ValueError) as ia_exc:
                                revelacion = f"Error al consultar el asistente IA: {ia_exc}"
                                logger.error("Error en chat IA: %s", ia_exc)
                                st.error(revelacion)

                        # Interceptar comando de infiltración
                        infil_match = re.search(r"\[EJECUTAR_INFILTRACION:\s*([\d\.\-Kk]+)\]", revelacion)

                        st.markdown(revelacion.replace(infil_match.group(0) if infil_match else "", ""))

                st.session_state.ia_messages.append({"role": "assistant", "content": revelacion})

                if infil_match:
                    rut_detectado = infil_match.group(1)
                    st.warning(f"⚡ DESCARGA AUTOMÁTICA DE HISTORIAL PARA RUT: {rut_detectado}")
                    with st.spinner("Consultando registros públicos de Mercado Público vía API..."):
                        from infiltrador_ia import infiltrar_rut
                        target_rut = rut_detectado.replace(".", "").strip()
                        # Validar formato RUT estricto: 7-8 dígitos, guión, dígito verificador
                        if re.fullmatch(r"\d{7,8}-[\dkK]", target_rut):
                            infiltrar_rut(target_rut)
                            st.success("✅ Descarga histórica exitosa en la DB local. Presiona F5 para cargar los radares.")
                            st.session_state.ia_messages.append({
                                "role": "system",
                                "content": f"SISTEMA: La infiltración para {rut_detectado} ha inyectado con éxito su historial a la base de datos SQL. Ahora el tablero de estadísticas detectará esta información."
                            })
                            st.rerun()

    # ─────────────────────────────────────────────────────────────────────────
    # COMPARTIR EN REDES SOCIALES
    # ─────────────────────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Compartir hallazgos")

    share_text = f"Encontré datos interesantes en Ojo del Pueblo: {total_oc:,} órdenes de compra por {format_clp(total_gasto)} bajo fiscalización ciudadana."
    encoded_text = urllib.parse.quote(share_text)

    col_share1, col_share2, col_share3 = st.columns(3)
    with col_share1:
        st.markdown(f"[X / Twitter](https://twitter.com/intent/tweet?text={encoded_text})", unsafe_allow_html=True)
    with col_share2:
        st.markdown(f"[WhatsApp](https://wa.me/?text={encoded_text})", unsafe_allow_html=True)
    with col_share3:
        st.markdown(f"[Facebook](https://www.facebook.com/sharer/sharer.php?quote={encoded_text})", unsafe_allow_html=True)

    # ─────────────────────────────────────────────────────────────────────────
    # DISCLAIMER LEGAL Y FOOTER (Protección Anti-Demandas)
    # ─────────────────────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("""
    <div style="font-size: 0.8rem; color: #6B7280; text-align: justify; border: 1px solid #374151; padding: 15px; border-radius: 5px; background-color: #111827;">
    <strong>ESTATUTO LEGAL Y DESCARGO DE RESPONSABILIDAD</strong><br>
    <em>Ojo del Pueblo</em> es una herramienta algorítmica de análisis de datos abiertos y transparencia cívica. Las alertas rojas, anomalías y cruces societarios presentados en este panel son el resultado de <strong>análisis matemáticos automatizados y algoritmos estadísticos</strong> desarrollados sobre bases de datos públicas gubernamentales (Mercado Público, SERVEL, InfoLobby).
    <br><br>
    La presencia de alertas de concentración de capital, horas inusuales o tratos directos <strong>NO constituyen una imputación ni una acusación formal de delitos</strong> (tales como fraude al fisco, colusión o cohecho). Todo análisis mostrado aquí persigue fines investigativos, educativos y de control ciudadano. Prevalece la <strong>presunción de inocencia</strong> de cualquier empresa, fundación o persona natural expuesta. Si usted observa un dato potencialmente difamatorio o erróneo en la API original del Estado, informe en la pestaña 'Inteligencia y Denuncias'.
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
