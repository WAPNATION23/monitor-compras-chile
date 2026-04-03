"""
Dashboard interactivo para "Monitor Ciudadano de Compras Públicas".
Construido con Streamlit.
"""

import sqlite3
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# ─────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN DE PÁGINA Y ESTILOS
# ─────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Monitor Ciudadano | Centro de Mando",
    page_icon="👁️‍🗨️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Inyectar CSS personalizado para look "Centro de Monitoreo" (Dark/Neon)
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');

    /* Fondo principal y tipografía */
    .stApp {
        background-color: #0A0E17;
        color: #C9D1D9;
        font-family: 'Inter', sans-serif;
    }
    
    /* Bloques y cards */
    .css-1r6slb0, .css-12oz5g7, div[data-testid="stSidebar"] {
        background-color: #111822 !important;
        border-right: 1px solid #1E293B;
    }
    
    /* Títulos elegantes */
    h1, h2, h3, h4 {
        color: #F8FAFC !important;
        font-family: 'Inter', sans-serif;
        font-weight: 600;
        letter-spacing: -0.5px;
    }
    h1 {
        border-bottom: 2px solid transparent;
        border-image: linear-gradient(to right, #00D2FF, #3A7BD5);
        border-image-slice: 1;
        padding-bottom: 12px;
        margin-bottom: 24px;
        font-size: 2.2rem;
        text-transform: uppercase;
        letter-spacing: 2px;
    }
    
    /* Métricas (KPIs) - Estilo Cyberpunk Corporativo */
    [data-testid="stMetricValue"] {
        font-size: 2.5rem;
        font-weight: 800;
        color: #00D2FF !important;
        font-family: 'Inter', monospace;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.9rem;
        color: #94A3B8 !important;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        font-weight: 600;
        margin-bottom: 5px;
    }
    [data-testid="stMetricDelta"] {
        font-size: 0.9rem;
    }
    
    /* Botones y enlaces */
    .btn-portal {
        display: inline-block;
        padding: 0.6em 1.2em;
        color: #ffffff !important;
        background: linear-gradient(135deg, #FF3366, #E6003E);
        border-radius: 4px;
        text-decoration: none;
        font-weight: 600;
        text-align: center;
        width: 100%;
        margin-top: 10px;
        margin-bottom: 20px;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
        text-transform: uppercase;
        letter-spacing: 1px;
        font-size: 0.85rem;
    }
    .btn-portal:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(255, 51, 102, 0.4);
    }
    
    /* Pestañas (Tabs) */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: transparent;
        padding-bottom: 5px;
        border-bottom: 1px solid #1E293B;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #111822;
        border-radius: 6px 6px 0 0;
        border: 1px solid #1E293B;
        border-bottom: none;
        padding: 10px 24px;
        transition: all 0.3s ease;
        color: #94A3B8;
        font-weight: 600;
    }
    .stTabs [aria-selected="true"] {
        background-color: #0A0E17;
        color: #00D2FF !important;
        border-top: 3px solid #00D2FF;
    }
    
    /* Inputs y Formularios */
    .stTextInput input, .stSelectbox div[data-baseweb="select"] {
        background-color: #0A0E17 !important;
        border: 1px solid #334155 !important;
        color: #E2E8F0 !important;
        border-radius: 6px;
    }
    .stTextInput input:focus, .stSelectbox div[data-baseweb="select"]:focus-within {
        border-color: #00D2FF !important;
        box-shadow: 0 0 0 1px #00D2FF !important;
    }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────
# CONFIGURACIONES Y CONSTANTES
# ─────────────────────────────────────────────────────────────────────────
DB_PATH = "auditoria_estado.db"

EMOJIS_RIESGO = {
    "MUNICIPALIDAD": "🏛️",
    "FUERZAS ARMADAS/ORDEN": "🚓",
    "ALERTA FUNDACIONES/TRATO DIRECTO": "🚨💰",
    "MOP/OBRAS": "🚧",
    "GENERAL": "📄"
}

# Paleta Cyberpunk para gráficos
COLOR_DISCRETE_MAP = {
    "MUNICIPALIDAD": "#00f3ff",
    "FUERZAS ARMADAS/ORDEN": "#00ff66",
    "ALERTA FUNDACIONES/TRATO DIRECTO": "#ff003c",
    "MOP/OBRAS": "#fcee0a",
    "GENERAL": "#8a2be2"
}

# ─────────────────────────────────────────────────────────────────────────
# LÓGICA DE DATOS
# ─────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_data():
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ordenes_items';")
            if not cursor.fetchone():
                return pd.DataFrame()
            
            query = """
                SELECT 
                    codigo_oc, nombre_producto, cantidad, precio_unitario, 
                    monto_total_item, nombre_comprador, nombre_proveedor, 
                    fecha_creacion, estado,
                    IFNULL(tipo_oc, '') as tipo_oc,
                    IFNULL(categoria_riesgo, 'GENERAL') as categoria_riesgo
                FROM ordenes_items
                WHERE precio_unitario > 0
                  AND estado != '9'
            """
            df = pd.read_sql_query(query, conn)
            if not df.empty:
                df['fecha_creacion'] = pd.to_datetime(df['fecha_creacion'], errors='coerce')
                df['precio_unitario'] = pd.to_numeric(df['precio_unitario'], errors='coerce')
                df['monto_total_item'] = pd.to_numeric(df['monto_total_item'], errors='coerce')
                df['cantidad'] = pd.to_numeric(df['cantidad'], errors='coerce')
            return df
    except sqlite3.Error:
        return pd.DataFrame()

def init_feedback_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS feedback_comunidad (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tipo_reporte TEXT,
                dato_reportado TEXT,
                comentario TEXT,
                fecha_reporte TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

def save_feedback(tipo: str, dato: str, comentario: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO feedback_comunidad (tipo_reporte, dato_reportado, comentario) VALUES (?, ?, ?)",
            (tipo, dato, comentario)
        )
        conn.commit()

def format_clp(value):
    return f"${value:,.0f}".replace(",", ".")

# ─────────────────────────────────────────────────────────────────────────
# INTERFAZ: CENTRO DE MONITOREO
# ─────────────────────────────────────────────────────────────────────────
def main():
    init_feedback_db()
    
    # ENCABEZADO PRINCIPAL (Compacto)
    col_t1, col_t2 = st.columns([0.8, 0.2])
    with col_t1:
        st.title("👁️‍🗨️ CENTRO DE MANDO: Monitor de Compras Públicas")
        st.markdown("*Sistema integral de auditoría algorítmica y forense ciudadana.*")
    with col_t2:
        st.caption(f"🕒 Estado: **ACTIVO** | Sinc: {datetime.now().strftime('%H:%M:%S')}")

    # CARGA DE DATOS
    df = load_data()

    if df.empty:
        st.error("📡 ENLACE CAÍDO: Base de datos vacía o no inicializada.")
        st.info("Ejecuta `python main.py` en la terminal para recargar los databanks.")
        return

    # ─────────────────────────────────────────────────────────────────────────
    # PANEL DE CONTROL LATERAL (FILTROS FORENSES)
    # ─────────────────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("### 🔍 MOTOR DE BÚSQUEDA GLOBAL")
        filtro_global = st.text_input("Ingrese RUT, Nombre, Empresa, o Boletín...", placeholder="Ej: 76.111.000-1")
        
        st.markdown("---")
        st.markdown("### 🎛️ FILTROS TÁCTICOS")

        st.markdown("**Radar Geográfico / Político**")
        radar_antofagasta = st.checkbox("🎯 Vigilar Ecosistema Antofagasta")
        radar_region = st.text_input("📍 Filtro de Territorio (ej. Biobío):", "")
        
        st.markdown("**Radar Operacional**")
        categorias_disp = df['categoria_riesgo'].dropna().unique().tolist()
        filtro_cat = st.multiselect("⚠️ Amenaza Identificada", options=categorias_disp, default=[])
        
        min_monto = float(df['monto_total_item'].min())
        max_monto = float(df['monto_total_item'].max())
        
        st.markdown("**Umbral Financiero Monitoreado ($CLP)**")
        filtro_rango_monto = st.slider(
            "",
            min_value=min_monto, max_value=max_monto, 
            value=(min_monto, max_monto), format="$%d"
        )
        
        st.markdown("---")
        st.caption("Arquitectura Forense by Antigravity AI. 🕵️‍♂️")

    # APLICAR FILTROS
    df_filtrado = df.copy()

    if filtro_global:
        query = filtro_global.lower()
        df_filtrado = df_filtrado[
            df_filtrado['nombre_comprador'].str.lower().str.contains(query, na=False) |
            df_filtrado['nombre_proveedor'].str.lower().str.contains(query, na=False) |
            df_filtrado['codigo_oc'].str.lower().str.contains(query, na=False)
        ]

    if radar_antofagasta:
        df_filtrado = df_filtrado[df_filtrado['nombre_comprador'].str.lower().str.contains("antofagasta", na=False)]
    if radar_region:
        df_filtrado = df_filtrado[df_filtrado['nombre_comprador'].str.lower().str.contains(radar_region.lower(), na=False)]
    if filtro_cat:
        df_filtrado = df_filtrado[df_filtrado['categoria_riesgo'].isin(filtro_cat)]
        
    if min_monto != max_monto:
        df_filtrado = df_filtrado[
            (df_filtrado['monto_total_item'] >= filtro_rango_monto[0]) & 
            (df_filtrado['monto_total_item'] <= filtro_rango_monto[1])
        ]

    # ─────────────────────────────────────────────────────────────────────────
    # ENRUTAMIENTO POR PESTAÑAS (Limpieza Visual)
    # ─────────────────────────────────────────────────────────────────────────
    tab_estadisticas, tab_cruce, tab_registro, tab_medios, tab_analistas = st.tabs([
        "📊 Panel Táctico (Análisis)", 
        "🔍 Cruce de Datos Forenses",
        "📋 Registro Forense (Base de Datos)", 
        "👁️ El Ojo del Pueblo (Señales en Vivo)",
        "🕵️‍♂️ Inteligencia y Denuncias"
    ])

    # ══════════════════════════════════════════════════════════════════════════
    # PESTAÑA 1: PANEL TÁCTICO
    # ══════════════════════════════════════════════════════════════════════════
    with tab_estadisticas:
        # KPIs Superiores
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        
        total_gasto = df_filtrado['monto_total_item'].sum() if not df_filtrado.empty else 0
        total_oc = df_filtrado['codigo_oc'].nunique() if not df_filtrado.empty else 0
        max_unitario = df_filtrado['precio_unitario'].max() if not df_filtrado.empty else 0
        total_proveedores = df_filtrado['nombre_proveedor'].nunique() if not df_filtrado.empty else 0
        
        with kpi1:
            st.metric("Volumen Fondeado", format_clp(total_gasto), delta="Fiscalización requerida", delta_color="inverse")
        with kpi2:
            st.metric("Total de OC", f"{total_oc:,}", delta="-", delta_color="off")
        with kpi3:
            st.metric("Pico Precio Unitario", format_clp(max_unitario), delta="Posible Sobreprecio", delta_color="inverse")
        with kpi4:
            st.metric("Recepciones (Emps)", f"{total_proveedores:,}", delta="Proveedores Únicos", delta_color="off")

        st.markdown("<br>", unsafe_allow_html=True)
        col_g1, col_g2 = st.columns(2)
        
        with col_g1:
            st.markdown("#### 🏆 Receptores Top (Concentración Capital)")
            if not df_filtrado.empty:
                top_prov = df_filtrado.groupby('nombre_proveedor')['monto_total_item'].sum().reset_index()
                top_prov = top_prov.nlargest(10, 'monto_total_item').sort_values('monto_total_item', ascending=True)
                
                fig_bar = px.bar(
                    top_prov, x='monto_total_item', y='nombre_proveedor', orientation='h',
                    labels={'monto_total_item': 'Total ($CLP)', 'nombre_proveedor': ''},
                    text_auto='.2s', template="plotly_dark",
                    color_discrete_sequence=["#ff3366"]
                )
                fig_bar.update_layout(margin=dict(l=0, r=0, t=10, b=0), font=dict(family="Inter", size=11))
                st.plotly_chart(fig_bar, use_container_width=True)
            else:
                st.info("Sin datos suficientes.")

        with col_g2:
            st.markdown("#### 🎯 Radar de Anomalías")
            if not df_filtrado.empty:
                mask = (df_filtrado['cantidad'] > 0) & (df_filtrado['precio_unitario'] > 0)
                df_scatter = df_filtrado[mask].copy()

                if not df_scatter.empty:
                    fig_scatter = px.scatter(
                        df_scatter, x='cantidad', y='precio_unitario', 
                        color='categoria_riesgo',
                        hover_data=['codigo_oc', 'nombre_producto', 'nombre_comprador'],
                        labels={'cantidad': 'Cantidad Sol.', 'precio_unitario': 'Precio Unit.'},
                        log_x=True, log_y=True,
                        template="plotly_dark",
                        color_discrete_map=COLOR_DISCRETE_MAP
                    )
                    fig_scatter.update_layout(margin=dict(l=0, r=0, t=10, b=0), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
                    st.plotly_chart(fig_scatter, use_container_width=True)
                else:
                    st.info("Sin datos para radar.")
            else:
                st.info("Sin datos.")

        st.markdown("#### 📈 Pulso Temporal de Transacciones (Flujo de Dinero)")
        if not df_filtrado.empty:
            df_time = df_filtrado.dropna(subset=['fecha_creacion']).copy()
            if not df_time.empty:
                gasto_diario = df_time.groupby([df_time['fecha_creacion'].dt.date, 'categoria_riesgo'])['monto_total_item'].sum().reset_index()
                fig_line = px.line(
                    gasto_diario, x='fecha_creacion', y='monto_total_item', color='categoria_riesgo',
                    template="plotly_dark", markers=True,
                    labels={'fecha_creacion': 'Fecha', 'monto_total_item': 'Total Día'},
                    color_discrete_map=COLOR_DISCRETE_MAP
                )
                fig_line.update_layout(height=280, margin=dict(l=0, r=0, t=10, b=0))
                st.plotly_chart(fig_line, use_container_width=True)
            else:
                st.info("No hay fechas válidas.")

    # ══════════════════════════════════════════════════════════════════════════
    # PESTAÑA 2: CRUCE DE DATOS FORENSES (CrossReferencer)
    # ══════════════════════════════════════════════════════════════════════════
    with tab_cruce:
        st.markdown("### 🔍 Motor de Inteligencia y Cruce de Datos")
        st.caption("Detección de patrones sistemáticos de corrupción usando el motor forense local.")
        
        try:
            from cross_referencer import CrossReferencer
            xref = CrossReferencer(DB_PATH)
            
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
                    df_sosp['monto_total'] = df_sosp['monto_total'].apply(format_clp)
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
                    df_riesgo['monto_total'] = df_riesgo['monto_total'].apply(format_clp)
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
                    df_td['monto_total'] = df_td['monto_total'].apply(format_clp)
                    df_td['monto_td'] = df_td['monto_td'].apply(format_clp)
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
                    df_conc['total_adjudicado'] = df_conc['total_adjudicado'].apply(format_clp)
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
                df_servel_cruzado['inversion_electoral'] = df_servel_cruzado['inversion_electoral'].apply(format_clp)
                df_servel_cruzado['retorno_licitaciones'] = df_servel_cruzado['retorno_licitaciones'].apply(format_clp)
                st.dataframe(df_servel_cruzado, hide_index=True, use_container_width=True)
            else:
                st.info("🛡️ Nivel 0 de corrupción detectado o matriz limpia bajo este filtro. (Asegúrate de haber procesado SERVEL).")
        
        except ImportError as e:
            st.error("Error: el módulo `cross_referencer.py` no está disponible o tiene errores.")

    # ══════════════════════════════════════════════════════════════════════════
    # PESTAÑA 3: REGISTRO FORENSE RAW
    # ══════════════════════════════════════════════════════════════════════════
    with tab_registro:
        st.markdown(f"### 📋 Base de Datos Auditada ({len(df_filtrado)} eventos)")
        
        if not df_filtrado.empty:
            mostrar_col = [
                'codigo_oc', 'categoria_riesgo', 'nombre_comprador', 
                'nombre_proveedor', 'nombre_producto', 'cantidad', 
                'precio_unitario', 'monto_total_item', 'fecha_creacion'
            ]
            dt_display = df_filtrado[mostrar_col].copy()
            if pd.api.types.is_datetime64_any_dtype(dt_display['fecha_creacion']):
                dt_display['fecha_creacion'] = dt_display['fecha_creacion'].dt.strftime('%Y-%m-%d')

            # Renderizado elegante utilizando st.column_config
            st.dataframe(
                dt_display,
                use_container_width=True,
                height=600,
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
                        "Monto Fondeado ($CLP)", 
                        format="$%f", 
                        min_value=0, 
                        max_value=max(dt_display['monto_total_item'].max(), 1)
                    ),
                    "fecha_creacion": st.column_config.DateColumn("Fecha Emisión")
                }
            )
        else:
            st.warning("No hay registros bajo las métricas del Radar Estratégico actual.")

    # ══════════════════════════════════════════════════════════════════════════
    # PESTAÑA 3: OJO DEL PUEBLO (MEDIOS Y TRANSMISIONES)
    # ══════════════════════════════════════════════════════════════════════════
    with tab_medios:
        st.markdown("### 👁️ Enlaces a Transmisiones Web Gratuitas")
        st.caption("Periodismo local oficial y portales gubernamentales en vivo para contrastar hallazgos.")
        
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
    # PESTAÑA 4: INTELIGENCIA CIVIL (REPORTES Y LOBBY)
    # ══════════════════════════════════════════════════════════════════════════
    with tab_analistas:
        st.markdown("### 🏛️ RADAR LEGISLATIVO (Alerta de Lobby)")
        st.warning("⚠️ Vigila leyes en curso que modifiquen normas de compras/subvenciones con nombre y apellido.")
        
        col_leg1, col_leg2, col_leg3 = st.columns(3)
        with col_leg1:
            st.info("📉 **Ley de Compras Públicas (Modificación)**\n\nBoletín: 14137-05 | **Rápida**\n\n[Ver Senado.cl](#)")
        with col_leg2:
            st.error("🚨 **Subvenciones Especiales Fundaciones**\n\nBoletín: 15423-01 | **M. Mixta**\n\n[Ver Senado.cl](#)")
        with col_leg3:
            st.warning("🚧 **Ley de Concesiones Viales MOP**\n\nBoletín: 16122-09 | **Prox. Votación**\n\n[Ver Camara.cl](#)")

        st.markdown("---")

        st.markdown("### 📡 TRANSMITIR INTELIGENCIA FORENSE AL BOT")
        st.info("Alimenta a la Inteligencia Artificial con patrones orgánicos (Nepotismo o Proyectos de Ley Fantasma) para que cruce bases de datos.")
        
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
            
            submit_btn = st.form_submit_button("🔥 Inyectar Alerta al Sistema Central")
            
            if submit_btn:
                if f_dato.strip() == "":
                    st.error("❌ Abortado: Falta el código OC, RUT o Dato Clave.")
                else:
                    save_feedback(f_tipo, f_dato, f_comentario)
                    st.success(f"✅ ¡Confirmado! La inteligencia sobre '{f_dato}' ha sido indexada para escrutinio algorítmico global.")

if __name__ == "__main__":
    main()
