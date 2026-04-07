"""
CrossReferencer — Motor de Cruce de Datos Forenses
════════════════════════════════════════════════════
Conecta TODAS las fuentes de datos para detectar patrones de corrupción
que solo son visibles al cruzar información entre sistemas.

Cruces implementados:
  1. Lobby → Compras: ¿Empresa se reunió con autoridad antes de ganar licitación?
  2. Proveedor → Multi-organismo: ¿Proveedor gana en muchas comunas distintas?
  3. Concentración: ¿Pocos proveedores acaparan % desproporcionado del gasto?
  4. Trato Directo vs Licitación: Ratio de tratos directos por organismo
  5. Ranking de riesgo por organismo

Uso:
    from cross_referencer import CrossReferencer
    xref = CrossReferencer()
    
    # Top proveedores sospechosos
    ranking = xref.ranking_proveedores_sospechosos()
    
    # Organismos con más tratos directos
    directos = xref.ratio_tratos_directos()
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd
import numpy as np

from config import DB_NAME, OC_TIPO_TRATO_DIRECTO

logger = logging.getLogger(__name__)


class CrossReferencer:
    """Motor de cruce de datos forenses entre múltiples fuentes."""

    def __init__(self, db_path: str | Path = DB_NAME) -> None:
        self.db_path = Path(db_path)

    def _load_all(self) -> pd.DataFrame:
        """Carga toda la tabla ordenes_items."""
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query(
                """
                SELECT * FROM ordenes_items 
                WHERE precio_unitario > 0 AND estado != '9'
                """,
                conn,
            )
        if not df.empty:
            df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion"], errors="coerce")
        return df

    # ═══════════ CRUCE 1: Concentración de Capital ═══════════ #

    def concentracion_capital(self, top_n: int = 20) -> pd.DataFrame:
        """
        Analiza la concentración de gasto público en pocos proveedores.
        
        Un indicador clásico de corrupción es cuando el 80% del gasto
        se concentra en el 20% de los proveedores (o peor).

        Returns:
            DataFrame con:
            - rut_proveedor, nombre_proveedor
            - total_adjudicado, n_ordenes, n_organismos_distintos
            - pct_del_total (porcentaje del gasto total)
            - pct_acumulado (Pareto)
        """
        df = self._load_all()
        if df.empty:
            return pd.DataFrame()

        gasto_total = df["monto_total_item"].sum()

        agg = df.groupby(["rut_proveedor", "nombre_proveedor"]).agg(
            total_adjudicado=("monto_total_item", "sum"),
            n_ordenes=("codigo_oc", "nunique"),
            n_organismos=("rut_comprador", "nunique"),
            n_items=("nombre_producto", "count"),
            categorias_unicas=("categoria", "nunique"),
        ).reset_index()

        agg = agg.sort_values("total_adjudicado", ascending=False).head(top_n)
        agg["pct_del_total"] = (agg["total_adjudicado"] / gasto_total * 100).round(2)
        agg["pct_acumulado"] = agg["pct_del_total"].cumsum().round(2)

        logger.info(
            "Top %d proveedores concentran %.1f%% del gasto total.",
            top_n, agg["pct_acumulado"].iloc[-1] if not agg.empty else 0,
        )
        return agg

    # ═══════════ CRUCE 2: Ratio de Tratos Directos ═══════════ #

    def ratio_tratos_directos(self) -> pd.DataFrame:
        """
        Calcula el ratio de Tratos Directos vs Licitaciones/CM por organismo.
        
        Un ratio alto de tratos directos puede indicar:
        - Evasión sistemática de licitaciones
        - Uso abusivo de excepciones legales
        - Favoritismo hacia proveedores específicos

        Returns:
            DataFrame con:
            - rut_comprador, nombre_comprador
            - n_total, n_trato_directo, ratio_td
            - monto_td, monto_total, pct_monto_td
        """
        df = self._load_all()
        if df.empty or "tipo_oc" not in df.columns:
            return pd.DataFrame()

        # Marcar tratos directos
        df["es_trato_directo"] = df["tipo_oc"].isin(OC_TIPO_TRATO_DIRECTO)

        agg = df.groupby(["rut_comprador", "nombre_comprador"]).agg(
            n_total=("codigo_oc", "nunique"),
            monto_total=("monto_total_item", "sum"),
        ).reset_index()

        # Contar OC únicas (no ítems) que son trato directo
        td_oc_counts = (
            df[df["es_trato_directo"]]
            .groupby(["rut_comprador", "nombre_comprador"])["codigo_oc"]
            .nunique()
            .reset_index()
            .rename(columns={"codigo_oc": "n_trato_directo"})
        )
        agg = agg.merge(td_oc_counts, on=["rut_comprador", "nombre_comprador"], how="left")
        agg["n_trato_directo"] = agg["n_trato_directo"].fillna(0).astype(int)

        # Calcular monto de tratos directos
        td_montos = df[df["es_trato_directo"]].groupby("rut_comprador").agg(
            monto_td=("monto_total_item", "sum"),
        ).reset_index()

        agg = agg.merge(td_montos, on="rut_comprador", how="left")
        agg["monto_td"] = agg["monto_td"].fillna(0)
        agg["ratio_td"] = (agg["n_trato_directo"] / agg["n_total"] * 100).round(1)
        agg["pct_monto_td"] = (agg["monto_td"] / agg["monto_total"] * 100).round(1)

        # Ordenar por ratio más alto
        agg = agg.sort_values("ratio_td", ascending=False)

        return agg

    # ═══════════ CRUCE 3: Proveedores Multi-Organismo ═══════════ #

    def proveedores_multi_organismo(self, min_organismos: int = 3) -> pd.DataFrame:
        """
        Identifica proveedores que ganan contratos en muchos organismos distintos.
        
        Un proveedor que opera en 10+ organismos diferentes puede ser:
        - Legítimo (empresa grande con capacidad real)
        - Sospechoso (empresa de papel con conexiones políticas)
        
        La señal de sospecha aumenta si:
        - Las categorías de productos son muy diversas
        - Los montos son altos y redondos
        - Hay tratos directos predominantes

        Returns:
            DataFrame con proveedores y sus estadísticas multi-organismo.
        """
        df = self._load_all()
        if df.empty:
            return pd.DataFrame()

        agg = df.groupby(["rut_proveedor", "nombre_proveedor"]).agg(
            n_organismos=("rut_comprador", "nunique"),
            organismos_lista=("nombre_comprador", lambda x: ", ".join(str(v) for v in x.dropna().unique()[:5])),
            n_categorias=("categoria", "nunique"),
            total_adjudicado=("monto_total_item", "sum"),
            n_ordenes=("codigo_oc", "nunique"),
        ).reset_index()

        # Filtrar por mínimo de organismos
        agg = agg[agg["n_organismos"] >= min_organismos]
        agg = agg.sort_values("n_organismos", ascending=False)

        return agg

    # ═══════════ CRUCE 4: Ranking de Riesgo por Organismo ═══════════ #

    def ranking_riesgo_organismos(self) -> pd.DataFrame:
        """
        Genera un ranking de organismos públicos según indicadores de riesgo.
        
        Score basado en:
        - % de tratos directos (peso: 30%)
        - Concentración en pocos proveedores - HHI (peso: 25%)
        - Compras en horario no hábil (peso: 20%)
        - Montos redondos sospechosos (peso: 15%)
        - Categorías atípicas de proveedores (peso: 10%)

        Returns:
            DataFrame con score de riesgo por organismo.
        """
        df = self._load_all()
        if df.empty:
            return pd.DataFrame()

        organismos = df.groupby(["rut_comprador", "nombre_comprador"]).agg(
            n_ordenes=("codigo_oc", "nunique"),
            monto_total=("monto_total_item", "sum"),
            n_proveedores=("rut_proveedor", "nunique"),
        ).reset_index()

        # Solo organismos con suficientes datos
        organismos = organismos[organismos["n_ordenes"] >= 3]

        scores = []
        for _, org in organismos.iterrows():
            rut = org["rut_comprador"]
            org_data = df[df["rut_comprador"] == rut]
            score = 0.0

            # 1. Ratio de tratos directos (30%)
            if "tipo_oc" in org_data.columns:
                n_ocs = org_data["codigo_oc"].nunique()
                n_td = org_data[org_data["tipo_oc"].isin(OC_TIPO_TRATO_DIRECTO)]["codigo_oc"].nunique()
                ratio_td = n_td / n_ocs if n_ocs > 0 else 0
                score += ratio_td * 30

            # 2. Concentración HHI (25%)
            if org["n_proveedores"] > 0:
                montos_prov = org_data.groupby("rut_proveedor")["monto_total_item"].sum()
                shares = montos_prov / montos_prov.sum()
                hhi = (shares ** 2).sum()  # 0-1, donde 1 = monopolio
                score += hhi * 25

            # 3. Compras fuera de horario (20%)
            valid_dates = org_data.dropna(subset=["fecha_creacion"])
            if not valid_dates.empty:
                weekend = valid_dates["fecha_creacion"].dt.dayofweek >= 5
                night = (valid_dates["fecha_creacion"].dt.hour < 8) | (valid_dates["fecha_creacion"].dt.hour >= 20)
                pct_off = (weekend | night).mean()
                score += pct_off * 20

            # 4. Montos redondos (15%)
            big = org_data[org_data["monto_total_item"] >= 1_000_000]
            if len(big) > 0:
                redondos = (big["monto_total_item"] % 1_000_000 == 0).mean()
                score += redondos * 15

            # 5. Proveedores multigiro (10%)
            cats_por_prov = org_data.groupby("rut_proveedor")["categoria"].nunique()
            if not cats_por_prov.empty:
                pct_multigiro = (cats_por_prov >= 3).mean()
                score += pct_multigiro * 10

            scores.append({
                "rut_comprador": rut,
                "nombre_comprador": org["nombre_comprador"],
                "n_ordenes": org["n_ordenes"],
                "monto_total": org["monto_total"],
                "n_proveedores": org["n_proveedores"],
                "score_riesgo": round(score, 1),
            })

        result = pd.DataFrame(scores)
        if not result.empty:
            result = result.sort_values("score_riesgo", ascending=False)

        return result

    # ═══════════ CRUCE 5: Proveedores Sospechosos (Score Compuesto) ═══════════ #

    def ranking_proveedores_sospechosos(self, top_n: int = 20) -> pd.DataFrame:
        """
        Genera un ranking de proveedores más sospechosos.
        
        Score basado en:
        - Dispersión de precios vs mediana del mercado (30%)
        - Cantidad de categorías distintas que vende (25%)
        - Concentración en pocos compradores (20%)
        - Predominancia de tratos directos (15%)
        - Montos redondos (10%)

        Returns:
            DataFrame con score de sospecha por proveedor.
        """
        df = self._load_all()
        if df.empty:
            return pd.DataFrame()

        proveedores = df.groupby(["rut_proveedor", "nombre_proveedor"]).agg(
            n_ordenes=("codigo_oc", "nunique"),
            monto_total=("monto_total_item", "sum"),
            n_categorias=("categoria", "nunique"),
            n_compradores=("rut_comprador", "nunique"),
        ).reset_index()

        # Solo proveedores con suficientes datos
        proveedores = proveedores[proveedores["n_ordenes"] >= 2]

        scores = []
        for _, prov in proveedores.iterrows():
            rut = prov["rut_proveedor"]
            prov_data = df[df["rut_proveedor"] == rut]
            score = 0.0

            # 1. Dispersión de precios (30%)
            for prod, group in prov_data.groupby("nombre_producto"):
                all_prices = df[df["nombre_producto"] == prod]["precio_unitario"]
                if len(all_prices) >= 3 and len(group) > 0:
                    mediana_mercado = all_prices.median()
                    if mediana_mercado > 0:
                        ratio = (group["precio_unitario"].mean() / mediana_mercado) - 1
                        score += min(ratio * 10, 30)  # Cap at 30
                        break

            # 2. Multigiro (25%)
            if prov["n_categorias"] >= 4:
                score += 25
            elif prov["n_categorias"] >= 3:
                score += 15

            # 3. Pocos compradores pero mucho dinero (20%)
            if prov["n_compradores"] <= 2 and prov["monto_total"] > 50_000_000:
                score += 20

            # 4. Tratos directos (15%)
            if "tipo_oc" in prov_data.columns:
                n_ocs = prov_data["codigo_oc"].nunique()
                n_td = prov_data[prov_data["tipo_oc"].isin(OC_TIPO_TRATO_DIRECTO)]["codigo_oc"].nunique()
                ratio_td = n_td / n_ocs if n_ocs > 0 else 0
                score += ratio_td * 15

            # 5. Montos redondos (10%)
            big = prov_data[prov_data["monto_total_item"] >= 1_000_000]
            if len(big) > 0:
                redondos = (big["monto_total_item"] % 1_000_000 == 0).mean()
                score += redondos * 10

            scores.append({
                "rut_proveedor": rut,
                "nombre_proveedor": prov["nombre_proveedor"],
                "n_ordenes": prov["n_ordenes"],
                "monto_total": prov["monto_total"],
                "n_categorias": prov["n_categorias"],
                "n_compradores": prov["n_compradores"],
                "score_sospecha": round(score, 1),
            })

        result = pd.DataFrame(scores)
        if not result.empty:
            result = result.sort_values("score_sospecha", ascending=False).head(top_n)

        return result

    # ══════════════════════════════════════════════════════════════════════════ #
    # CRUCE 6: SERVEL (Donaciones Electorales vs Licitaciones)
    # ══════════════════════════════════════════════════════════════════════════ #

    def cruce_servel_compras(self) -> pd.DataFrame:
        """
        Cruza los datos de aportes de campaña (SERVEL) vs. las órdenes de compra adjudicadas.
        Detecta casos donde un proveedor que donó dinero a una campaña (o partido) luego 
        ganó una licitación o trato directo.
        """
        with sqlite3.connect(self.db_path) as conn:
            check_table = pd.read_sql_query(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='aportes_servel';",
                conn
            )
            if check_table.empty:
                return pd.DataFrame() 
            
            query = """
                SELECT 
                    a.rut_aportante as rut_proveedor_aportante,
                    a.nombre_aportante,
                    a.rut_receptor as rut_politico_partido,
                    a.nombre_receptor as politico_o_partido,
                    a.monto_aporte as inversion_electoral,
                    a.eleccion_campaña as eleccion,
                    o.rut_comprador,
                    o.nombre_comprador as organismo_que_adjudico,
                    SUM(o.monto_total_item) as retorno_licitaciones,
                    COUNT(DISTINCT o.codigo_oc) as n_ordenes,
                    GROUP_CONCAT(DISTINCT o.tipo_oc) as tipos_de_orden
                FROM 
                    aportes_servel a
                INNER JOIN 
                    ordenes_items o 
                    ON (
                        REPLACE(a.rut_aportante, '-', '') = REPLACE(o.rut_proveedor, '-', '')
                        OR a.nombre_aportante = o.nombre_proveedor
                    )
                WHERE 
                    a.monto_aporte > 0 AND o.monto_total_item > 0
                GROUP BY 
                    a.rut_aportante, a.nombre_aportante, a.rut_receptor, a.nombre_receptor, a.monto_aporte
                ORDER BY 
                    retorno_licitaciones DESC
            """
            try:
                return pd.read_sql_query(query, conn)
            except Exception as e:
                logger.error(f"Error realizando cruce SERVEL vs Compras: {e}")
                return pd.DataFrame()

    # ══════════════════════════════════════════════════════════════════════════ #
    # CRUCE 7: MALLA SOCIETARIA (Dueños Reales Fantasmas) ⭐️ EL SANTO GRIAL
    # ══════════════════════════════════════════════════════════════════════════ #

    def cruce_malla_societaria(self) -> pd.DataFrame:
        """
        El 'Ojo de Dios': Cruza las compras del Mercado Público con el Registro 
        de Empresas y Sociedades para revelar a los dueños y beneficiarios finales.
        Si la base SERVEL está presente, también revelará si el dueño oculto 
        financió campañas.
        """
        with sqlite3.connect(self.db_path) as conn:
            check_table = pd.read_sql_query(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='socios_empresa';",
                conn
            )
            if check_table.empty:
                return pd.DataFrame()
                
            query = """
                SELECT 
                    s.nombre_socio as CABECILLA_OCULTO,
                    s.rut_socio as RUT_CABECILLA,
                    s.porcentaje as PARTICIPACION_ACCIONARIA,
                    o.nombre_proveedor as EMPRESA_PANTALLA,
                    o.rut_proveedor as RUT_EMPRESA,
                    o.nombre_comprador as ORGANISMO_VULNERADO,
                    SUM(o.monto_total_item) as MONTO_EXTRAIDO,
                    GROUP_CONCAT(DISTINCT o.codigo_oc) as ORDENES_ASOCIADAS
                FROM 
                    ordenes_items o
                INNER JOIN 
                    socios_empresa s
                    ON REPLACE(o.rut_proveedor, '-', '') = REPLACE(s.rut_empresa, '-', '')
                WHERE
                    o.monto_total_item > 0
                GROUP BY 
                    s.rut_socio, s.nombre_socio, o.rut_proveedor, o.nombre_proveedor, o.nombre_comprador
                ORDER BY 
                    MONTO_EXTRAIDO DESC
            """
            try:
                return pd.read_sql_query(query, conn)
            except Exception as e:
                logger.error(f"Error en escaneo de red societaria: {e}")
                return pd.DataFrame()

    # ═══════════ Reporte Ejecutivo ═══════════ #

    def reporte_ejecutivo(self) -> dict[str, Any]:
        """
        Genera un resumen ejecutivo con las métricas clave de la auditoría.

        Returns:
            dict con estadísticas globales.
        """
        df = self._load_all()
        if df.empty:
            return {"error": "Sin datos"}

        return {
            "total_ordenes": df["codigo_oc"].nunique(),
            "total_items": len(df),
            "monto_total_clp": float(df["monto_total_item"].sum()),
            "total_proveedores": df["nombre_proveedor"].nunique(),
            "total_compradores": df["nombre_comprador"].nunique(),
            "precio_unitario_max": float(df["precio_unitario"].max()),
            "oc_mas_cara": df.loc[df["monto_total_item"].idxmax()].to_dict() if not df.empty else {},
            "categorias_riesgo": df["categoria_riesgo"].value_counts().to_dict() if "categoria_riesgo" in df.columns else {},
            "tipos_oc": df["tipo_oc"].value_counts().to_dict() if "tipo_oc" in df.columns else {},
        }
