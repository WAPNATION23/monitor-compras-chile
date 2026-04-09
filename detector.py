"""
AnomalyDetector
═══════════════
Lee la base de datos SQLite y aplica métodos estadísticos para detectar
sobreprecios en las compras del Estado.

Métodos implementados:
  1. IQR (Rango Intercuartílico)  – Robusto ante outliers.
  2. Z-Score modificado           – Basado en la mediana (MAD).
  3. Horario Vampiro              – Compras en horario no hábil.
  4. Fraccionamiento              – Evasión de licitación pública.
  5. Ley del Fantasma             – Proveedores multigiro sospechosos.
  6. Ley de Benford               – Detección de montos fabricados.
  7. Red de Araña                 – Asesorías con montos redondos.

Métodos de ejecución:
  • "iqr"          → Solo IQR
  • "zscore"       → Solo Z-Score
  • "estadistico"  → IQR + Z-Score (sin forenses)
  • "serenata"     → TODOS los algoritmos (estadísticos + forenses)
  • "all"          → Alias de serenata

Inspirado en los clasificadores de anomalías de Rosie (Serenata de Amor).
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from config import (
    DB_NAME,
    FRACCIONAMIENTO_MIN_OCS,
    FRACCIONAMIENTO_WINDOW_DAYS,
    IQR_MULTIPLIER,
    MIN_AMOUNT_FRACCIONAMIENTO,
    MIN_AMOUNT_VAMPIRE,
    MIN_CATEGORIAS_FANTASMA,
    MIN_MONTO_SPIDER,
    MIN_OBSERVATIONS,
    ZSCORE_THRESHOLD,
)

logger = logging.getLogger(__name__)

# Métodos que activan detectores estadísticos (IQR/Z-Score)
_ESTADISTICOS = {"iqr", "zscore", "estadistico", "serenata", "all"}
# Métodos que activan detectores forenses (Serenata)
_FORENSES = {"serenata", "all"}


class AnomalyDetector:
    """Detecta compras con precios unitarios anómalos respecto a su historial."""

    def __init__(self, db_path: str | Path = DB_NAME) -> None:
        self.db_path = Path(db_path)

    # ──────────────────── Carga de datos ──────────────────── #

    def _load_data(self) -> pd.DataFrame:
        """Carga todos los ítems de la tabla *ordenes_items*."""
        query: str = """
            SELECT
                codigo_oc,
                nombre_producto,
                categoria,
                cantidad,
                precio_unitario,
                monto_total_item,
                rut_comprador,
                nombre_comprador,
                rut_proveedor,
                nombre_proveedor,
                fecha_creacion,
                estado,
                IFNULL(tipo_oc, '') as tipo_oc,
                IFNULL(categoria_riesgo, 'GENERAL') as categoria_riesgo
            FROM ordenes_items
            WHERE precio_unitario > 0
              AND estado != '9'
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                df: pd.DataFrame = pd.read_sql_query(query, conn)

            # Formatear la fecha para facilitar análisis temporal
            if not df.empty:
                df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion"], errors="coerce")

            logger.info("Datos cargados: %d registros.", len(df))
            return df
        except sqlite3.Error as exc:
            logger.error("Error leyendo la BD: %s", exc)
            raise

    # ──────────────────── Método IQR ──────────────────── #

    @staticmethod
    def _detect_iqr(
        group: pd.DataFrame, multiplier: float = IQR_MULTIPLIER
    ) -> pd.DataFrame:
        """
        Marca como anomalía los registros cuyo precio_unitario supera
        Q3 + multiplier × IQR dentro de su grupo (mismo producto).
        """
        q1: float = group["precio_unitario"].quantile(0.25)
        q3: float = group["precio_unitario"].quantile(0.75)
        iqr: float = q3 - q1
        upper_bound: float = q3 + multiplier * iqr

        outliers: pd.DataFrame = group[group["precio_unitario"] > upper_bound].copy()
        outliers["metodo"] = "IQR"
        outliers["umbral_superior"] = upper_bound
        outliers["q1"] = q1
        outliers["q3"] = q3
        outliers["iqr"] = iqr
        return outliers

    # ──────────────────── Método Z-Score Modificado ──────────────────── #

    @staticmethod
    def _detect_zscore(
        group: pd.DataFrame, threshold: float = ZSCORE_THRESHOLD
    ) -> pd.DataFrame:
        """
        Usa Z-score modificado (basado en MAD) para detectar outliers.
        Z_modificado = 0.6745 × (x - mediana) / MAD
        """
        precios: pd.Series = group["precio_unitario"]
        mediana: float = precios.median()
        mad: float = np.median(np.abs(precios - mediana))

        if mad == 0:
            return pd.DataFrame()

        z_mod: pd.Series = 0.6745 * (precios - mediana) / mad
        outliers: pd.DataFrame = group[z_mod > threshold].copy()
        outliers["metodo"] = "Z-Score"
        outliers["z_score"] = z_mod[z_mod > threshold]
        outliers["mediana"] = mediana
        outliers["mad"] = mad
        return outliers

    # ──────────────────── Método Horario Vampiro ──────────────────── #

    @staticmethod
    def _detect_vampiro(df: pd.DataFrame) -> pd.DataFrame:
        """
        Detecta compras creadas en horarios inusuales (fines de semana o fuera
        de horario laboral, e.g. 20:00 a 07:59) y con montos altos (> 10M).
        """
        if "fecha_creacion" not in df.columns or df.empty:
            return pd.DataFrame()

        monto_sospechoso = MIN_AMOUNT_VAMPIRE

        df_valid = df.dropna(subset=["fecha_creacion"]).copy()

        # Fines de semana (5=Sábado, 6=Domingo)
        is_weekend = df_valid["fecha_creacion"].dt.dayofweek >= 5
        # Noche/Madrugada (antes de las 8am o después de las 8pm)
        is_night = (df_valid["fecha_creacion"].dt.hour < 8) | (df_valid["fecha_creacion"].dt.hour >= 20)

        is_vampiro = (is_weekend | is_night) & (df_valid["monto_total_item"] >= monto_sospechoso)

        outliers = df_valid[is_vampiro].copy()
        outliers["metodo"] = "Horario Vampiro"
        outliers["motivo_alerta"] = "Compra de alto monto en horario no hábil"
        return outliers

    # ──────────────────── Método Fraccionamiento ──────────────────── #

    @staticmethod
    def _detect_fraccionamiento(df: pd.DataFrame) -> pd.DataFrame:
        """
        Detecta múltiples compras menores a un mismo proveedor por el mismo comprador
        en una ventana de 7 días, sugiriendo evasión de licitación pública.
        """
        if "fecha_creacion" not in df.columns or df.empty:
            return pd.DataFrame()

        df_sorted = df.dropna(subset=["fecha_creacion"]).sort_values("fecha_creacion")

        df_sorted = df_sorted.set_index("fecha_creacion")
        grouper = df_sorted.groupby(["rut_comprador", "rut_proveedor"])

        # Ventana configurable
        rolling_counts = grouper["codigo_oc"].rolling(f"{FRACCIONAMIENTO_WINDOW_DAYS}D").count()
        rolling_sums = grouper["monto_total_item"].rolling(f"{FRACCIONAMIENTO_WINDOW_DAYS}D").sum()

        # Criterio: Al menos N compras en la ventana sumando más del umbral
        mask = (rolling_counts >= FRACCIONAMIENTO_MIN_OCS) & (rolling_sums > MIN_AMOUNT_FRACCIONAMIENTO)

        indices_sospechosos = mask[mask].reset_index()

        if indices_sospechosos.empty:
            return pd.DataFrame()

        anomalias_list = []
        for _, row in indices_sospechosos.iterrows():
            comprador, proveedor, fecha = row["rut_comprador"], row["rut_proveedor"], row["fecha_creacion"]

            fecha_inicio = fecha - pd.Timedelta(days=FRACCIONAMIENTO_WINDOW_DAYS)
            subset = df_sorted[
                (df_sorted["rut_comprador"] == comprador) &
                (df_sorted["rut_proveedor"] == proveedor)
            ].loc[fecha_inicio:fecha]

            anomalias_list.append(subset)

        if not anomalias_list:
             return pd.DataFrame()

        outliers = pd.concat(anomalias_list).drop_duplicates(subset=["codigo_oc", "nombre_producto"])
        outliers = outliers.reset_index()
        outliers["metodo"] = "Fraccionamiento"
        outliers["motivo_alerta"] = "Múltiples compras al mismo proveedor en 7 días"

        return outliers

    # ──────────────────── Método Ley del Fantasma (Multigiro) ──────────────────── #

    @staticmethod
    def _detect_fantasma(df: pd.DataFrame) -> pd.DataFrame:
        """
        Detecta proveedores que venden productos de categorías extremadamente disímiles
        (ej: "Servicios Médicos" y "Obras de Construcción"), sugiriendo un "Giro de papel".
        """
        if "categoria" not in df.columns or df.empty:
            return pd.DataFrame()

        df_valid = df.dropna(subset=["categoria"]).copy()

        cats_por_prov = df_valid.groupby(["rut_proveedor", "nombre_proveedor"])["categoria"].nunique().reset_index()

        # Criterio empírico: Más de N categorías completamente distintas
        rut_sospechosos = cats_por_prov[cats_por_prov["categoria"] >= MIN_CATEGORIAS_FANTASMA]["rut_proveedor"]

        if rut_sospechosos.empty:
            return pd.DataFrame()

        outliers = df[df["rut_proveedor"].isin(rut_sospechosos)].copy()
        outliers["metodo"] = "Ley del Fantasma (Serenata)"
        outliers["motivo_alerta"] = "Proveedor vende en rubros incompatibles (Posible Empresa de Papel)"
        return outliers

    # ──────────────────── Método Ley de Benford (Manipulación) ──────────────────── #

    @staticmethod
    def _detect_benford(df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica la matemática forense de la Ley de Benford sobre montos totales de proveedores.
        """
        if "monto_total_item" not in df.columns or df.empty:
            return pd.DataFrame()

        df_valid = df.dropna(subset=["rut_proveedor", "monto_total_item"]).copy()
        df_valid = df_valid[df_valid["monto_total_item"] >= 10]

        df_valid["primer_digito"] = df_valid["monto_total_item"].apply(lambda x: int(str(int(abs(x)))[0]))

        outliers_list = []
        for _rut, group in df_valid.groupby("rut_proveedor"):
            if len(group) < 10:
                continue

            conteo = group["primer_digito"].value_counts(normalize=True)
            freq_1 = conteo.get(1, 0.0)
            freq_alta = sum([conteo.get(d, 0.0) for d in [7, 8, 9]])

            if freq_1 < 0.10 and freq_alta > 0.40:
                outliers_list.append(group)

        if not outliers_list:
            return pd.DataFrame()

        outliers = pd.concat(outliers_list).copy()
        outliers = outliers.drop(columns=["primer_digito"])
        outliers["metodo"] = "Ley de Benford (Serenata)"
        outliers["motivo_alerta"] = "Curva de montos manipulada aritméticamente"
        return outliers

    # ──────────────────── Método Red de Araña (Tratos Redondos) ──────────────────── #

    @staticmethod
    def _detect_spider(df: pd.DataFrame) -> pd.DataFrame:
        """
        Detecta Tratos Directos de "Asesorías" o "Estudios" con montos cerrados
        sospechosamente perfectos.
        """
        if "nombre_producto" not in df.columns or df.empty:
            return pd.DataFrame()

        df_valid = df.dropna(subset=["nombre_producto", "monto_total_item"]).copy()

        regex_humo = "ASESORÍA|ASESORIA|ESTUDIO|CONSULTORÍA|CONSULTORIA|CAPACITACIÓN|EVALUACIÓN"
        mask_humo = df_valid["nombre_producto"].str.upper().str.contains(regex_humo)

        import numpy as np
        mask_redondo = (df_valid["monto_total_item"] >= MIN_MONTO_SPIDER) & (np.isclose(df_valid["monto_total_item"] % 1_000_000, 0, atol=0.01))

        is_spider = mask_humo & mask_redondo
        outliers = df_valid[is_spider].copy()

        if outliers.empty:
            return pd.DataFrame()

        outliers["metodo"] = "Red de Araña (Serenata)"
        outliers["motivo_alerta"] = "Servicio intangible millonario con monto sospechosamente cerrado"
        return outliers

    # ──────────────────── Pipeline principal ──────────────────── #

    def detect(self, method: str = "serenata") -> pd.DataFrame:
        """
        Ejecuta la detección de anomalías.

        Args:
            method:
                "iqr"          – Solo IQR
                "zscore"       – Solo Z-Score
                "estadistico"  – IQR + Z-Score
                "serenata"     – TODOS los algoritmos
                "all"          – Alias de serenata

        Returns:
            DataFrame con las compras anómalas encontradas.
        """
        df: pd.DataFrame = self._load_data()

        if df.empty:
            logger.warning("No hay datos en la BD para analizar.")
            return pd.DataFrame()

        anomalies: list[pd.DataFrame] = []
        grouped = df.groupby("nombre_producto")

        # ── Detectores Estadísticos (por producto) ──
        for _product_name, group in grouped:
            if len(group) < MIN_OBSERVATIONS:
                continue

            if method in ("iqr", "estadistico", "serenata", "all"):
                iqr_outliers: pd.DataFrame = self._detect_iqr(group)
                if not iqr_outliers.empty:
                    anomalies.append(iqr_outliers)

            if method in ("zscore", "estadistico", "serenata", "all"):
                z_outliers: pd.DataFrame = self._detect_zscore(group)
                if not z_outliers.empty:
                    anomalies.append(z_outliers)

        # ── Detectores Forenses (solo con serenata/all) ──
        if method in _FORENSES:
            # Horarios Vampiro
            vampiro_outliers = self._detect_vampiro(df)
            if not vampiro_outliers.empty:
                anomalies.append(vampiro_outliers)

            # Fraccionamiento
            fracc_outliers = self._detect_fraccionamiento(df)
            if not fracc_outliers.empty:
                anomalies.append(fracc_outliers)

            # Ley del Fantasma
            fantasma_outliers = self._detect_fantasma(df)
            if not fantasma_outliers.empty:
                anomalies.append(fantasma_outliers)

            # Ley de Benford
            benford_outliers = self._detect_benford(df)
            if not benford_outliers.empty:
                anomalies.append(benford_outliers)

            # Red de Araña
            spider_outliers = self._detect_spider(df)
            if not spider_outliers.empty:
                anomalies.append(spider_outliers)

        if not anomalies:
            logger.info("✓ No se detectaron anomalías con el método '%s'.", method)
            return pd.DataFrame()

        result: pd.DataFrame = pd.concat(anomalies, ignore_index=True)

        # Eliminar duplicados (un registro puede aparecer en múltiples métodos)
        result = result.drop_duplicates(
            subset=["codigo_oc", "nombre_producto", "precio_unitario", "metodo"]
        )

        # Ordenar por sobreprecio más extremo
        result = result.sort_values("precio_unitario", ascending=False)

        logger.info(
            "⚠ Se detectaron %d compras anómalas con el método '%s'.",
            len(result), method,
        )
        return result

    # ──────────────────── Reporte en consola ──────────────────── #

    def report(self, method: str = "serenata") -> None:
        """Ejecuta detect() e imprime reporte. Cuidado: llama detect() internamente."""
        anomalies: pd.DataFrame = self.detect(method)
        self.report_from_dataframe(anomalies)

    def report_from_dataframe(self, anomalies: pd.DataFrame) -> None:
        """Imprime un reporte legible de anomalías ya calculadas (sin re-ejecutar detect)."""
        if anomalies.empty:
            print("\n" + "=" * 60)
            print("  ✅  SIN ANOMALÍAS DETECTADAS")
            print("=" * 60)
            return

        print("\n" + "=" * 70)
        print("  🚨  REPORTE DE ANOMALÍAS EN COMPRAS PÚBLICAS")
        print("=" * 70)
        print(f"  Total de compras sospechosas: {len(anomalies)}")
        print("-" * 70)

        # Agrupar por método para mejor legibilidad
        metodos_encontrados = anomalies["metodo"].unique() if "metodo" in anomalies.columns else []
        if len(metodos_encontrados) > 0:
            print(f"  Métodos activos: {', '.join(metodos_encontrados)}")
            print("-" * 70)

        for i, (_, row) in enumerate(anomalies.iterrows(), start=1):
            print(f"\n  [{i}] OC: {row.get('codigo_oc', 'N/A')}")
            print(f"      Producto   : {row.get('nombre_producto', 'N/A')}")
            print(f"      Precio Unit: ${row.get('precio_unitario', 0):,.0f} CLP")
            print(f"      Cantidad   : {row.get('cantidad', 0)}")
            print(f"      Monto Total: ${row.get('monto_total_item', 0):,.0f} CLP")
            print(f"      Proveedor  : {row.get('nombre_proveedor', 'N/A')} ({row.get('rut_proveedor', 'N/A')})")
            print(f"      Comprador  : {row.get('nombre_comprador', 'N/A')}")
            print(f"      Método     : {row.get('metodo', 'N/A')}")

            if row.get("metodo") == "IQR":
                print(f"      Umbral IQR : ${row.get('umbral_superior', 0):,.0f} CLP")
            elif row.get("metodo") == "Z-Score":
                print(f"      Z-Score    : {row.get('z_score', 0):.2f}")
                print(f"      Mediana    : ${row.get('mediana', 0):,.0f} CLP")
            elif row.get("motivo_alerta"):
                print(f"      Motivo     : {row.get('motivo_alerta')}")

            # Mostrar categoría de riesgo si existe
            cat = row.get("categoria_riesgo", "")
            if cat and cat != "GENERAL":
                print(f"      ⚠ Riesgo   : {cat}")

        print("\n" + "=" * 70)
