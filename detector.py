"""
AnomalyDetector
═══════════════
Lee la base de datos SQLite y aplica métodos estadísticos para detectar
sobreprecios en las compras del Estado.

Métodos implementados:
  1. IQR (Rango Intercuartílico)  – Robusto ante outliers.
  2. Z-Score modificado           – Basado en la mediana (MAD).

Inspirado en los clasificadores de anomalías de Rosie (Serenata de Amor).
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from config import DB_NAME, IQR_MULTIPLIER, MIN_OBSERVATIONS, ZSCORE_THRESHOLD

logger = logging.getLogger(__name__)


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
                estado
            FROM ordenes_items
            WHERE precio_unitario > 0
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                df: pd.DataFrame = pd.read_sql_query(query, conn)
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

        Args:
            group: Subconjunto de datos con el mismo nombre_producto.
            multiplier: Factor IQR (1.5 estándar, 3.0 extremo).

        Returns:
            Filas que son outliers por IQR.
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

        Un valor |Z_mod| > threshold se considera anómalo.

        Args:
            group: Subconjunto de datos con el mismo nombre_producto.
            threshold: Umbral de Z-score modificado.

        Returns:
            Filas que son outliers por Z-score modificado.
        """
        precios: pd.Series = group["precio_unitario"]
        mediana: float = precios.median()
        mad: float = np.median(np.abs(precios - mediana))

        if mad == 0:
            # Si MAD es 0, todos los precios son iguales → no hay outliers
            return pd.DataFrame()

        z_mod: pd.Series = 0.6745 * (precios - mediana) / mad
        outliers: pd.DataFrame = group[z_mod > threshold].copy()
        outliers["metodo"] = "Z-Score"
        outliers["z_score"] = z_mod[z_mod > threshold]
        outliers["mediana"] = mediana
        outliers["mad"] = mad
        return outliers

    # ──────────────────── Pipeline principal ──────────────────── #

    def detect(self, method: str = "both") -> pd.DataFrame:
        """
        Ejecuta la detección de anomalías.

        Args:
            method: "iqr", "zscore", o "both" (ambos).

        Returns:
            DataFrame con las compras anómalas encontradas.
        """
        df: pd.DataFrame = self._load_data()

        if df.empty:
            logger.warning("No hay datos en la BD para analizar.")
            return pd.DataFrame()

        anomalies: list[pd.DataFrame] = []
        grouped = df.groupby("nombre_producto")

        for product_name, group in grouped:
            # Solo analizar productos con suficientes observaciones
            if len(group) < MIN_OBSERVATIONS:
                continue

            if method in ("iqr", "both"):
                iqr_outliers: pd.DataFrame = self._detect_iqr(group)
                if not iqr_outliers.empty:
                    anomalies.append(iqr_outliers)

            if method in ("zscore", "both"):
                z_outliers: pd.DataFrame = self._detect_zscore(group)
                if not z_outliers.empty:
                    anomalies.append(z_outliers)

        if not anomalies:
            logger.info("✓ No se detectaron anomalías con el método '%s'.", method)
            return pd.DataFrame()

        result: pd.DataFrame = pd.concat(anomalies, ignore_index=True)

        # Eliminar duplicados (un registro puede aparecer en ambos métodos)
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

    def report(self, method: str = "both") -> None:
        """Imprime un reporte legible de las anomalías detectadas."""
        anomalies: pd.DataFrame = self.detect(method)

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

        cols_display: list[str] = [
            "codigo_oc",
            "nombre_producto",
            "precio_unitario",
            "cantidad",
            "monto_total_item",
            "rut_proveedor",
            "nombre_proveedor",
            "metodo",
        ]
        # Filtrar solo columnas que existan
        cols_display = [c for c in cols_display if c in anomalies.columns]

        for i, (_, row) in enumerate(anomalies.iterrows(), start=1):
            print(f"\n  [{i}] OC: {row.get('codigo_oc', 'N/A')}")
            print(f"      Producto   : {row.get('nombre_producto', 'N/A')}")
            print(f"      Precio Unit: ${row.get('precio_unitario', 0):,.0f} CLP")
            print(f"      Cantidad   : {row.get('cantidad', 0)}")
            print(f"      Monto Total: ${row.get('monto_total_item', 0):,.0f} CLP")
            print(f"      Proveedor  : {row.get('nombre_proveedor', 'N/A')} ({row.get('rut_proveedor', 'N/A')})")
            print(f"      Método     : {row.get('metodo', 'N/A')}")

            if row.get("metodo") == "IQR":
                print(f"      Umbral IQR : ${row.get('umbral_superior', 0):,.0f} CLP")
            elif row.get("metodo") == "Z-Score":
                print(f"      Z-Score    : {row.get('z_score', 0):.2f}")
                print(f"      Mediana    : ${row.get('mediana', 0):,.0f} CLP")

        print("\n" + "=" * 70)
