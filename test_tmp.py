"""Extraccion real con ticket de produccion."""
import sys
sys.stdout.reconfigure(encoding='utf-8')

from datetime import date, timedelta
from config import API_TICKET
print(f"Ticket cargado: {API_TICKET[:8]}...{API_TICKET[-4:]}")

from extractor import MercadoPublicoExtractor
from processor import DataProcessor
from detector import AnomalyDetector
from cross_referencer import CrossReferencer

# Probar con ayer y anteayer
for dias in [1, 2]:
    target = date.today() - timedelta(days=dias)
    print(f"\n{'='*50}")
    print(f"Extrayendo OC de {target.strftime('%d/%m/%Y')}...")
    
    ext = MercadoPublicoExtractor()
    ordenes = ext.extract(target)
    print(f"  OC extraidas: {len(ordenes)}")
    
    if ordenes:
        proc = DataProcessor()
        df = proc.process_and_store(ordenes)
        print(f"  Items nuevos: {len(df)}")
        
        if not df.empty and "categoria_riesgo" in df.columns:
            print("  Riesgo:")
            for cat, count in df["categoria_riesgo"].value_counts().head(5).items():
                print(f"    {cat}: {count}")

# Detectar anomalias sobre todos los datos
print(f"\n{'='*50}")
print("Detectando anomalias (serenata)...")
det = AnomalyDetector()
anomalies = det.detect("serenata")
print(f"Total anomalias: {len(anomalies)}")

if not anomalies.empty and "metodo" in anomalies.columns:
    print("\nPor metodo:")
    for met, count in anomalies["metodo"].value_counts().items():
        print(f"  {met}: {count}")

# Cross-reference
print(f"\n{'='*50}")
print("Reporte ejecutivo:")
xref = CrossReferencer()
rep = xref.reporte_ejecutivo()
for k in ["total_ordenes","total_items","total_proveedores","total_compradores"]:
    print(f"  {k}: {rep.get(k, 0)}")
print(f"  monto_total: ${rep.get('monto_total_clp', 0):,.0f} CLP")

if rep.get("categorias_riesgo"):
    print("\n  Categorias de riesgo:")
    for cat, n in rep["categorias_riesgo"].items():
        print(f"    {cat}: {n}")

# Top proveedores sospechosos
print(f"\n{'='*50}")
print("Top 5 proveedores sospechosos:")
sosp = xref.ranking_proveedores_sospechosos(top_n=5)
if not sosp.empty:
    for _, row in sosp.iterrows():
        print(f"  [{row['score_sospecha']:.0f}pts] {row['nombre_proveedor']} | ${row['monto_total']:,.0f} | {row['n_ordenes']} OC")

print("\nDone!")
