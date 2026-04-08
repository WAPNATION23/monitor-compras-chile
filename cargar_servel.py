import sys
import os
from servel_extractor import ServelExtractor

def main():
    if len(sys.argv) < 2:
        print("❌ Uso incorrecto.")
        print("👉 Forma de uso en la terminal:")
        print('   python cargar_servel.py "ruta/al/archivo/servel.xlsx"')
        sys.exit(1)

    archivo = sys.argv[1]

    if not os.path.exists(archivo):
        print(f"❌ Error: El archivo '{archivo}' no existe o la ruta es incorrecta.")
        sys.exit(1)

    print(f"⏳ Iniciando ingesta forense del SERVEL desde: {archivo}")
    extractor = ServelExtractor()

    # Aquí hace la magia de procesar y guardar a la base de datos
    df = extractor.procesar_csv_aportes(archivo)

    if not df.empty:
        print("✅ ¡Operación Exitosa! Se inyectaron correctamente bases del SERVEL a la Red Central.")
        print("👉 Abre tu centro de mando usando: streamlit run dashboard.py")
    else:
        print("⚠️ Algoritmo falló al procesar o el archivo subido no tenía registros válidos.")

if __name__ == "__main__":
    main()
