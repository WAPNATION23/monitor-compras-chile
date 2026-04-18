"""
Configuración central del proyecto Monitor Compras Chile.
Inspirado en Operação Serenata de Amor (okfn-brasil).

Los valores sensibles se leen desde variables de entorno (GitHub Actions)
con fallback al valor local para desarrollo.

IMPORTANTE: NO poner tokens o secrets en este archivo.
           Usar variables de entorno o archivo .env (no versionado).
"""

import logging
import os
from pathlib import Path

# Cargar variables de entorno desde .env (si existe)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass  # python-dotenv no instalado, usar solo variables de entorno del sistema

# ─────────────────────────── API Mercado Público ──────────────────────────── #
# Endpoint oficial de Órdenes de Compra - Formato JSON
API_OC_URL: str = (
    "https://api.mercadopublico.cl/servicios/v1/publico/ordenesdecompra.json"
)
# Alias de compatibilidad
API_BASE_URL: str = API_OC_URL

# Endpoint de Licitaciones
API_LICITACIONES_URL: str = (
    "https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json"
)

# Endpoint de búsqueda de Proveedores (por RUT)
API_BUSCAR_PROVEEDOR_URL: str = (
    "https://api.mercadopublico.cl/servicios/v1/Publico/Empresas/BuscarProveedor"
)

# Endpoint de búsqueda de Organismos Compradores
API_BUSCAR_COMPRADOR_URL: str = (
    "https://api.mercadopublico.cl/servicios/v1/Publico/Empresas/BuscarComprador"
)

# Ticket de prueba proporcionado por ChileCompra.
# Para producción, solicitar uno propio en https://api.mercadopublico.cl → "Solicitud de Ticket".
API_TICKET: str = os.getenv("MERCADO_PUBLICO_TICKET", "")
if not API_TICKET:
    logging.getLogger(__name__).warning(
        "MERCADO_PUBLICO_TICKET no configurado — las consultas a la API de "
        "Mercado Público fallarán. Configúralo en .env o como variable de entorno."
    )

# ──────────────────────────── Base de datos ────────────────────────────────── #
_BASE_DIR = Path(__file__).resolve().parent
DB_NAME: str = str(_BASE_DIR / "auditoria_estado.db")

# ──────────────────────────── Parámetros de Red ────────────────────────────── #
REQUEST_TIMEOUT: int = 30          # segundos
MAX_RETRIES: int = 3               # reintentos ante fallo de conexión
RETRY_BACKOFF: float = 2.0         # factor exponencial de espera entre reintentos

# ──────────────────────── Detección de Anomalías ──────────────────────────── #
IQR_MULTIPLIER: float = 1.5        # Umbral IQR clásico (1.5 = outlier, 3.0 = outlier extremo)
ZSCORE_THRESHOLD: float = 2.5      # Umbral Z-score para considerar sobreprecio
MIN_OBSERVATIONS: int = 5          # Mínimo de registros históricos para calcular anomalías

# Umbrales forenses (Serenata)
MIN_AMOUNT_VAMPIRE: int = 10_000_000      # CLP — sospechoso en horario no hábil
MIN_AMOUNT_FRACCIONAMIENTO: int = 1_900_000  # ~30 UTM — umbral para evasión de licitación
FRACCIONAMIENTO_WINDOW_DAYS: int = 7       # Ventana de días para patrón de fraccionamiento
FRACCIONAMIENTO_MIN_OCS: int = 3           # Mínimo de compras en la ventana
MIN_CATEGORIAS_FANTASMA: int = 4           # Categorías distintas para "empresa de papel"
MIN_MONTO_SPIDER: int = 5_000_000          # CLP — monto mínimo para Red de Araña
MONOPOLIO_PCT: float = 0.80                # Proveedor concentra >80% de las OCs de un organismo
MONOPOLIO_MIN_OCS: int = 10                # Mínimo de OCs de ese comprador para aplicar la regla
MONOPOLIO_MIN_MONTO: int = 50_000_000      # Monto mínimo agregado para alertar monopolio
PROV_NUEVO_DIAS: int = 30                  # Proveedor con primera OC hace <N días
PROV_NUEVO_MIN_MONTO: int = 20_000_000     # Y ya factura más de este monto => shell sospechosa
DAILY_QUERY_LIMIT: int = 20               # Consultas IA por día por IP

# ────────────────────── Notificaciones Telegram ───────────────────────────── #
# Crear un bot con @BotFather y pegar el token aquí.
# Para obtener el chat_id, envía un mensaje al bot y consulta:
#   https://api.telegram.org/bot<TOKEN>/getUpdates
# ⚠️ NUNCA hardcodear tokens reales aquí. Usar variables de entorno.
TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

# ─────────────────── Link directo a OC en Mercado Público ─────────────────── #
MERCADO_PUBLICO_OC_URL: str = "https://www.mercadopublico.cl/Procurement/Modules/RFB/DetailsAcquisition.aspx?qs="

# ──────────────── Clasificación Automática de Riesgo ──────────────────────── #
# Palabras clave en nombre_comprador para clasificar categoría de riesgo
RISK_CLASSIFICATION: dict[str, list[str]] = {
    "MUNICIPALIDAD": ["MUNICIPALIDAD", "MUNICIPAL", "I. MUNICIPALIDAD"],
    "FUERZAS ARMADAS/ORDEN": [
        "EJÉRCITO", "EJERCITO", "ARMADA", "FUERZA AÉREA", "FUERZA AEREA",
        "CARABINEROS", "PDI", "POLICÍA", "POLICIA", "GENDARMERÍA", "GENDARMERIA",
    ],
    "ALERTA FUNDACIONES/TRATO DIRECTO": [
        "FUNDACIÓN", "FUNDACION", "CORPORACIÓN", "CORPORACION",
        "ONG", "ASOCIACIÓN", "ASOCIACION",
    ],
    "MOP/OBRAS": [
        "MOP", "OBRAS PÚBLICAS", "OBRAS PUBLICAS", "SERVIU",
        "DIRECCIÓN DE VIALIDAD", "DIRECCION DE VIALIDAD",
    ],
}

# ──────────── Códigos de tipo de OC (para filtros avanzados) ──────────────── #
# Ref: https://api.mercadopublico.cl/ → Documentación de orden de compra
OC_TIPO_TRATO_DIRECTO: list[str] = ["D1", "C1", "F3", "G1", "FG", "TD", "SE"]
OC_TIPO_CONVENIO_MARCO: list[str] = ["CM"]
OC_TIPO_COMPRA_AGIL: list[str] = ["AG", "MC", "R1"]

# ──────────── Códigos de estado de OC ──────────────────────── #
OC_ESTADO_LABELS: dict[str, str] = {
    "4": "Enviada a Proveedor",
    "5": "En proceso",
    "6": "Aceptada",
    "9": "Cancelada",
    "12": "Recepción Conforme",
    "13": "Pendiente de Recepcionar",
    "14": "Recepcionada Parcialmente",
    "15": "Recepción Conforme Incompleta",
}
