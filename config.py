"""
Configuración central del proyecto Monitor Compras Chile.
Inspirado en Operação Serenata de Amor (okfn-brasil).

Los valores sensibles se leen desde variables de entorno (GitHub Actions)
con fallback al valor local para desarrollo.
"""

import os

# ─────────────────────────── API Mercado Público ──────────────────────────── #
# Endpoint oficial de Órdenes de Compra - Formato JSON
API_BASE_URL: str = (
    "https://api.mercadopublico.cl/servicios/v1/publico/ordenesdecompra.json"
)

# Ticket de prueba proporcionado por ChileCompra.
# Para producción, solicitar uno propio en https://api.mercadopublico.cl → "Solicitud de Ticket".
API_TICKET: str = os.getenv("MERCADO_PUBLICO_TICKET", "F8537A18-6766-4DEF-9E59-426B4FEE2844")

# ──────────────────────────── Base de datos ────────────────────────────────── #
DB_NAME: str = "auditoria_estado.db"

# ──────────────────────────── Parámetros de Red ────────────────────────────── #
REQUEST_TIMEOUT: int = 30          # segundos
MAX_RETRIES: int = 3               # reintentos ante fallo de conexión
RETRY_BACKOFF: float = 2.0         # factor exponencial de espera entre reintentos

# ──────────────────────── Detección de Anomalías ──────────────────────────── #
IQR_MULTIPLIER: float = 1.5        # Umbral IQR clásico (1.5 = outlier, 3.0 = outlier extremo)
ZSCORE_THRESHOLD: float = 2.5      # Umbral Z-score para considerar sobreprecio
MIN_OBSERVATIONS: int = 5          # Mínimo de registros históricos para calcular anomalías

# ────────────────────── Notificaciones Telegram ───────────────────────────── #
# Crear un bot con @BotFather y pegar el token aquí.
# Para obtener el chat_id, envía un mensaje al bot y consulta:
#   https://api.telegram.org/bot<TOKEN>/getUpdates
TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_TOKEN", "7965315725:AAGSXH-uShjvfxgus5Ncu9XdKsKS8hqL5K0")
TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "8498127525")

# ─────────────────── Link directo a OC en Mercado Público ─────────────────── #
MERCADO_PUBLICO_OC_URL: str = "https://www.mercadopublico.cl/Procurement/Modules/RFB/DetailsAcquisition.aspx?qs="
