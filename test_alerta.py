from notifier import TelegramNotifier
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

def probar_alerta():
    # Inicializamos tu clase
    bot = TelegramNotifier(token=TELEGRAM_BOT_TOKEN, chat_id=TELEGRAM_CHAT_ID)
    
    print("Enviando alerta de prueba a Telegram...")
    
    # Disparamos el misil con datos falsos (pero que parecen muy reales ksksks)
    bot.enviar_alerta_desfalco(
        categoria_riesgo="ALERTA FUNDACIONES/TRATO DIRECTO",
        comprador="Gobierno Regional de Antofagasta",
        producto="Asesoría Habitacional Campamentos (Servicio de pintado de fachadas)",
        precio_pagado=426000000.0,  # 426 millones
        precio_promedio=15000000.0, # 15 millones
        z_score=8.5,
        link_orden="https://www.mercadopublico.cl/ordenesdecompra/1234-56-SE26"
    )
    
    print("¡Alerta enviada! Revisa tu celular 📱")

if __name__ == "__main__":
    probar_alerta()
