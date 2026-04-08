"""
Obtiene tu chat_id de Telegram automáticamente.

Instrucciones:
  1. Pon tu TELEGRAM_BOT_TOKEN en config.py
  2. Envíale un mensaje cualquiera a tu bot en Telegram
  3. Ejecuta: py obtener_chat_id.py
"""

import sys
import requests
from config import TELEGRAM_BOT_TOKEN

def main() -> None:
    if not TELEGRAM_BOT_TOKEN:
        print("❌ TELEGRAM_BOT_TOKEN está vacío en config.py")
        print("   Pega el token que te dio @BotFather y vuelve a ejecutar.")
        sys.exit(1)

    url: str = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"

    print("📡 Consultando Telegram...")
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()
    except Exception as exc:
        print(f"❌ Error de conexión: {exc}")
        sys.exit(1)

    if not data.get("ok"):
        print(f"❌ Token inválido. Telegram respondió: {data.get('description')}")
        sys.exit(1)

    results = data.get("result", [])
    if not results:
        print("⚠  No hay mensajes. Envíale algo a tu bot en Telegram y vuelve a ejecutar.")
        sys.exit(1)

    # Mostrar todos los chats encontrados
    chats_vistos: set = set()
    print("\n✅ Chats encontrados:\n")
    for update in results:
        msg = update.get("message") or update.get("channel_post", {})
        chat = msg.get("chat", {})
        chat_id = chat.get("id")
        if chat_id and chat_id not in chats_vistos:
            chats_vistos.add(chat_id)
            tipo = chat.get("type", "?")
            nombre = chat.get("title") or chat.get("first_name", "")
            print(f"   💬 {nombre} ({tipo})")
            print(f"      chat_id = {chat_id}")
            print()

    # Si solo hay uno, dar la línea lista para copiar
    if len(chats_vistos) == 1:
        the_id = list(chats_vistos)[0]
        print('📋 Copia esto en config.py:\n')
        print(f'   TELEGRAM_CHAT_ID: str = "{the_id}"')
        print()

if __name__ == "__main__":
    main()
