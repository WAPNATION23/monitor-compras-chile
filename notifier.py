"""
TelegramNotifier
════════════════
Envía alertas de anomalías en compras públicas a un chat/canal de Telegram
usando la API HTTP pura (sin dependencias extra, solo requests).

Formato de mensajes: MarkdownV2 con emojis para máxima legibilidad.
"""

from __future__ import annotations

import logging
from typing import Any

import requests

from config import (
    MERCADO_PUBLICO_OC_URL,
    REQUEST_TIMEOUT,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
)

logger = logging.getLogger(__name__)

# ──────────────────── Constantes internas ──────────────────── #

_TG_API_BASE: str = "https://api.telegram.org/bot{token}"
_SEND_MESSAGE_ENDPOINT: str = "/sendMessage"


class TelegramNotifier:
    """Envía alertas formateadas a Telegram vía API HTTP."""

    def __init__(
        self,
        token: str = TELEGRAM_BOT_TOKEN,
        chat_id: str = TELEGRAM_CHAT_ID,
    ) -> None:
        if not token or not chat_id:
            raise ValueError(
                "TELEGRAM_BOT_TOKEN y TELEGRAM_CHAT_ID deben estar configurados "
                "en config.py (no pueden estar vacíos)."
            )
        self.token = token
        self.chat_id = chat_id
        self._api_url: str = _TG_API_BASE.format(token=self.token)
        self.session = requests.Session()

    # ──────────────── Helper: escapar MarkdownV2 ──────────────── #

    @staticmethod
    def _escape_md(text: str) -> str:
        """
        Escapa caracteres reservados de Telegram MarkdownV2.

        Ref: https://core.telegram.org/bots/api#markdownv2-style
        """
        reserved = r"_*[]()~`>#+-=|{}.!"
        return "".join(f"\\{c}" if c in reserved else c for c in str(text))

    # ──────────── Helper: formatear moneda CLP ──────────────── #

    @staticmethod
    def _fmt_clp(valor: float) -> str:
        """Formatea un número como moneda chilena."""
        return f"${valor:,.0f}".replace(",", ".")

    # ──────────────── Envío genérico de mensaje ──────────────── #

    def _send_message(self, text: str, parse_mode: str = "MarkdownV2") -> dict[str, Any]:
        """
        Envía un mensaje de texto al chat configurado.

        Args:
            text: Contenido del mensaje (ya formateado).
            parse_mode: Modo de parseo ("MarkdownV2", "HTML", etc.).

        Returns:
            dict con la respuesta de la API de Telegram.

        Raises:
            requests.exceptions.RequestException: si falla el envío.
        """
        url: str = self._api_url + _SEND_MESSAGE_ENDPOINT
        payload: dict[str, str] = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": "true",
        }

        try:
            response = self.session.post(url, json=payload, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            result: dict[str, Any] = response.json()

            if not result.get("ok"):
                logger.error(
                    "Telegram API respondió con error: %s",
                    result.get("description", "Sin descripción"),
                )
            else:
                logger.info("✓ Mensaje enviado a Telegram (chat: %s).", self.chat_id)

            return result

        except requests.exceptions.Timeout:
            logger.error("Timeout al enviar mensaje a Telegram.")
            raise
        except requests.exceptions.ConnectionError as exc:
            logger.error("Error de conexión con Telegram: %s", exc)
            raise
        except requests.exceptions.HTTPError as exc:
            logger.error("Error HTTP de Telegram: %s", exc)
            raise

    # ═══════════════ MÉTODO PRINCIPAL: Alerta de Desfalco ═══════════════ #

    def enviar_alerta_desfalco(
        self,
        producto: str,
        precio_pagado: float,
        precio_promedio: float,
        z_score: float,
        link_orden: str,
    ) -> dict[str, Any]:
        """
        Envía una alerta formateada sobre una compra con sobreprecio.

        Args:
            producto: Nombre del producto/servicio.
            precio_pagado: Precio unitario pagado en la OC sospechosa ($CLP).
            precio_promedio: Precio promedio/mediana histórico del producto ($CLP).
            z_score: Valor Z-score de la anomalía.
            link_orden: URL o código de la orden de compra.

        Returns:
            dict con la respuesta de la API de Telegram.
        """
        # Calcular sobreprecio
        if precio_promedio > 0:
            sobreprecio_pct: float = ((precio_pagado - precio_promedio) / precio_promedio) * 100
        else:
            sobreprecio_pct = 0.0

        # Construir link completo si solo es un código de OC
        if not link_orden.startswith("http"):
            link_orden = f"{MERCADO_PUBLICO_OC_URL}{link_orden}"

        # ── Formatear mensaje MarkdownV2 ──
        esc = self._escape_md
        fmt = self._fmt_clp

        message: str = (
            f"🚨🚨🚨 *ALERTA DE SOBREPRECIO* 🚨🚨🚨\n"
            f"\n"
            f"🏷️ *Producto:*\n"
            f"`{esc(producto)}`\n"
            f"\n"
            f"💰 *Precio pagado:*  {esc(fmt(precio_pagado))} CLP\n"
            f"📊 *Precio promedio:*  {esc(fmt(precio_promedio))} CLP\n"
            f"📈 *Sobreprecio:*  {esc(f'{sobreprecio_pct:+.1f}%')}\n"
            f"🔬 *Z\\-Score:*  {esc(f'{z_score:.2f}')}\n"
            f"\n"
            f"{'🔴' if sobreprecio_pct > 200 else '🟠' if sobreprecio_pct > 100 else '🟡'} "
            f"{'*SOBREPRECIO EXTREMO*' if sobreprecio_pct > 200 else '*SOBREPRECIO ALTO*' if sobreprecio_pct > 100 else '*SOBREPRECIO MODERADO*'}\n"
            f"\n"
            f"🔗 [Ver orden en Mercado Público]({esc(link_orden)})\n"
            f"\n"
            f"─────────────────────────\n"
            f"_🇨🇱 Monitor Ciudadano de Compras Públicas_"
        )

        return self._send_message(message)

    # ═══════════════ Alerta por lote (múltiples anomalías) ═══════════════ #

    def enviar_resumen_diario(
        self,
        fecha: str,
        total_oc: int,
        total_items: int,
        total_anomalias: int,
    ) -> dict[str, Any]:
        """
        Envía un resumen diario del análisis.

        Args:
            fecha: Fecha analizada (formato legible).
            total_oc: Total de OC procesadas.
            total_items: Total de ítems evaluados.
            total_anomalias: Total de anomalías detectadas.
        """
        esc = self._escape_md

        if total_anomalias == 0:
            emoji = "✅"
            estado = "*Sin anomalías detectadas*"
        elif total_anomalias <= 5:
            emoji = "⚠️"
            estado = f"*{total_anomalias} anomalía{'s' if total_anomalias > 1 else ''} detectada{'s' if total_anomalias > 1 else ''}*"
        else:
            emoji = "🚨"
            estado = f"*{total_anomalias} anomalías detectadas*"

        message: str = (
            f"📋 *RESUMEN DIARIO \\- MONITOR COMPRAS* 📋\n"
            f"\n"
            f"📅 *Fecha:* {esc(fecha)}\n"
            f"📦 *OC procesadas:* {esc(str(total_oc))}\n"
            f"🏷️ *Ítems evaluados:* {esc(str(total_items))}\n"
            f"\n"
            f"{emoji} {estado}\n"
            f"\n"
            f"─────────────────────────\n"
            f"_🇨🇱 Monitor Ciudadano de Compras Públicas_"
        )

        return self._send_message(message)
