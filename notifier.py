"""
TelegramNotifier
════════════════
Envía alertas de anomalías en compras públicas a un chat/canal de Telegram
usando la API HTTP pura (sin dependencias extra, solo requests).

Formato de mensajes: HTML con emojis para máxima legibilidad.

Protecciones anti-spam:
  • Máximo de alertas individuales por ejecución (MAX_ALERTS_PER_RUN).
  • Rate limiting entre mensajes (DELAY_BETWEEN_MESSAGES).
  • Deduplicación: no envía la misma OC dos veces en la misma ejecución.
  • Si hay más alertas que el máximo, envía un resumen consolidado.
"""

from __future__ import annotations

import logging
import time
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

# ──────────────────── Protecciones anti-spam ──────────────────── #
MAX_ALERTS_PER_RUN: int = 10          # Máximo de alertas individuales por ejecución
DELAY_BETWEEN_MESSAGES: float = 1.5   # Segundos entre cada mensaje (Telegram rate limit: 30/seg)
MAX_MESSAGE_LENGTH: int = 4096        # Límite de Telegram para un mensaje


class TelegramNotifier:
    """Envía alertas formateadas a Telegram vía API HTTP con protección anti-spam."""

    def __init__(
        self,
        token: str = TELEGRAM_BOT_TOKEN,
        chat_id: str = TELEGRAM_CHAT_ID,
    ) -> None:
        if not token or not chat_id:
            raise ValueError(
                "TELEGRAM_BOT_TOKEN y TELEGRAM_CHAT_ID deben estar configurados. "
                "Usa variables de entorno TELEGRAM_TOKEN y TELEGRAM_CHAT_ID."
            )
        self.token = token
        self.chat_id = chat_id
        self._api_url: str = _TG_API_BASE.format(token=self.token)
        self.session = requests.Session()

        # Anti-spam: tracking por ejecución
        self._alerts_sent: int = 0
        self._oc_codes_sent: set[str] = set()

    # ──────────────── Helper: escapar HTML ──────────────── #

    @staticmethod
    def _escape_html(text: str) -> str:
        """Escapa caracteres especiales para HTML en Telegram."""
        return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    # ──────────── Helper: formatear moneda CLP ──────────────── #

    @staticmethod
    def _fmt_clp(valor: float) -> str:
        """Formatea un número como moneda chilena."""
        return f"${valor:,.0f}".replace(",", ".")

    # ──────────────── Envío genérico de mensaje ──────────────── #

    def _send_message(self, text: str, parse_mode: str = "HTML") -> dict[str, Any]:
        """
        Envía un mensaje de texto al chat configurado con rate limiting.
        """
        # Truncar si excede el límite de Telegram
        if len(text) > MAX_MESSAGE_LENGTH:
            text = text[:MAX_MESSAGE_LENGTH - 20] + "\n\n[...truncado]"

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

            # Rate limiting: esperar entre mensajes
            time.sleep(DELAY_BETWEEN_MESSAGES)
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

    # ──────────────── Check anti-spam ──────────────── #

    def _can_send_alert(self, codigo_oc: str) -> bool:
        """Verifica si podemos enviar una alerta más."""
        if self._alerts_sent >= MAX_ALERTS_PER_RUN:
            logger.warning(
                "Límite de alertas alcanzado (%d/%d). Omitiendo OC %s.",
                self._alerts_sent, MAX_ALERTS_PER_RUN, codigo_oc,
            )
            return False

        if codigo_oc in self._oc_codes_sent:
            logger.debug("OC %s ya fue alertada en esta ejecución. Omitiendo.", codigo_oc)
            return False

        return True

    # ═══════════════ MÉTODO PRINCIPAL: Alerta de Desfalco ═══════════════ #

    def enviar_alerta_desfalco(
        self,
        producto: str,
        comprador: str,
        precio_pagado: float,
        precio_promedio: float,
        z_score: float,
        link_orden: str,
        categoria_riesgo: str = "GENERAL",
    ) -> dict[str, Any] | None:
        """
        Envía una alerta estructurada en HTML sobre una compra con sobreprecio.
        Incluye protección anti-spam.

        Returns:
            Resultado de la API de Telegram, o None si fue omitida por anti-spam.
        """
        # Anti-spam check
        codigo_oc = link_orden if not link_orden.startswith("http") else link_orden.split("=")[-1]
        if not self._can_send_alert(codigo_oc):
            return None

        # Calcular sobreprecio
        if precio_promedio > 0:
            sobreprecio_pct: float = ((precio_pagado - precio_promedio) / precio_promedio) * 100
        else:
            sobreprecio_pct = 0.0

        # Construir link completo si solo es un código de OC
        if not link_orden.startswith("http"):
            link_orden = f"{MERCADO_PUBLICO_OC_URL}{link_orden}"

        # Mapeo de Emojis por Categoría
        emojis = {
            "MUNICIPALIDAD": "🏛️",
            "FUERZAS ARMADAS/ORDEN": "🚓",
            "ALERTA FUNDACIONES/TRATO DIRECTO": "🚨💰",
            "MOP/OBRAS": "🚧",
            "GENERAL": "📄"
        }
        emoji_categoria = emojis.get(categoria_riesgo, "📄")

        # Nivel de severidad
        if sobreprecio_pct > 200:
            nivel_alerta = '🔴 <b>SOBREPRECIO EXTREMO</b>'
        elif sobreprecio_pct > 100:
            nivel_alerta = '🟠 <b>SOBREPRECIO ALTO</b>'
        else:
            nivel_alerta = '🟡 <b>SOBREPRECIO MODERADO</b>'

        # ── Formatear mensaje HTML ──
        esc = self._escape_html
        fmt = self._fmt_clp

        message: str = (
            f"🚨 <b>ALERTA DE SOBREPRECIO</b> 🚨\n"
            f"\n"
            f"{emoji_categoria} <b>Categoría:</b> {esc(categoria_riesgo)}\n"
            f"🏢 <b>Comprador:</b> {esc(comprador)}\n"
            f"🏷️ <b>Producto:</b> <code>{esc(producto)}</code>\n"
            f"\n"
            f"💰 <b>Precio pagado:</b> {esc(fmt(precio_pagado))} CLP\n"
            f"📊 <b>Precio promedio:</b> {esc(fmt(precio_promedio))} CLP\n"
            f"📈 <b>Sobreprecio:</b> {esc(f'{sobreprecio_pct:+.1f}%')}\n"
            f"🔬 <b>Z-Score:</b> {esc(f'{z_score:.2f}')}\n"
            f"\n"
            f"{nivel_alerta}\n"
            f"\n"
            f'🔗 <a href="{esc(link_orden)}">Ver orden en Mercado Público</a>\n'
            f"\n"
            f"─────────────────────────\n"
            f"<i>🇨🇱 Monitor Ciudadano de Compras Públicas</i>"
        )

        result = self._send_message(message, parse_mode="HTML")

        # Registrar como enviada
        self._alerts_sent += 1
        self._oc_codes_sent.add(codigo_oc)

        return result

    # ═══════════════ ALERTA: TRATO DIRECTO MASIVO ═══════════════ #

    def enviar_alerta_trato_directo(
        self,
        comprador: str,
        proveedor: str,
        monto_pagado: float,
        producto_o_servicio: str,
        link_orden: str,
    ) -> dict[str, Any] | None:
        """Envía una alerta cuando se detecta un Trato Directo por montos exorbitantes."""
        codigo_oc = link_orden if not link_orden.startswith("http") else link_orden.split("=")[-1]
        if not self._can_send_alert("TD_" + codigo_oc):
            return None

        if not link_orden.startswith("http"):
            link_orden = f"{MERCADO_PUBLICO_OC_URL}{link_orden}"

        esc = self._escape_html
        fmt = self._fmt_clp

        message: str = (
            f"🚨💰 <b>ALERTA DE TRATO DIRECTO EXTREMO</b> 🚨💰\n"
            f"\n"
            f"🏢 <b>Institución:</b> {esc(comprador)}\n"
            f"🤝 <b>Beneficiario:</b> <code>{esc(proveedor)}</code>\n"
            f"📦 <b>Motivo/Objeto:</b> {esc(producto_o_servicio)}\n"
            f"\n"
            f"💵 <b>Monto Adjudicado a dedo:</b> <b>{esc(fmt(monto_pagado))} CLP</b>\n"
            f"\n"
            f"⚠️ <i>Monto supera el umbral crítico para saltarse licitación.</i>\n"
            f"\n"
            f'🔗 <a href="{esc(link_orden)}">Auditar orden en Mercado Público</a>\n'
            f"\n"
            f"─────────────────────────\n"
            f"<i>🇨🇱 Monitor Ciudadano de Compras Públicas</i>"
        )

        result = self._send_message(message, parse_mode="HTML")
        self._alerts_sent += 1
        self._oc_codes_sent.add("TD_" + codigo_oc)
        return result

    # ═══════════════ ALERTA: CORRUPCIÓN POLÍTICA (SERVEL) ═══════════════ #

    def enviar_alerta_servel(
        self,
        proveedor: str,
        politico_partido: str,
        inversion_electoral: float,
        monto_adjudicado: float,
        organismo_comprador: str,
    ) -> dict[str, Any] | None:
        """Alerta de hallazgo de correlación entre donante electoral y ganador de Trato Estatal."""
        
        # Como estas alertas son raras, no queremos filtrarlas fácilmente, pero si proteger el spam.
        codigo_unico = f"SRV_{proveedor[:5]}_{monto_adjudicado}"
        if not self._can_send_alert(codigo_unico):
            return None

        esc = self._escape_html
        fmt = self._fmt_clp

        message: str = (
            f"🏛️🕵️‍♂️ <b>¡ALERTA DE POSIBLE RED POLÍTICA-COMERCIAL!</b> 🕵️‍♂️🏛️\n"
            f"\n"
            f"Se ha detectado una empresa que inyectó capital al sistema político "
            f"y acaba de recuperar su dinero vía contratos públicos.\n"
            f"\n"
            f"🏢 <b>Empresa / Donante:</b> <code>{esc(proveedor)}</code>\n"
            f"🗳️ <b>Político o Partido Fondeado:</b> {esc(politico_partido)}\n"
            f"💸 <b>Donación Electoral (SERVEL):</b> {esc(fmt(inversion_electoral))} CLP\n"
            f"\n"
            f"🏦 <b>Organismo que le pagó:</b> {esc(organismo_comprador)}\n"
            f"💰 <b>Retorno vía Adjudicaciones:</b> <b>{esc(fmt(monto_adjudicado))} CLP</b>\n"
            f"\n"
            f"⚠️ <i>Evidencia de devolución de favores políticos detectada algorítmicamente.</i>\n"
            f"\n"
            f"─────────────────────────\n"
            f"<i>🇨🇱 Monitor Ciudadano de Compras Públicas</i>"
        )

        result = self._send_message(message, parse_mode="HTML")
        self._alerts_sent += 1
        self._oc_codes_sent.add(codigo_unico)
        return result

    # ═══════════════ Resumen diario ═══════════════ #

    def enviar_resumen_diario(
        self,
        fecha: str,
        total_oc: int,
        total_items: int,
        total_anomalias: int,
        alertas_enviadas: int | None = None,
        alertas_omitidas: int = 0,
    ) -> dict[str, Any]:
        """
        Envía un resumen diario del análisis en HTML.
        Incluye cuántas alertas se enviaron vs omitidas por anti-spam.
        """
        esc = self._escape_html

        if total_anomalias == 0:
            emoji = "✅"
            estado = "<b>Sin anomalías detectadas</b>"
        elif total_anomalias <= 5:
            emoji = "⚠️"
            estado = f"<b>{total_anomalias} anomalía{'s' if total_anomalias > 1 else ''} detectada{'s' if total_anomalias > 1 else ''}</b>"
        else:
            emoji = "🚨"
            estado = f"<b>{total_anomalias} anomalías detectadas</b>"

        # Info sobre alertas enviadas
        if alertas_enviadas is None:
            alertas_enviadas = self._alerts_sent
        alertas_omitidas = max(0, total_anomalias - alertas_enviadas)

        alerta_info = ""
        if alertas_omitidas > 0:
            alerta_info = (
                f"\n📤 <b>Alertas enviadas:</b> {alertas_enviadas}\n"
                f"⏭️ <b>Omitidas (anti-spam):</b> {alertas_omitidas}"
            )

        message: str = (
            f"📋 <b>RESUMEN DIARIO - MONITOR COMPRAS</b> 📋\n"
            f"\n"
            f"📅 <b>Fecha:</b> {esc(fecha)}\n"
            f"📦 <b>OC procesadas:</b> {esc(str(total_oc))}\n"
            f"🏷️ <b>Ítems evaluados:</b> {esc(str(total_items))}\n"
            f"\n"
            f"{emoji} {estado}"
            f"{alerta_info}\n"
            f"\n"
            f"─────────────────────────\n"
            f"<i>🇨🇱 Monitor Ciudadano de Compras Públicas</i>"
        )

        return self._send_message(message, parse_mode="HTML")

    # ═══════════════ Stats de la sesión ═══════════════ #

    @property
    def alerts_sent(self) -> int:
        """Cantidad de alertas enviadas en esta sesión."""
        return self._alerts_sent

    @property
    def alerts_remaining(self) -> int:
        """Capacidad restante de alertas."""
        return max(0, MAX_ALERTS_PER_RUN - self._alerts_sent)
