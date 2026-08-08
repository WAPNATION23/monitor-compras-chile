"""
Conversión de monedas a CLP para normalizar montos de Mercado Público.

Usa la API pública mindicador.cl (sin API key). Cache en memoria por día.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

import requests

from config import REQUEST_TIMEOUT

logger = logging.getLogger(__name__)

# Cache: (moneda, fecha_iso) -> valor en CLP por unidad
_RATE_CACHE: dict[tuple[str, str], float] = {}

MINDICADOR_URL = "https://mindicador.cl/api/{moneda}/{fecha}"
SUPPORTED = {"CLP", "UF", "USD", "EUR", "UTM", "UTA"}


def _parse_fecha(fecha_str: str) -> date | None:
    if not fecha_str:
        return None
    raw = str(fecha_str)[:10]
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def get_rate(moneda: str, fecha: date | None = None) -> float:
    """
    Retorna cuántos CLP vale 1 unidad de `moneda` en `fecha`.
    CLP retorna 1.0. Si falla la API, retorna 1.0 y loguea warning.
    """
    moneda = (moneda or "CLP").upper().strip()
    if moneda in ("", "CLP", "PESO", "PESOS"):
        return 1.0

    ref = fecha or date.today()
    key = (moneda, ref.isoformat())
    if key in _RATE_CACHE:
        return _RATE_CACHE[key]

    url = MINDICADOR_URL.format(moneda=moneda.lower(), fecha=ref.strftime("%d-%m-%Y"))
    try:
        r = requests.get(url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            data: dict[str, Any] = r.json()
            serie = data.get("serie") or []
            if serie:
                rate = float(serie[0].get("valor", 1))
                _RATE_CACHE[key] = rate
                return rate
    except (requests.RequestException, ValueError, TypeError) as exc:
        logger.warning("No se pudo obtener tasa %s para %s: %s", moneda, ref, exc)

    # Fallback: intentar hoy sin fecha
    if fecha is not None:
        return get_rate(moneda, None)

    _RATE_CACHE[key] = 1.0
    return 1.0


def to_clp(monto: float, moneda: str, fecha_str: str = "") -> float:
    """Convierte monto en moneda original a CLP."""
    if monto is None or monto <= 0:
        return 0.0
    ref = _parse_fecha(fecha_str)
    rate = get_rate(moneda, ref)
    return float(monto) * rate
