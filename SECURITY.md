# Política de Seguridad

## Versiones soportadas

Actualmente se da soporte de seguridad a la rama `main` (beta pública).

## Reportar una vulnerabilidad

Si encuentras una vulnerabilidad en Ojo del Pueblo, **no abras un issue público**.

Reporta privadamente a los mantenedores mediante:

- GitHub Security Advisories: https://github.com/WAPNATION23/monitor-compras-chile/security/advisories/new
- O bien un issue marcado como `security` si el repositorio no tiene advisories habilitados, incluyendo lo mínimo indispensable para reproducir.

Compromiso de respuesta:

- Acuse de recibo en ≤ 7 días hábiles.
- Evaluación y plan de mitigación en ≤ 30 días.
- Disclosure coordinado una vez publicado el fix.

## Alcance

Aplica a todo el código del repositorio, incluyendo:

- Pipeline de extracción y procesamiento (`main.py`, `extractor.py`, `processor.py`).
- Motor de detección y cruces (`detector.py`, `cross_referencer.py`).
- Dashboard Streamlit (`dashboard.py`, `queries.py`, `chat_service.py`).
- Workflows de CI/CD en `.github/workflows/`.

## Fuera de alcance

- Vulnerabilidades en dependencias de terceros (reportar upstream).
- Disponibilidad de APIs públicas externas (Mercado Público, SERVEL, etc.).
- Ataques que requieran control previo del host/entorno del usuario.

## Buenas prácticas esperadas

- Nunca commitear secretos (tokens, API keys). Usar variables de entorno.
- TLS habilitado por defecto en toda ingesta de datos externos.
- Sanitizar entradas antes de ejecutar queries SQL/SPARQL.
- Rate-limiting activo en endpoints públicos y chat IA.
