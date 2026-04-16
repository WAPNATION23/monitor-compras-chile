# Ojo del Pueblo

**Fiscalización ciudadana de compras del Estado de Chile en tiempo real.**

![Beta Pública](https://img.shields.io/badge/Estado-Beta%20Pública-orange)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Tests](https://img.shields.io/badge/Tests-89%20passing-brightgreen)
![CI](https://img.shields.io/github/actions/workflow/status/WAPNATION23/monitor-compras-chile/ci.yml?label=CI)
![Licencia](https://img.shields.io/badge/Licencia-AGPL--3.0-blue)

Plataforma open source que extrae, procesa y analiza órdenes de compra del Mercado Público (ChileCompra) para detectar anomalías estadísticas en el gasto estatal. Pensada para periodistas de investigación, organizaciones de la sociedad civil y ciudadanos interesados en transparencia.

> **Beta pública**: Sistema funcional en desarrollo activo, con suite de 89 tests automatizados.
> Reporta bugs, sugiere mejoras y ayuda a validar datos reales.
> Ver [CONTRIBUTING.md](CONTRIBUTING.md) para empezar.

---

## Estado de madurez

| Módulo | Estado | Descripción |
|---|---|---|
| ETL Mercado Público | **Estable** | Extracción diaria de OC vía API ChileCompra, procesamiento a SQLite |
| Dashboard Streamlit | **Estable** | Panel con KPIs, gráficos, filtros por fecha, tabla de alertas |
| Detector estadístico | **Estable** | Benford, Z-score, IQR, concentración, trato directo |
| Asistente IA (DeepSeek) | **Experimental** | Chat con búsqueda web + consulta local a DB. Requiere API key |
| Notificador Telegram | **Experimental** | Envío de alertas automáticas. Requiere bot token |
| Cruce SERVEL (donaciones) | **Operativo** | Carga CSV real desde servel.cl, cruce con proveedores activo |
| Cruce malla societaria | **Pendiente** | Lógica implementada, requiere scraper real del Diario Oficial |
| Scraper InfoLobby | **Pendiente** | Conector esqueleto, API no disponible públicamente |

---

## Instalación

```bash
git clone https://github.com/WAPNATION23/monitor-compras-chile.git
cd monitor-compras-chile
pip install -r requirements.txt
```

### Configuración

Crea un archivo `.env` en la raíz:

```env
# Obligatorio para el asistente IA
DEEPSEEK_API_KEY=tu_clave

# Obligatorio para extracción de datos
MERCADO_PUBLICO_TICKET=tu_ticket

# Opcional: alertas Telegram
TELEGRAM_TOKEN=tu_token
TELEGRAM_CHAT_ID=tu_chat_id
```

> El ticket de Mercado Público se solicita en https://api.mercadopublico.cl

### Uso

```bash
# Primera ejecución: crea la BD y extrae datos reales
py main.py

# Extraer con límite custom
py main.py --max-oc 1000          # Máximo 1000 OC
py main.py --max-oc 0             # Sin límite

# Backfill multi-día
py main.py --rango-fechas 01042026-07042026

# Backup de la base de datos
py backup.py                      # Crear backup
py backup.py --list               # Ver backups
py backup.py --restore backups/archivo.db

# Lanzar dashboard
streamlit run dashboard.py
```

> **Nota:** `auditoria_estado.db` es un artefacto local generado por el pipeline.
> No se versiona en git. Si clonas el repo, ejecuta `py main.py` o usa el botón
> "Extraer datos" en el dashboard para poblar la base.

---

## Arquitectura

```
main.py              → Orquestador de extracción diaria
extractor.py         → Cliente API Mercado Público con paginación
processor.py         → ETL: aplana OC → SQLite con deduplicación
detector.py          → Análisis estadístico (Benford, Z-score, IQR)
cross_referencer.py  → Cruces forenses entre fuentes de datos
alertas_personas.py  → Motor de alertas "En la Mira" (5 fuentes reales)
generar_expediente.py→ Generador CLI de expedientes por persona/empresa
infolobby_connector.py→ Conector SPARQL + CSV a datos de InfoLobby
dashboard.py         → UI Streamlit (panel, cruces, chat IA, alertas)
chat_service.py      → Asistente IA con contexto DB + web
config.py            → Configuración centralizada (sin secrets)
notifier.py          → Alertas Telegram
backup.py            → Backup/restauración de BD (SQLite backup API)
```

---

## Calidad de datos

La base actual tiene limitaciones conocidas:

- **RUT proveedor**: La API de ChileCompra no siempre devuelve el RUT en el listado de OC. El chat IA lo resuelve consultando la API por OC individual.
- **Fechas vacías**: Algunas OC no traen `FechaCreacion` en el campo esperado. Corregido en el procesador.
- **Nombres de producto**: Algunos ítems vienen sin descripción desde la fuente.

Estas limitaciones están documentadas como issues abiertos para mejora continua.

---

## Tests

```bash
py -m pytest tests/test_core.py -q
```

89 tests cubriendo: procesador (ETL, deduplicación, migración de esquema, validación RUT), detector (IQR, Z-score, Benford), cross_referencer (concentración, rankings, SERVEL), notificador (anti-spam, dedup, truncación), extractor (retry, max_oc), chat service (errores DB, timeout, rate limit), dashboard (KPIs, filtros, formato CLP, radar forense), configuración, backup (create/restore/list).

---

## Advertencia legal

Los análisis presentados son hallazgos estadísticos y matemáticos (anomalías de facturación, concentración de trato directo). **No constituyen imputaciones de delitos ni reemplazan el debido proceso.** Toda persona o empresa mencionada goza de presunción de inocencia. Herramienta de uso cívico, investigativo y educacional.

---

## Contribuir

Ver [CONTRIBUTING.md](CONTRIBUTING.md) para guía de contribución.

**Roadmap abierto:**
- [x] Separar dashboard en módulos (`queries.py`, `chat_service.py`, `dashboard.py`)
- [x] Logging estructurado JSON con métricas de pipeline
- [x] Auditoría de seguridad (SQL injection, secrets, deduplicación)
- [x] Migración automática de esquema SQLite para BD existentes
- [ ] Mejorar cobertura de `rut_proveedor` en la extracción
- [ ] Implementar scraper real del Diario Oficial (sociedades)
- [x] Conectar datos reales de SERVEL para cruce electoral
- [ ] Migrar storage a PostgreSQL para producción
- [x] CI/CD con GitHub Actions (tests + lint automático)
- [x] Backup y restauración de base de datos

---

## Licencia

[AGPL-3.0](LICENSE) — Si usas o modificas este código, debes publicar tu versión.
