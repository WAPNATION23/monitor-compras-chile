# Guía de despliegue

## Docker (recomendado — VPS, local, Railway con Dockerfile)

Stack: **web** (Streamlit) + **cron** (actualización diaria) + volumen persistente `/data`.
La misma imagen, misma BD, cero reescritura de lógica.

### Requisitos

- Docker 24+ y Docker Compose v2
- Archivo `.env` con secretos (copiar de `.env.example`)

### Arranque rápido (BD nueva → seed incluido)

```bash
cp .env.example .env          # editar MERCADO_PUBLICO_TICKET, DEEPSEEK_API_KEY
docker compose up -d --build
# Dashboard: http://localhost:8501
```

### Migrar tu BD local existente (sin perder datos)

```bash
mkdir data
cp auditoria_estado.db data/           # Linux/macOS
# Windows: copy auditoria_estado.db data\

cp docker-compose.override.example.yml docker-compose.override.yml
docker compose up -d --build
```

El volumen `./data` reemplaza el volumen nombrado y monta tu `auditoria_estado.db` tal cual.

### Comandos útiles

```bash
docker compose logs -f web cron       # logs
docker compose run --rm web update    # pipeline manual (--full --force)
docker compose exec web bash          # shell dentro del contenedor
docker compose down                   # parar (datos en volumen se conservan)
```

Con Makefile: `make up`, `make logs`, `make update`.

### Servicios

| Servicio | Rol | Puerto |
|----------|-----|--------|
| `web` | Dashboard Streamlit + Conan | 8501 |
| `cron` | `daily_update.py --full` a las 04:00 CLT | — |
| `caddy` (perfil `production`) | HTTPS + dominio | 80/443 |

Producción con TLS:

```bash
OJO_DOMAIN=monitor.wapnation.cl docker compose --profile production up -d --build
```

Apuntar DNS (A o CNAME) al VPS. Caddy obtiene certificado Let's Encrypt solo.

### Variables de entorno

| Variable | Default | Uso |
|----------|---------|-----|
| `OJO_DATA_DIR` | `/data` | Ruta BD + `last_update.json` + `cron.log` |
| `DISABLE_STREAMLIT_SCHEDULER` | `1` | Cron vive en contenedor aparte |
| `DAILY_UPDATE_MAX_OC` | `5000` | OCs por día en pipeline |
| `MERCADO_PUBLICO_TICKET` | — | Obligatorio para datos |
| `DEEPSEEK_API_KEY` | — | Obligatorio para Conan |

### VPS (Hetzner u otro)

1. Instalar Docker en el servidor
2. Clonar repo + `.env`
3. Copiar `auditoria_estado.db` a `data/` (o dejar que seed arranque vacío)
4. `docker compose --profile production up -d --build`
5. Firewall: abrir 80/443 (no hace falta exponer 8501 públicamente)

---

## Railway (Hobby) — producción rápida

Requiere plan **Hobby** (~US$5/mes). El trial free ya no sirve.

### 0. Subir código a GitHub

Los cambios Docker/mejoras deben estar en `main` del repo
`WAPNATION23/monitor-compras-chile` (commit + push).

### 1. Proyecto y servicio web

1. [railway.app](https://railway.app) → New Project → **Deploy from GitHub repo**
2. Elige `monitor-compras-chile` (branch `main`)
3. Settings del servicio:
   - Builder: **Dockerfile** (usa `railway.toml`)
   - Public Networking: Generate Domain (temporal) + Custom Domain `monitor.wapnation.cl`
4. **Variables** (Variables tab):

| Variable | Valor |
|----------|--------|
| `MERCADO_PUBLICO_TICKET` | tu ticket |
| `DEEPSEEK_API_KEY` | tu key |
| `OJO_DATA_DIR` | `/data` |
| `DISABLE_STREAMLIT_SCHEDULER` | `1` |
| `TELEGRAM_TOKEN` / `TELEGRAM_CHAT_ID` | opcionales |

5. **Volume**: Add Volume → mount path `/data` (persistir BD)
6. Deploy. Healthcheck: `/_stcore/health`

### 2. Servicio cron (actualización diaria)

1. En el mismo proyecto → **Add Service** → mismo repo / misma imagen
2. Settings → **Cron Schedule**: `0 7 * * *` (04:00 Chile)
3. **Custom Start Command**: `update`
4. Mismas variables + **mismo volumen** montado en `/data`

### 3. Dominio en NIC Chile

1. En Railway → Custom Domain → copia el valor CNAME que te dan
2. En NIC Chile (zona DNS de `wapnation.cl`):

| Tipo | Nombre | Valor |
|------|--------|--------|
| CNAME | `monitor` | el target que muestra Railway (ej. `xxx.up.railway.app`) |

Espera propagación (minutos a unas horas). Railway emite HTTPS solo.

### 4. Cargar la BD (primera vez)

El `.dockerignore` no mete `*.db` en la imagen. Opciones:

- Dejar volumen vacío → el entrypoint crea esquema → en el servicio web:
  **Settings → Run command once** / one-off: start command `update`
  (tarda; descarga OCs frescas), **o**
- Subir tu `auditoria_estado.db` al volumen (Railway CLI / panel de volumen si está disponible)

### Checklist Railway

1. Deploy web verde + healthcheck OK
2. `https://monitor.wapnation.cl` carga el dashboard
3. Cron programado a las 07:00 UTC
4. Tras un `update`, KPIs con datos reales

---


## GitHub Actions (backup pipeline)

- Workflow: `.github/workflows/diario.yml`
- Ejecuta `daily_update.py --full --force`
- Publica `auditoria_estado.db` como artefacto (7 días)

---

## Ejecución local sin Docker

```bash
pip install -r requirements.txt
cp .env.example .env
python daily_update.py --full --force
streamlit run dashboard.py
```

---

## Checklist post-despliegue

1. `docker compose ps` → web healthy, cron running
2. `data/last_update.json` o volumen con timestamp reciente
3. Dashboard carga KPIs
4. Conan responde con organismo real (no solo «Bienes y Servicios»)
5. `docker compose logs cron` tras las 04:00 CLT → `[daily_update] fin`
