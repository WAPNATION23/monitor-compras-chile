# Guía de despliegue

## Streamlit Community Cloud (recomendado para la web pública)

1. Fork / push del repo a GitHub.
2. En https://share.streamlit.io crear una nueva app apuntando a `dashboard.py`.
3. En **Settings → Secrets** pegar:
   ```toml
   MERCADO_PUBLICO_TICKET = "..."
   DEEPSEEK_API_KEY = "..."
   TELEGRAM_TOKEN = "..."       # opcional
   TELEGRAM_CHAT_ID = "..."     # opcional
   ```
4. La base `auditoria_estado.db` se genera al primer `main.py` o desde el botón "Extraer datos" del dashboard.

## GitHub Actions (pipeline diario)

- Workflow: `.github/workflows/diario.yml`
- Trigger: cron diario 04:00 CLT + manual.
- Artefacto publicado: `auditoria-estado-db` (retención 7 días).
- **No** se hace push de la BD a `main`. Para publicar snapshots, descargar el artefacto vía `actions/download-artifact@v4`.

## Ejecución local

```bash
pip install -r requirements.txt     # rangos (dev)
# o con lockfile (prod/CI reproducible):
pip install -r requirements.lock

cp .env.example .env                # editar con tus secretos
py main.py                          # primera extracción
streamlit run dashboard.py          # levantar dashboard
```

## Persistencia a mediano plazo (PostgreSQL)

El diseño actual usa SQLite local. Para operación continua y multiusuario:

- Reemplazar `sqlite3.connect(DB_NAME)` por un pool (`psycopg2` / `asyncpg`).
- Migrar schemas con Alembic.
- Exponer DSN vía `DATABASE_URL`.
- Correr extractor en contenedor separado del dashboard.

Issue de seguimiento: ver roadmap en `README.md`.
