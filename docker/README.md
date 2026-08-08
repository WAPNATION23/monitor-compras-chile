# Docker — Ojo del Pueblo

Ver [DEPLOY.md](../DEPLOY.md) para la guía completa.

```bash
cp .env.example .env
cp docker-compose.override.example.yml docker-compose.override.yml   # opcional: ./data con tu BD
docker compose up -d --build
```

Servicios: `web` (8501), `cron` (04:00 CLT), `caddy` (perfil `production`).
