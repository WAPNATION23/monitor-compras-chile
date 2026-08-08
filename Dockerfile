# Ojo del Pueblo — imagen única para dashboard (web) y pipeline (cron).
FROM python:3.11-slim-bookworm

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Cron para contenedores (supercronic)
ARG SUPERCRONIC_VERSION=v0.2.33
RUN curl -fsSLo /usr/local/bin/supercronic \
    "https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/supercronic-linux-amd64" \
    && chmod +x /usr/local/bin/supercronic

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN sed -i 's/\r$//' /app/docker/entrypoint.sh /app/scripts/cron_update.sh /app/docker/crontab \
    && mkdir -p /data \
    && chmod +x /app/docker/entrypoint.sh \
    && useradd --create-home --uid 1000 ojo \
    && chown -R ojo:ojo /app /data

ENV OJO_DATA_DIR=/data \
    PYTHONUNBUFFERED=1 \
    DISABLE_STREAMLIT_SCHEDULER=1 \
    PYTHONDONTWRITEBYTECODE=1

EXPOSE 8501

# Root en arranque: Railway monta volúmenes root-owned; entrypoint hace chown y baja a ojo.
USER root

ENTRYPOINT ["/app/docker/entrypoint.sh"]
CMD ["web"]
