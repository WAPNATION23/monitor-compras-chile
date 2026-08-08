.PHONY: up down build logs update shell test-docker

up:
	docker compose up -d --build

down:
	docker compose down

build:
	docker compose build

logs:
	docker compose logs -f web cron

update:
	docker compose run --rm web update

shell:
	docker compose run --rm web shell

# Migrar BD local al volumen ./data (Windows PowerShell compatible via compose override)
migrate-db:
	mkdir -p data 2>/dev/null || mkdir data
	@echo "Copia manual: auditoria_estado.db -> ./data/"

production:
	docker compose --profile production up -d --build
