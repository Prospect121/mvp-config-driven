SHELL := /bin/bash
NET := mvp-config-driven-net
ALIAS := local
ENDPOINT := http://minio:9000
AK ?= minio
SK ?= minio12345
WORKDIR := $(shell pwd)
SEED_DATE ?= 2025/09/26

.PHONY: up seed run logs ps down clean mc-alias mc-buckets mc-cp

up:
	docker compose up -d --build

mc-alias:
	rm -rf $(WORKDIR)/.mc
	mkdir -p $(WORKDIR)/.mc
	docker run --rm --network $(NET) \
	  -v "$(WORKDIR):/mvp" \
	  -v "$(WORKDIR)/.mc:/root/.mc" \
	  minio/mc:latest alias set $(ALIAS) $(ENDPOINT) $(AK) $(SK)

mc-buckets: mc-alias
	docker run --rm --network $(NET) \
	  -v "$(WORKDIR):/mvp" \
	  -v "$(WORKDIR)/.mc:/root/.mc" \
	  minio/mc:latest mb -p $(ALIAS)/raw || true
	docker run --rm --network $(NET) \
	  -v "$(WORKDIR):/mvp" \
	  -v "$(WORKDIR)/.mc:/root/.mc" \
	  minio/mc:latest mb -p $(ALIAS)/silver || true

mc-cp:
	docker run --rm --network $(NET) \
	  -v "$(WORKDIR):/mvp" \
	  -v "$(WORKDIR)/.mc:/root/.mc" \
	  minio/mc:latest cp /mvp/data/raw/payments/sample.csv $(ALIAS)/raw/payments/2025/09/26/sample.csv

seed:
	@DATE=$(SEED_DATE) ./scripts/seed.sh

seed-today:
	@DATE=$$(date +%Y/%m/%d) ./scripts/seed.sh

# --- BIG DATASET ---

# Copia el big_sample.csv a MinIO (bucket raw) en una fecha separada
seed-big: up
	@echo ">> Seedeando big_sample.csv a MinIO (raw)"
	docker run --rm --network $(NET) \
	  -v "$(WORKDIR):/mvp" \
	  -v "$(WORKDIR)/.mc:/root/.mc" \
	  minio/mc:latest cp /mvp/data/raw/payments/big_sample.csv local/raw/payments/2025/09/28/big_sample.csv

# Ejecuta el pipeline usando el config alterno dataset.big.yml
run-big:
	docker compose run --rm \
	  -e CFG=/mvp/config/datasets/finanzas/payments_v1/dataset.big.yml \
	  -e ENVF=/mvp/config/env.yml \
	  runner


run:
	docker compose run --rm runner

logs:
	docker compose logs -f

ps:
	docker compose ps

down:
	docker compose down -v

clean: down
	rm -rf .mc _minio
