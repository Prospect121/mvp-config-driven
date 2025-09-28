SHELL := /bin/bash
NET := mvp-config-driven-net
ALIAS := local
ENDPOINT := http://minio:9000
AK ?= minio
SK ?= minio12345
WORKDIR := $(shell pwd)

.PHONY: up seed run logs ps down clean mc-alias mc-buckets mc-cp

up:
	docker compose up -d --build

mc-alias:
	mkdir -p .mc
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

seed: up mc-buckets mc-cp

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
