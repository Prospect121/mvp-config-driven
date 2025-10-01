SHELL := /bin/bash
NET := mvp-config-driven-net
ALIAS := local
ENDPOINT := http://minio:9000
AK ?= minio
SK ?= minio12345
WORKDIR := $(shell pwd)

.PHONY: up setup-dev test logs ps down clean mc-alias mc-buckets help

# Default target
help:
	@echo "Available commands:"
	@echo "  up           - Start all services with Docker Compose"
	@echo "  setup-dev    - Setup development environment (MinIO buckets)"
	@echo "  test         - Run tests"
	@echo "  logs         - Show container logs"
	@echo "  ps           - Show running containers"
	@echo "  down         - Stop all services"
	@echo "  clean        - Stop services and clean up volumes"

# Start all services
up:
	docker compose up -d --build

# Setup development environment
setup-dev: up mc-buckets
	@echo "Development environment ready!"

# Setup MinIO alias
mc-alias:
	rm -rf $(WORKDIR)/.mc
	mkdir -p $(WORKDIR)/.mc
	docker run --rm --network $(NET) \
	  -v "$(WORKDIR):/mvp" \
	  -v "$(WORKDIR)/.mc:/root/.mc" \
	  minio/mc:latest alias set $(ALIAS) $(ENDPOINT) $(AK) $(SK)

# Create MinIO buckets
mc-buckets: mc-alias
	docker run --rm --network $(NET) \
	  -v "$(WORKDIR):/mvp" \
	  -v "$(WORKDIR)/.mc:/root/.mc" \
	  minio/mc:latest mb -p $(ALIAS)/raw || true
	docker run --rm --network $(NET) \
	  -v "$(WORKDIR):/mvp" \
	  -v "$(WORKDIR)/.mc:/root/.mc" \
	  minio/mc:latest mb -p $(ALIAS)/silver || true
	docker run --rm --network $(NET) \
	  -v "$(WORKDIR):/mvp" \
	  -v "$(WORKDIR)/.mc:/root/.mc" \
	  minio/mc:latest mb -p $(ALIAS)/gold || true

# Run tests
test:
	python -m pytest tests/ -v

# Show logs
logs:
	docker compose logs -f

# Show running containers
ps:
	docker compose ps

# Stop all services
down:
	docker compose down -v

# Clean up everything
clean: down
	rm -rf .mc _minio
	docker system prune -f
