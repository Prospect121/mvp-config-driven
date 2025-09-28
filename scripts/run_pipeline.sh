#!/usr/bin/env bash
set -euo pipefail
docker compose up -d --build
./scripts/seed.sh
docker compose run --rm runner
