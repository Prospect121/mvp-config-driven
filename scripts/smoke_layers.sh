#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

prodi run-layer raw -c "${ROOT_DIR}/cfg/raw/example.yml"
prodi run-layer bronze -c "${ROOT_DIR}/cfg/bronze/example.yml"
prodi run-layer silver -c "${ROOT_DIR}/cfg/silver/example.yml"
prodi run-layer gold -c "${ROOT_DIR}/cfg/gold/example.yml"
prodi run-pipeline -p "${ROOT_DIR}/cfg/pipelines/example.yml"
