#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PRODI_FORCE_DRY_RUN="${PRODI_FORCE_DRY_RUN:-1}"

mapfile -t CONFIGS < <(find "${ROOT_DIR}/cfg" -maxdepth 2 -name '*.prod.yml' -print | sort)

if [[ ${#CONFIGS[@]} -eq 0 ]]; then
  echo "[smoke-prod] No production configurations found" >&2
  exit 1
fi

for cfg in "${CONFIGS[@]}"; do
  layer="$(basename "$(dirname "${cfg}")")"
  echo "[smoke-prod] Validating ${layer} -> ${cfg}" >&2
  prodi run-layer "${layer}" -c "${cfg}"
done
