#!/usr/bin/env bash
set -euo pipefail

echo "[ci] Validando config YAML/JSON…"
yamllint config/ || { echo "YAML inválido"; exit 1; }
jq empty config/datasets/finanzas/payments_v1/schema.json || { echo "JSON inválido"; exit 1; }

echo "[ci] OK configs"
