#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <layer>" >&2
  exit 1
fi

layer="$1"
case "$layer" in
  raw|bronze|silver|gold) ;;
  *)
    echo "Unsupported layer: $layer" >&2
    exit 1
    ;;
esac

cfg_base="${AWS_PRODI_CFG_BASE:-s3://datalake-artifacts/cfg}"
job_prefix="${AWS_PRODI_JOB_PREFIX:-prodi-layer}"
config_uri="${cfg_base}/${layer}/aws.prod.yml"
job_name="${job_prefix}-${layer}"

arguments=$(python - <<PY
import json
import os
payload = {"--layer": "${layer}", "--config": "${config_uri}"}
if os.getenv("FORCE_DRY_RUN", "0").lower() not in {"0", "false", ""}:
    payload["--env.PRODI_FORCE_DRY_RUN"] = "true"
print(json.dumps(payload))
PY
)

echo "[aws-glue] starting job ${job_name} with config ${config_uri}" >&2
aws glue start-job-run --job-name "${job_name}" --arguments "${arguments}"
