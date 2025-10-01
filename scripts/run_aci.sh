#!/usr/bin/env bash
set -euo pipefail

RG=$(terraform -chdir=infra/envs/dev output -raw rg_name)
ACI=$(terraform -chdir=infra/envs/dev output -raw aci_name)

# RUN_DATE opcional: export RUN_DATE=2025/09/26
CMD='spark-submit --master local[*] /app/pipelines/spark_job.py /app/config/datasets/finanzas/payments_v1/dataset.yml /app/config/env.yml'

az container exec -g "$RG" -n "$ACI" --container-name runner --exec-command "/bin/sh -lc '$CMD'"
