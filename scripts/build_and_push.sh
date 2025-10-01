#!/usr/bin/env bash
set -euo pipefail

RG="$(terraform -chdir=infra/envs/dev output -raw rg_name)"
ACR="$(terraform -chdir=infra/envs/dev output -raw acr_login_server)"

az acr login --name "${ACR%%.*}"

docker build -t "${ACR}/runner:latest" -f docker/Dockerfile.runner .
docker push "${ACR}/runner:latest"

# Actualiza la imagen en ACI si el nombre difiere
