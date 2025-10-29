# Azure Deployment Guide

Follow this guide to deploy the MVP configuration-driven platform to Microsoft Azure using the provided CLI tooling.

## Prerequisites

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
brew install azure-cli || curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
```

Install optional infrastructure tooling:

```bash
brew install terraform || sudo apt-get install -y terraform
```

## Authentication

1. Log in with Azure CLI:
   ```bash
   az login
   ```
2. Set the active subscription:
   ```bash
   az account set --subscription "MVP Production"
   ```
3. For service principals, use:
   ```bash
   az login --service-principal \
     --username "$AZURE_CLIENT_ID" \
     --password "$AZURE_CLIENT_SECRET" \
     --tenant "$AZURE_TENANT_ID"
   ```

## Deployment Steps

### 1. Package Configuration Bundle

```bash
scripts/package_bundle.py --bundle cfg/bundles/prod.yaml --output build/bundle-azure.tar.gz
```

### 2. Provision Infrastructure (optional)

```bash
cd deploy/terraform/azure
terraform init
terraform apply -var-file=prod.tfvars
cd -
```

### 3. Deploy Adapters and Services

```bash
scripts/deploy.py \
  --bundle build/bundle-azure.tar.gz \
  --provider azure \
  --orchestrator data-factory \
  --storage https://mvpstorageprod.blob.core.windows.net/config \
  --secrets https://mvp-kv-prod.vault.azure.net/
```

### 4. Verify Deployment

```bash
az datafactory pipeline list --factory-name mvp-prod --resource-group mvp-rg
az monitor metrics list --resource /subscriptions/<SUB>/resourceGroups/mvp-rg/providers/Microsoft.Web/sites/mvp-orchestrator
```

## Example End-to-End Command

```bash
AZURE_SUBSCRIPTION_ID=$(az account show --query id -o tsv) \
scripts/run_pipeline.py \
  --bundle cfg/bundles/prod.yaml \
  --pipeline ingest-customers \
  --context azure
```
