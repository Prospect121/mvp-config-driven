# GCP Deployment Guide

Use this guide to deploy the MVP configuration-driven platform on Google Cloud Platform.

## Prerequisites

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
brew install --cask google-cloud-sdk || sudo apt-get install -y google-cloud-sdk
```

Install optional tooling:

```bash
brew install terraform || sudo apt-get install -y terraform
```

## Authentication

1. Initialize the gcloud CLI:
   ```bash
   gcloud init
   ```
2. Set the project:
   ```bash
   gcloud config set project mvp-prod
   ```
3. For service accounts, activate credentials:
   ```bash
   gcloud auth activate-service-account mvp-deployer@mvp-prod.iam.gserviceaccount.com \
     --key-file ~/.config/gcloud/mvp-deployer.json
   ```
4. Configure Application Default Credentials for libraries:
   ```bash
   gcloud auth application-default login
   ```

## Deployment Steps

### 1. Package Configuration Bundle

```bash
scripts/package_bundle.py --bundle cfg/bundles/prod.yaml --output build/bundle-gcp.tar.gz
```

### 2. Provision Infrastructure (optional)

```bash
cd deploy/terraform/gcp
terraform init
terraform apply -var-file=prod.tfvars
cd -
```

### 3. Deploy Adapters and Services

```bash
scripts/deploy.py \
  --bundle build/bundle-gcp.tar.gz \
  --provider gcp \
  --orchestrator composer \
  --storage gs://mvp-artifacts-prod \
  --secrets projects/mvp-prod/secrets/mvp-config
```

### 4. Verify Deployment

```bash
gcloud composer environments list --locations us-central1
gcloud logging read "resource.type=cloud_composer_environment" --limit 50
```

## Example End-to-End Command

```bash
GOOGLE_APPLICATION_CREDENTIALS=~/.config/gcloud/mvp-deployer.json \
scripts/run_pipeline.py \
  --bundle cfg/bundles/prod.yaml \
  --pipeline ingest-customers \
  --context gcp
```
