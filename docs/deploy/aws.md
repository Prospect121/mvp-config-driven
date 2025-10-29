# AWS Deployment Guide

This guide describes how to install prerequisites, authenticate, and deploy the MVP configuration-driven platform on AWS.

## Prerequisites

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
brew install awscli || sudo apt-get install -y awscli
```

Ensure Terraform is installed if infrastructure provisioning is required:

```bash
brew install terraform || sudo apt-get install -y terraform
```

## Authentication

1. Configure AWS credentials for the deployment account:
   ```bash
   aws configure --profile mvp-deployer
   ```
   Provide the Access Key ID, Secret Access Key, default region, and output format.
2. Export the profile for CLI tools:
   ```bash
   export AWS_PROFILE=mvp-deployer
   export AWS_DEFAULT_REGION=us-east-1
   ```
3. If using SSO, log in with:
   ```bash
   aws sso login --profile mvp-deployer
   ```

## Deployment Steps

### 1. Package Configuration Bundle

```bash
scripts/package_bundle.py --bundle cfg/bundles/prod.yaml --output build/bundle-aws.tar.gz
```

### 2. Provision Infrastructure (optional)

```bash
cd deploy/terraform/aws
terraform init
terraform apply -var-file=prod.tfvars
cd -
```

### 3. Deploy Adapters and Services

```bash
scripts/deploy.py \
  --bundle build/bundle-aws.tar.gz \
  --provider aws \
  --orchestrator ecs \
  --storage s3://mvp-artifacts-prod \
  --secrets arn:aws:secretsmanager:us-east-1:123456789012:secret:mvp/config
```

### 4. Verify Deployment

```bash
aws ecs list-services --cluster mvp-prod
aws logs tail /aws/ecs/mvp-prod --follow
```

## Example End-to-End Command

```bash
AWS_PROFILE=mvp-deployer \
scripts/run_pipeline.py \
  --bundle cfg/bundles/prod.yaml \
  --pipeline ingest-customers \
  --context aws
```
