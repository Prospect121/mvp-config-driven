# Configs Directory Structure

This directory contains configuration files organized by environment.

## Structure

```
configs/
├── envs/                    # Configurations by environment
│   ├── dev/
│   │   ├── project.yml
│   │   └── layers/
│   ├── test/
│   │   ├── project.yml
│   │   └── layers/
│   ├── prod/
│   │   ├── project.yml
│   │   └── layers/
│   └── databricks/
│       └── project.yml
└── platforms/               # Platform-specific configs
    ├── aws.yml
    ├── azure.yml
    └── gcp.yml
```

## Environments

### Development (`envs/dev/`)
- Platform: `local`
- Shuffle partitions: 1
- Uses local file system paths

### Test (`envs/test/`)
- Platform: `azure`
- Shuffle partitions: 4  
- Uses Azure ABFS storage

### Production (`envs/prod/`)
- Platform: `gcp`
- Shuffle partitions: 8
- Uses GCS storage and BigQuery

### Databricks (`envs/databricks/`)
- Platform: `azure`
- Optimized for Azure Databricks runtime

## Platform Configs

Platform-specific configurations for:
- **AWS**: S3 backends, Glue, Redshift
- **Azure**: ABFS, Databricks, Synapse  
- **GCP**: GCS, Dataproc, BigQuery

## Usage

```bash
# Development
prodi run --layer bronze --config configs/envs/dev/project.yml

# Test
prodi run --layer bronze --config configs/envs/test/project.yml --platform azure

# Production
prodi run --layer gold --config configs/envs/prod/project.yml --platform gcp
```

## Validation

All configurations validate against `datacore/config/schemas/project.schema.json`:

```bash
prodi validate --config configs/envs/dev/project.yml
```
