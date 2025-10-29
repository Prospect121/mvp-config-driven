# Despliegue en Azure Databricks

## Prerrequisitos
- Workspace Databricks con runtime 13.3 LTS (Spark 3.4+) o superior.
- Acceso a Azure Data Lake Storage Gen2 (ABFS) y Azure Key Vault.
- Librerías: `delta`, `azure-identity`, `hadoop-azure`.

## Empaquetado
```bash
pip install build
python -m build
# Genera dist/datacore-<version>-py3-none-any.whl
```

## Carga de librería
1. Sube el wheel a DBFS (`dbfs cp dist/datacore-*.whl dbfs:/libs/`).
2. Adjunta la librería al Job o Cluster.

## Configuración del Job
- Tipo: Job sin notebooks (Python wheel task).
- Parámetros de entorno: `PROJECT_CONFIG=/Workspace/mvp/configs/envs/prod/project.yml`.
- Comando: `prodi run --layer raw --config /Workspace/mvp/configs/envs/prod/layers/raw.yml --platform azure --env prod`.

## Checkpoints
Configura en `configs/platforms/azure.yml` los paths `abfss://<container>@<storage>.dfs.core.windows.net/checkpoints`.
