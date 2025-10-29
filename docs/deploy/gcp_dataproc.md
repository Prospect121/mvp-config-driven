# Despliegue en GCP Dataproc Serverless

## Prerrequisitos
- Proyecto GCP con Dataproc Serverless habilitado.
- Bucket GCS para datos y librerías.
- Servicio gestionado con permisos en GCS, Logging y Secret Manager.

## Empaquetado
```bash
pip install build
python -m build
gsutil cp dist/datacore-<version>-py3-none-any.whl gs://<bucket>/libs/
```

## Ejecución
```bash
gcloud dataproc batches submit pyspark gs://<bucket>/jobs/runner.py \
  --project=<project> \
  --region=<region> \
  --deps-bucket=gs://<bucket> \
  --py-files=gs://<bucket>/libs/datacore-<version>-py3-none-any.whl \
  -- \
  --layer raw \
  --config gs://<bucket>/configs/envs/prod/layers/raw.yml \
  --platform gcp \
  --env prod
```

## Consideraciones
- Configura `spark.jars.packages` con `com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:<version>` para sinks BigQuery.
- Usa Secret Manager para credenciales externas.
