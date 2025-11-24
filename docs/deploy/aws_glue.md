# Despliegue en AWS Glue

## Prerrequisitos
- AWS Glue 4.0 (Spark 3.3+).
- Bucket S3 para datos, checkpoints y librerías.
- IAM Role con permisos en S3, CloudWatch Logs y Secrets Manager (opcional).

## Empaquetado y subida
```bash
pip install build
python -m build
aws s3 cp dist/datacore-<version>-py3-none-any.whl s3://<bucket>/libs/
```

## Creación del Job
- Tipo: Spark.
- Script: `s3://<bucket>/jobs/runner.py` (ver ejemplo en `/examples/aws`).
- Additional Python modules: `s3://<bucket>/libs/datacore-<version>-py3-none-any.whl`.
- Parámetros: `--layer raw --config s3://<bucket>/configs/envs/prod/layers/raw.yml --platform aws --env prod`.

## Networking y seguridad
Asegura endpoints privados o VPC endpoints a S3 cuando sea necesario. Usa Secrets Manager para credenciales JDBC.
