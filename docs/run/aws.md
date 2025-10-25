# Ejecución de `prodi run-layer` en AWS Glue

Esta guía describe cómo orquestar el pipeline de capas (raw → bronze → silver →
gold) en AWS Glue utilizando jobs basados en wheel y comandos `prodi run-layer`.

## 1. Empaquetar y publicar el wheel

1. Construye el wheel del proyecto:
   ```bash
   poetry build -f wheel
   ```
2. Carga el artefacto a un bucket S3 accesible desde Glue junto con las
   configuraciones declarativas:
   ```bash
   aws s3 cp dist/mvp_config_driven-0.1.0-py3-none-any.whl s3://datalake-artifacts/prodi/
   aws s3 sync cfg s3://datalake-artifacts/cfg/
   ```

## 2. Configurar el Job de Glue

Crea un Job de tipo Python shell 3.9 o Spark (según la capa) y define como script
principal el entrypoint del wheel usando `--additional-python-modules`.

```bash
aws glue create-job \
  --name prodi-layer-raw \
  --role AWSGlueServiceRoleDefault \
  --command '{
    "Name": "glueetl",
    "PythonVersion": "3",
    "ScriptLocation": "s3://datalake-artifacts/scripts/prodi_glue_entry.py"
  }' \
  --default-arguments '{
    "--additional-python-modules": "s3://datalake-artifacts/prodi/mvp_config_driven-0.1.0-py3-none-any.whl",
    "--extra-py-files": "s3://datalake-artifacts/cfg.zip",
    "--layer": "raw",
    "--config": "s3://datalake-artifacts/cfg/raw/example.yml"
  }'
```

El script `prodi_glue_entry.py` expone la invocación al entrypoint:

```python
import prodi.cli

if __name__ == "__main__":
    prodi.cli.main(["run-layer"])
```

### Encadenar capas

Define un job por capa y utiliza Workflows de Glue o Step Functions para
ejecutarlos secuencialmente. El archivo [`docs/run/jobs/aws_stepfunctions.json`](jobs/aws_stepfunctions.json)
ya referencia las rutas `cfg/<layer>/example.yml` y puede importarse sin
modificaciones.

```json
{
  "Comment": "Cadena prodi",
  "StartAt": "raw",
  "States": {
    "raw": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {"JobName": "prodi-layer-raw"},
      "Next": "bronze"
    },
    "bronze": {"Type": "Task", "Resource": "arn:aws:states:::glue:startJobRun.sync", "Parameters": {"JobName": "prodi-layer-bronze"}, "Next": "silver"},
    "silver": {"Type": "Task", "Resource": "arn:aws:states:::glue:startJobRun.sync", "Parameters": {"JobName": "prodi-layer-silver"}, "Next": "gold"},
    "gold": {"Type": "Task", "Resource": "arn:aws:states:::glue:startJobRun.sync", "Parameters": {"JobName": "prodi-layer-gold"}, "End": true}
  }
}
```

Para ejecuciones sobre EMR o EMR Serverless, lanza un `spark-submit` por capa
con el mismo wheel:

```bash
spark-submit \
  --deploy-mode cluster \
  --py-files s3://datalake-artifacts/prodi/mvp_config_driven-0.1.0-py3-none-any.whl \
  s3://datalake-artifacts/scripts/prodi_emr_entry.py \
  --layer bronze \
  --config s3://datalake-artifacts/cfg/bronze/example.yml
```

El script `prodi_emr_entry.py` debe delegar en `prodi.cli.main(["run-layer", ...])`,
idéntico al usado en Glue y Dataproc.

## 3. Validación `dry-run`

Ejecuta pruebas en modo validación agregando el argumento `--dry-run`:

```bash
aws glue start-job-run --job-name prodi-layer-raw --arguments '{"--dry-run":"true"}'
```

En entornos de QA puedes forzar el flag desde la definición del job en
`--default-arguments` y sobreescribirlo en producción.

## 4. Parámetros dinámicos

* Define parámetros globales en el Workflow y pásalos a cada job como
  `--env=prod` o `--date=2024-03-31`.
* Usa AWS Secrets Manager para credenciales y recupéralos en tiempo de ejecución
  con `boto3` antes de invocar `prodi run-layer`.

## 5. Observabilidad

* Activa CloudWatch Logs para cada job y configura métricas de error.
* Exporta el catálogo de datasets actualizado tras cada ejecución a S3 para
  trazabilidad.

Siguiendo estos pasos puedes ejecutar las cuatro capas de manera modular y
reprocesar sólo la capa afectada ante incidentes.
