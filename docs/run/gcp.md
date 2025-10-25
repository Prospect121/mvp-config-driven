# Ejecución de `prodi run-layer` en Google Cloud Dataproc

Esta guía explica cómo desplegar la cadena raw → bronze → silver → gold en
Dataproc utilizando jobs de tipo PySpark que consumen el wheel de `prodi`.

## 1. Preparar el wheel y subirlo a GCS

1. Construye el paquete:
   ```bash
   poetry build -f wheel
   ```
2. Publica el wheel y configuraciones en un bucket regional:
   ```bash
   gsutil cp dist/mvp_config_driven-0.1.0-py3-none-any.whl gs://datalake-artifacts/libs/
   gsutil cp -r cfg gs://datalake-artifacts/cfg
   ```

## 2. Definir un workflow template

Crea un [Dataproc Workflow Template](https://cloud.google.com/dataproc/docs/concepts/workflows/overview)
con un paso por capa. Cada paso invoca `prodi run-layer` mediante
`gcloud dataproc jobs submit pyspark` usando el wheel como archivo extra.

```yaml
jobs:
  - step-id: raw
    pyspark-job:
      main-python-file-uri: gs://datalake-artifacts/scripts/prodi_dataproc_entry.py
      python-file-uris:
        - gs://datalake-artifacts/libs/prodi-1.4.0-py3-none-any.whl
      args: ["--layer", "raw", "--config", "gs://datalake-artifacts/cfg/raw/example.yml"]
  - step-id: bronze
    prerequisite-step-ids: [raw]
    pyspark-job:
      main-python-file-uri: gs://datalake-artifacts/scripts/prodi_dataproc_entry.py
      python-file-uris:
        - gs://datalake-artifacts/libs/prodi-1.4.0-py3-none-any.whl
      args: ["--layer", "bronze", "--config", "gs://datalake-artifacts/cfg/bronze/example.yml"]
  - step-id: silver
    prerequisite-step-ids: [bronze]
    pyspark-job:
      main-python-file-uri: gs://datalake-artifacts/scripts/prodi_dataproc_entry.py
      python-file-uris:
        - gs://datalake-artifacts/libs/prodi-1.4.0-py3-none-any.whl
      args: ["--layer", "silver", "--config", "gs://datalake-artifacts/cfg/silver/example.yml"]
  - step-id: gold
    prerequisite-step-ids: [silver]
    pyspark-job:
      main-python-file-uri: gs://datalake-artifacts/scripts/prodi_dataproc_entry.py
      python-file-uris:
        - gs://datalake-artifacts/libs/prodi-1.4.0-py3-none-any.whl
      args: ["--layer", "gold", "--config", "gs://datalake-artifacts/cfg/gold/example.yml"]
cluster-selector:
  zone: us-central1-a
  cluster-labels:
    env: prod

## 3. Script de entrada

El archivo `prodi_dataproc_entry.py` debe residir en el mismo bucket y delegar en
el CLI de `prodi`:

```python
import sys
from prodi.cli import main

if __name__ == "__main__":
    main(["run-layer", *sys.argv[1:]])
```

## 4. Validación `dry-run`

Antes de ejecutar el workflow completo, lanza un job aislado con el flag
`--dry-run` para validar la configuración de la capa:

```bash
gcloud dataproc jobs submit pyspark gs://datalake-artifacts/scripts/prodi_dataproc_entry.py \
  --cluster=dp-ops-validation \
  --region=us-central1 \
  --jars=gs://datalake-artifacts/libs/mvp_config_driven-0.1.0-py3-none-any.whl \
  -- \
  --layer raw \
  --config gs://datalake-artifacts/cfg/raw/example.yml \
  --dry-run
```

Puedes repetir el comando para cada capa, o bien crear una versión alternativa
del template que fije `args: [..., "--dry-run"]` para entornos de QA.

Cuando necesites ejecutar la cadena completa desde un orquestador externo,
crea una tarea por capa reutilizando el mismo wheel. Encadena `run-layer` para
raw → bronze → silver → gold y mantén `--dry-run` hasta validar la integración.

## 5. Buenas prácticas operativas

* Activa [diagnostic logs](https://cloud.google.com/dataproc/docs/guides/logging) y
  envía los logs de `stdout` a Cloud Logging con filtros por `task-id`.
* Usa variables de plantilla (`{{execution_date}}`) al parametrizar rutas de
  entrada/salida en los YAML.
* Desactiva el cluster tras la ejecución utilizando Workflow Templates sin
  clúster preexistente o [Dataproc Serverless](https://cloud.google.com/dataproc-serverless/docs).

Consulta plantillas adicionales en [`docs/run/jobs/`](jobs/) para integrarlas con
Cloud Composer u orquestadores externos.
