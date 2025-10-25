# Ejecución de `prodi run-layer` en Google Cloud Dataproc

Esta guía explica cómo desplegar la cadena raw → bronze → silver → gold en
Dataproc utilizando jobs de tipo PySpark que consumen el wheel de `prodi`.

## 1. Preparar el wheel y subirlo a GCS

1. Construye el paquete:
   ```bash
   poetry build -f wheel
   ```
2. Publica el wheel y las configuraciones productivas en un bucket regional:
   ```bash
   gsutil cp dist/mvp_config_driven-0.2.0-py3-none-any.whl gs://datalake-artifacts/libs/
   gsutil cp -r cfg gs://datalake-artifacts/cfg
   ```

## 2. Definir el workflow template

El archivo [`docs/run/jobs/dataproc_v020.yaml`](jobs/dataproc_v020.yaml) declara
cuatro pasos encadenados (`raw` → `bronze` → `silver` → `gold`) que ejecutan
`gcloud dataproc jobs submit pyspark` con los YAML `cfg/<layer>/gcp.prod.yml`.
Importa el template, ajusta la región/zona y actualiza los URIs si tu bucket
cambia.

Cada paso invoca un entrypoint ligero (`prodi_dataproc_entry.py`) que reenvía los
argumentos a `prodi.cli.main`. Coloca este script en el bucket junto al wheel:

```python
import sys
from prodi.cli import main

if __name__ == "__main__":
    main(["run-layer", *sys.argv[1:]])
```

## 3. Validación `dry-run`

Antes de calendarizar el workflow, ejecuta un job aislado habilitando el override
por identidad administrada:

```bash
gcloud dataproc jobs submit pyspark gs://datalake-artifacts/scripts/prodi_dataproc_entry.py \
  --cluster=dp-ops-validation \
  --region=us-central1 \
  --jars=gs://datalake-artifacts/libs/mvp_config_driven-0.2.0-py3-none-any.whl \
  -- \
  --layer raw \
  --config gs://datalake-artifacts/cfg/raw/gcp.prod.yml \
  --env.PRODI_FORCE_DRY_RUN=true
```

El parámetro `--env.PRODI_FORCE_DRY_RUN=true` aplica el mismo mecanismo que la
job `smoke-prod` en CI y evita side-effects aun cuando `dry_run: false` está
configurado en los YAML productivos.

## 4. Buenas prácticas operativas

* Activa [diagnostic logs](https://cloud.google.com/dataproc/docs/guides/logging) y
  envía los logs de `stdout` a Cloud Logging con filtros por `step-id`.
* Parametriza fechas y rutas mediante variables del template y mantén las
  identidades de servicio con alcance mínimo; los YAML se apoyan en cuentas de
  servicio administradas sin llaves embebidas.
* Desactiva el clúster tras la ejecución utilizando Workflow Templates sin
  clúster preexistente o [Dataproc Serverless](https://cloud.google.com/dataproc-serverless/docs).

Consulta plantillas adicionales en [`docs/run/jobs/`](jobs/) para integraciones
con Cloud Composer u orquestadores externos.
