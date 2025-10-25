# Ejecución de `prodi run-layer` en Azure Synapse

Esta guía cubre la ejecución del pipeline por capas en Synapse Pipelines o Azure
Data Factory utilizando actividades Spark que consumen el wheel de `prodi`.

## 1. Publicar el wheel y configuraciones

1. Construye el wheel localmente:
   ```bash
   poetry build -f wheel
   ```
2. Carga el artefacto y las configuraciones productivas al almacenamiento ligado
   al workspace (por ejemplo ADLS):
   ```bash
   az storage blob upload \
     --account-name datalake \
     --container-name artifacts \
     --file dist/mvp_config_driven-0.2.0-py3-none-any.whl \
     --name libs/mvp_config_driven-0.2.0-py3-none-any.whl
   az storage blob upload-batch \
     --account-name datalake \
     --destination cfg \
     --source cfg
   ```

## 2. Definir actividades Spark por capa

Crea una [Spark job definition](https://learn.microsoft.com/azure/synapse-analytics/spark/spark-job-definitions)
por capa. El JSON [`docs/run/jobs/azure_adf_pipeline.json`](jobs/azure_adf_pipeline.json)
se actualizó para apuntar a `cfg/<layer>/azure.prod.yml` y reutilizar el wheel
`mvp_config_driven-0.2.0`. Importa la plantilla, actualiza los vínculos de
servicio y publica la pipeline.

Ejemplo de definición para `raw`:

```json
{
  "name": "prodi-layer-raw",
  "type": "SparkJobDefinition",
  "properties": {
    "file": "abfss://artifacts@datalake.dfs.core.windows.net/scripts/prodi_synapse_entry.py",
    "args": ["--layer", "raw", "--config", "abfss://artifacts@datalake.dfs.core.windows.net/cfg/raw/azure.prod.yml"],
    "packages": ["abfss://artifacts@datalake.dfs.core.windows.net/libs/mvp_config_driven-0.2.0-py3-none-any.whl"],
    "driverMemory": "8g",
    "executorMemory": "8g",
    "executorCores": 4
  }
}
```

Replica la definición para bronze, silver y gold cambiando `args` y el nombre.

## 3. Orquestar con Synapse Pipelines o ADF

El pipeline de ejemplo en [`docs/run/jobs/azure_adf_pipeline.json`](jobs/azure_adf_pipeline.json)
contiene cuatro actividades `ExecuteSparkJob` con dependencias secuenciales y
parametriza `configUri` usando los YAML productivos. Ajusta las identidades
administradas según tu subscription; los archivos `.prod.yml` están diseñados
para autenticarse mediante Managed Identity sin llaves embebidas.

## 4. Script de entrada y parámetros

El script `prodi_synapse_entry.py` debe reenviar los argumentos recibidos:

```python
import sys
from prodi.cli import main

if __name__ == "__main__":
    main(["run-layer", *sys.argv[1:]])
```

Pasa parámetros a nivel de pipeline (por ejemplo `executionDate`) y sustitúyelos
en los argumentos con `@{pipeline().parameters.executionDate}`.

## 5. Validación `dry-run`

Agrega `--env.PRODI_FORCE_DRY_RUN=true` como argumento adicional en Synapse o ADF
para validar la pipeline completa sin tocar datos reales. El comportamiento es
idéntico al job `smoke-prod` configurado en CI.

## 6. Observabilidad

* Habilita [Apache Spark application monitoring](https://learn.microsoft.com/azure/synapse-analytics/spark/apache-spark-monitor-application)
  para seguir el progreso por capa.
* Enruta los logs de `stdout` a Log Analytics mediante la integración nativa de
  Synapse.

Estas prácticas permiten reiniciar capas específicas y reproducir la cadena
completa cuando se despliegan nuevas configuraciones.
