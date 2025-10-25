# Ejecución de `prodi run-layer` en Azure Synapse

Esta guía cubre la ejecución del pipeline por capas en Synapse Pipelines con
notebooks o actividades de Spark que consumen el wheel de `prodi`.

## 1. Publicar el wheel en un workspace

1. Construye el wheel localmente:
   ```bash
   poetry build -f wheel
   ```
2. Carga el artefacto al almacenamiento ligado al workspace (por ejemplo ADLS):
   ```bash
   az storage blob upload \
     --account-name datalake \
     --container-name artifacts \
     --file dist/prodi-1.4.0-py3-none-any.whl \
     --name libs/prodi-1.4.0-py3-none-any.whl
   ```

## 2. Configurar un Spark job definition

Crea una [Spark job definition](https://learn.microsoft.com/azure/synapse-analytics/spark/spark-job-definitions)
que invoque el entrypoint de `prodi`.

```json
{
  "name": "prodi-layer-raw",
  "type": "SparkJobDefinition",
  "properties": {
    "file": "abfss://artifacts@datalake.dfs.core.windows.net/scripts/prodi_synapse_entry.py",
    "className": "",
    "args": ["--layer", "raw", "--config", "abfss://artifacts@datalake.dfs.core.windows.net/cfg/run/raw.yml"],
    "packages": ["abfss://artifacts@datalake.dfs.core.windows.net/libs/prodi-1.4.0-py3-none-any.whl"],
    "driverMemory": "8g",
    "executorMemory": "8g",
    "executorCores": 4
  }
}
```

Replica la definición para bronze, silver y gold cambiando `args` y el nombre.

## 3. Orquestar con Synapse Pipelines

Dentro de una pipeline usa actividades **Execute Spark job** con dependencias
secuenciales:

```yaml
activities:
  - name: raw
    type: SynapseNotebook
    dependsOn: []
    typeProperties:
      sparkJob: prodi-layer-raw
  - name: bronze
    dependsOn:
      - activity: raw
        dependencyConditions: [Succeeded]
    type: SynapseNotebook
    typeProperties:
      sparkJob: prodi-layer-bronze
  - name: silver
    dependsOn:
      - activity: bronze
        dependencyConditions: [Succeeded]
    type: SynapseNotebook
    typeProperties:
      sparkJob: prodi-layer-silver
  - name: gold
    dependsOn:
      - activity: silver
        dependencyConditions: [Succeeded]
    type: SynapseNotebook
    typeProperties:
      sparkJob: prodi-layer-gold
```

Revisa configuraciones listas para importar en [`docs/run/jobs/`](jobs/).

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

* Ejecuta el Spark job con `args`: `["--layer", "raw", "--config", "...", "--dry-run"]`
  para validar sin procesar datos.
* Crea un trigger separado (por ejemplo `ManualDryRun`) que fije el parámetro
  `dryRun=true` y lo concatene como argumento adicional.

## 6. Observabilidad

* Habilita [Apache Spark application monitoring](https://learn.microsoft.com/azure/synapse-analytics/spark/apache-spark-monitor-application)
  para seguir el progreso por capa.
* Enruta los logs de `stdout` a Log Analytics mediante la integración nativa de
  Synapse.

Estas prácticas permiten reiniciar capas específicas y reproducir la cadena
completa cuando se despliegan nuevas configuraciones.
