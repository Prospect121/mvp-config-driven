# Ejecución de `prodi run-layer` en Databricks

Esta guía orienta a los equipos de operaciones para ejecutar pipelines de `prodi`
utilizando Jobs de Databricks respaldados por wheel tasks. El flujo cubre la
preparación del artefacto, la configuración del job y comandos de verificación
(`dry-run`).

## 1. Empaquetar el proyecto como wheel

1. Construye el paquete desde la raíz del repositorio:
   ```bash
   poetry build -f wheel
   ```
2. Publica el wheel (`dist/prodi-<version>-py3-none-any.whl`) en DBFS o en un
   repositorio de artefactos accesible desde Databricks:
   ```bash
   databricks fs cp dist/prodi-1.4.0-py3-none-any.whl dbfs:/libs/prodi-1.4.0.whl
   ```

## 2. Definir la tarea tipo wheel

Dentro de un Job multipaso se recomienda una tarea por capa. Cada tarea reutiliza
el mismo wheel y varía los parámetros del comando `prodi run-layer`.

```json
{
  "name": "prodi-layer-raw",
  "task_key": "raw",
  "job_cluster_key": "shared_cluster",
  "wheel_task": {
    "package_name": "prodi",
    "entry_point": "run-layer",
    "parameters": [
      "--layer", "raw",
      "--config", "dbfs:/cfg/run/raw.yml"
    ]
  },
  "libraries": [
    { "whl": "dbfs:/libs/prodi-1.4.0.whl" }
  ]
}
```

### Secuencia completa raw → bronze → silver → gold

Replica la definición anterior ajustando `task_key`, `--layer` y la ruta del
YAML para cada capa. Encadena las tareas estableciendo dependencias:

```json
{
  "tasks": [
    { "task_key": "raw", ... },
    { "task_key": "bronze", "depends_on": [{"task_key": "raw"}], ... },
    { "task_key": "silver", "depends_on": [{"task_key": "bronze"}], ... },
    { "task_key": "gold", "depends_on": [{"task_key": "silver"}], ... }
  ]
}
```

Consulta ejemplos más completos en [`docs/run/jobs/`](jobs/).

## 3. Ejecutar en modo `dry-run`

Antes de calendarizar el job, valida el pipeline ejecutando una corrida de
prueba que sólo evalúa la configuración:

```bash
prodi run-layer --layer raw --config cfg/run/raw.yml --dry-run
prodi run-layer --layer bronze --config cfg/run/bronze.yml --dry-run
```

Para tareas en Databricks, agrega `"parameters": ["--dry-run", ...]` a la tarea
`wheel_task`. El job se detendrá al primer error de validación sin tocar datos.

## 4. Variables y secretos operativos

* Usa `job_parameters` o [task parameters](https://docs.databricks.com/jobs/jobs-parameterization.html)
  para inyectar rutas de configuración o flags como `--env prod`.
* Gestiona credenciales mediante [Databricks Secrets](https://docs.databricks.com/security/secrets/index.html)
  y refiérelas dentro de los YAML con `{{secrets/<scope>/<key>}}`.
* Registra el build del wheel junto con la versión de configuración aplicada
  para reproducibilidad.

## 5. Monitoreo y recuperación

* Activa [Job run notifications](https://docs.databricks.com/workflows/jobs/jobs-notifications.html)
  para avisos de fallos entre capas.
* Consolida los logs de `prodi` (stdout) en Lakehouse escribiendo desde Databricks
  a un `delta` controlado, o envíalos a observabilidad centralizada (Datadog,
  Splunk).

Con esta estructura cada capa se mantiene desacoplada, lo que facilita repetir
un eslabón específico (por ejemplo `silver`) ante un fallo aguas abajo.
