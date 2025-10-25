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
2. Publica el wheel (`dist/mvp_config_driven-0.2.0-py3-none-any.whl`) y las
   configuraciones de producción (`cfg/*/*.prod.yml`)
   en DBFS o en un repositorio de artefactos accesible desde Databricks:
   ```bash
   databricks fs cp dist/mvp_config_driven-0.2.0-py3-none-any.whl dbfs:/libs/mvp-config-driven.whl
   databricks fs cp -r cfg dbfs:/cfg
   ```

## 2. Definir la tarea tipo wheel

Dentro de un Job multipaso se recomienda una tarea por capa. Cada tarea reutiliza
el mismo wheel y varía los parámetros del comando `prodi run-layer`. El archivo
[`docs/run/jobs/databricks_v020.json`](jobs/databricks_v020.json) declara la
cadena completa para AWS (ajusta el sufijo `aws` por `azure` o `gcp` según tu
workspace):

```json
{
  "tasks": [
    {
      "task_key": "raw",
      "job_cluster_key": "shared_cluster",
      "depends_on": [],
      "wheel_task": {
        "package_name": "prodi",
        "entry_point": "run-layer",
        "parameters": ["--layer", "raw", "--config", "dbfs:/cfg/raw/aws.prod.yml"]
      },
      "libraries": [{"whl": "dbfs:/libs/mvp-config-driven.whl"}]
    },
    {
      "task_key": "bronze",
      "depends_on": [{"task_key": "raw"}],
      "job_cluster_key": "shared_cluster",
      "wheel_task": {
        "package_name": "prodi",
        "entry_point": "run-layer",
        "parameters": ["--layer", "bronze", "--config", "dbfs:/cfg/bronze/aws.prod.yml"]
      },
      "libraries": [{"whl": "dbfs:/libs/mvp-config-driven.whl"}]
    }
  ]
}
```

El JSON de referencia incluye también `silver` y `gold` con dependencias en
cadena. Copia el archivo, modifica las rutas `dbfs:/cfg/<layer>/<cloud>.prod.yml`
al proveedor correcto y sube la definición mediante la API de Databricks o la UI.

## 3. Ejecutar en modo `dry-run`

Antes de calendarizar el job, valida el pipeline ejecutando una corrida de
prueba que sólo evalúa la configuración:

```bash
prodi run-layer raw -c cfg/raw/example.yml
prodi run-layer bronze -c cfg/bronze/example.yml
```

Para tareas en Databricks define `PRODI_FORCE_DRY_RUN=1` como variable de entorno
del job o del task. `prodi` forzará el `dry_run` incluso si el YAML productivo lo
declara en `false`, lo que permite validar `cfg/*/*.prod.yml` sin tocar datos.
La configuración de CI (`smoke-prod`) usa exactamente este patrón.

## 4. Variables y secretos operativos

* Usa `job_parameters` o [task parameters](https://docs.databricks.com/jobs/jobs-parameterization.html)
  para inyectar rutas de configuración o flags como `--env prod`.
* Gestiona credenciales mediante [Databricks Secrets](https://docs.databricks.com/security/secrets/index.html)
  y refiérelas dentro de los YAML con `{{secrets/<scope>/<key>}}`. Las rutas
  productivas incluidas en este repositorio emplean identidades administradas
  (IAM/Managed Identity/Service Account) descritas en el README.
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
