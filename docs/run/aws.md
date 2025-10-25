# Ejecución de `prodi run-layer` en AWS Glue

Esta guía describe cómo orquestar el pipeline de capas (raw → bronze → silver →
gold) en AWS Glue utilizando jobs basados en wheel y comandos `prodi run-layer`.

## 1. Empaquetar y publicar el wheel

1. Construye el wheel del proyecto:
   ```bash
   poetry build -f wheel
   ```
2. Carga el artefacto y las configuraciones productivas en un bucket S3 accesible
   desde Glue:
   ```bash
   aws s3 cp dist/mvp_config_driven-0.2.1-py3-none-any.whl s3://datalake-artifacts/prodi/
   aws s3 sync cfg s3://datalake-artifacts/cfg/
   ```

## 2. Configurar los Jobs de Glue y Step Functions

El archivo [`docs/run/jobs/aws_glue_v020.json`](jobs/aws_glue_v020.json) define un
Workflow de Step Functions que dispara cuatro jobs de Glue (uno por capa) con las
configuraciones `cfg/<layer>/aws.prod.yml`. Sube el JSON mediante la consola o la
CLI de Step Functions y crea los jobs Glue referenciados (`prodi-layer-raw`,
`prodi-layer-bronze`, etc.).

Para ejecutar manualmente cada capa o integrarla en pipelines existentes puedes
utilizar el script `scripts/aws_glue_submit.sh`:

```bash
bash scripts/aws_glue_submit.sh raw
bash scripts/aws_glue_submit.sh bronze
bash scripts/aws_glue_submit.sh silver
bash scripts/aws_glue_submit.sh gold
```

Los jobs y el script delegan en un entrypoint Python mínimo (`prodi_glue_entry.py`)
que invoca `prodi.cli.main(["run-layer", ...])` reutilizando el wheel publicado.

Para ejecuciones sobre EMR o EMR Serverless lanza un `spark-submit` por capa con
el mismo wheel y YAML de producción:

```bash
spark-submit \
  --deploy-mode cluster \
  --py-files s3://datalake-artifacts/prodi/mvp_config_driven-0.2.1-py3-none-any.whl \
  s3://datalake-artifacts/scripts/prodi_emr_entry.py \
  --layer bronze \
  --config s3://datalake-artifacts/cfg/bronze/aws.prod.yml
```

## 3. Producción (finanzas)

Despliega jobs dedicados para la cadena financiera utilizando los YAML
`cfg/finance/**/*.prod.yml`. A modo de referencia, el siguiente bloque arranca las
cuatro capas sobre Glue (sustituye `<bucket>` por tu ubicación real) y fuerza el
uso de la configuración HTTP; cambia `transactions_http` por
`transactions_jdbc` cuando el origen sea JDBC:

```bash
aws glue start-job-run --job-name prodi-fin-raw \
  --arguments '{"--layer":"raw","--config":"s3://<bucket>/cfg/finance/raw/transactions_http.aws.prod.yml"}'
aws glue start-job-run --job-name prodi-fin-bronze \
  --arguments '{"--layer":"bronze","--config":"s3://<bucket>/cfg/finance/bronze/transactions.aws.prod.yml"}'
aws glue start-job-run --job-name prodi-fin-silver \
  --arguments '{"--layer":"silver","--config":"s3://<bucket>/cfg/finance/silver/transactions.aws.prod.yml"}'
aws glue start-job-run --job-name prodi-fin-gold \
  --arguments '{"--layer":"gold","--config":"s3://<bucket>/cfg/finance/gold/kpis.aws.prod.yml"}'
```

Para EMR o EMR Serverless, arma un `spark-submit` por capa con el wheel 0.2.1 y
las rutas de configuración equivalentes:

```bash
spark-submit \
  --deploy-mode cluster \
  --py-files s3://datalake-artifacts/prodi/mvp_config_driven-0.2.1-py3-none-any.whl \
  s3://datalake-artifacts/scripts/prodi_emr_entry.py \
  --layer raw \
  --config s3://datalake-artifacts/cfg/finance/raw/transactions_http.aws.prod.yml
```

## 4. Validación `dry-run`

Antes de tocar datos reales ejecuta una validación `dry-run` forzada con la misma
configuración productiva. Tanto los jobs de Glue como Step Functions aceptan
variables de entorno a través de `--arguments`:

```bash
aws glue start-job-run --job-name prodi-layer-raw \
  --arguments '{"--env.PRODI_FORCE_DRY_RUN":"true"}'
```

El flag `PRODI_FORCE_DRY_RUN=1` replica el job `smoke-prod` de CI y obliga a que
`prodi` no ejecute acciones sobre los buckets aun cuando el YAML defina
`dry_run: false`.

## 5. Parámetros dinámicos y credenciales

* Define parámetros globales en Step Functions y pásalos a cada job como
  `--env=prod` o fechas de partición.
* Usa AWS Secrets Manager o Parameter Store para secretos operativos; las rutas
  `cfg/*/aws.prod.yml` se apoyan únicamente en IAM (sin llaves embebidas) como se
  describe en la sección de credenciales por identidad del README.

## 6. Observabilidad

* Activa CloudWatch Logs para cada job y configura métricas de error.
* Exporta el catálogo de datasets actualizado tras cada ejecución a S3 para
  trazabilidad.

Siguiendo estos pasos puedes ejecutar las cuatro capas de manera modular y
reprocesar sólo la capa afectada ante incidentes.
