Implementación de Transformaciones de Datos (Config-Driven + Pandas UDF)

Resumen
- Objetivo: definir una arquitectura y guías prácticas para ingestar, transformar y publicar datos mediante Spark, con configuración dinámica y Pandas UDFs para rendimiento.
- Alcance: fuentes externas (APIs/BDs), flujo de capas (Raw→Silver→Gold), manejo de errores, seguridad y optimización.

Arquitectura Propuesta
- Componentes:
  - `Spark Master/Workers`: ejecución distribuida; imagen basada en Bitnami Spark 3.5.1 con `numpy==1.26.4`, `pandas==2.2.2`, `pyarrow==14.0.2` instalados para Pandas UDFs.
  - `Runner`: orquesta `spark-submit` y carga configuración (`dataset.yml`, `env.yml`, `database.yml`).
  - `MinIO (S3A)`: almacenamiento de datos Raw/Silver/Gold usando `s3a://` con `hadoop-aws` y `aws-java-sdk-bundle`.
  - `PostgreSQL`: metadatos y auditoría de ejecuciones (inicio/fin, versiones de datasets).
  - `Config YAML`: define fuentes, transformaciones (expresiones/UDFs), calidad, particionamiento y destinos.
- Despliegue:
  - `docker-compose.yml`: servicios `spark-master`, `spark-worker`, `runner`, `minio`, `postgres`.
  - Imagen personalizada `mvp-spark:3.5.1-pandas` (Dockerfile en `docker/spark-pandas-udf/Dockerfile`).
  - `scripts/runner.sh`: envía el job con Arrow activado (`spark.sql.execution.arrow.pyspark.enabled=true`).

Flujo de Datos
- Ingesta (Raw):
  - APIs REST: obtención paginada y almacenamiento en `s3a://bucket/raw/...` (JSON/Parquet) o conversión directa a DataFrame.
  - Bases de datos externas: lectura vía JDBC con `spark.read.format("jdbc")` a Raw/Silver según volumen.
  - Archivos: lectura directa desde rutas S3A/FS locales (CSV/JSON/Parquet).
- Transformación (Silver):
  - Limpieza, tipado y normalización mediante expresiones (`expr`) y UDFs del catálogo (Pandas UDFs).
  - Reglas de calidad y cuarentena opcional.
- Publicación (Gold):
  - Escritura particionada a S3A (Parquet/Delta opcional) y carga de metadatos en Postgres.

Mecanismos de Configuración
- Archivos:
  - `config/env.yml`: parámetros globales (timezone, endpoints, credenciales vía entorno, etc.).
  - `config/database.yml`: conexiones y tablas de metadatos.
  - `config/datasets/<dominio>/<dataset>/dataset.yml`: fuente, transformaciones, calidad, destino y particionamiento.
- Ejemplo `dataset.yml` (API REST):
```
id: payments_v1
source:
  type: api
  input_format: json
  endpoint: https://api.example.com/payments
  options:
    method: GET
    headers_ref: api_headers_payments   # clave en env.yml
    page_param: page
    page_size: 1000
    max_pages: 100
transformations:
  expr:
    - {target: amount_usd, expr: "amount * fx_rate", type: double, on_error: null}
  udf:
    on_error: skip
    - {target_column: normalized_id, function: normalize_id, args: [id]}
    - {target_column: full_name_std, function: standardize_name, args: [first_name, last_name]}
quality:
  expectations_ref: configs/quality/payments_expectations.yml
output:
  silver:
    path: s3a://data-lake/silver/payments_v1
    format: parquet
  gold:
    path: s3a://data-lake/gold/payments_v1
    format: parquet
partitioning:
  by: [year, month, day]
```
- Ejemplo `dataset.yml` (JDBC):
```
source:
  type: jdbc
  input_format: parquet
  jdbc:
    url: jdbc:postgresql://dbhost:5432/maindb
    table: public.payments
    user_ref: db_user
    password_ref: db_password
    fetchsize: 10000
```
- Patrón de lectura en el job:
  - `type: csv/json/parquet`: `spark.read.options(...).<fmt>(path)`.
  - `type: jdbc`: `spark.read.format("jdbc").option("url", url).option("dbtable", table)....load()`.
  - `type: api`: extracción con `requests` (paginado) y `spark.createDataFrame` o staging a S3A.

Estrategias de Manejo de Errores
- Transformaciones:
  - `on_error: null|skip` por columna/udf para evitar abortos masivos.
  - Tipado seguro (casts explícitos) y saneamiento de strings (`sanitize_string`).
- Calidad:
  - Expectativas configurables; envío a cuarentena (`quarantine`) con estadística agregada.
- Ingesta APIs:
  - Retries exponenciales, límites de tasa (backoff), validación de esquema, idempotencia por `run_id`.
- JDBC:
  - `fetchsize`, pushdown de filtros (`where` inicial), particionamiento por columna de rango.
- Auditoría:
  - Registro de inicio/fin y versiones en Postgres (módulo `database/db_manager.py`).

Consideraciones de Seguridad
- Credenciales:
  - Variables de entorno `.env`/Compose (no hardcode). Usar referencias en `env.yml` (`*_ref`).
  - En producción, secretos Docker/Kubernetes y rotación de credenciales.
- Transporte y almacenamiento:
  - HTTPS para APIs, TLS para JDBC; habilitar TLS en MinIO; cifrado en reposo (server-side).
- Acceso mínimo:
  - Políticas IAM/ACL restrictivas en S3A/MinIO; redes privadas y firewalls.
- Sanitización y cumplimiento:
  - Remoción/anonimización de PII en Silver/Gold; registros de acceso; cumplimiento de normativa.
- Supply chain:
  - Versiones fijadas (`numpy/pandas/pyarrow`) y escaneo de vulnerabilidades de imágenes.

Ejemplos de Implementación
- Lectura desde API (staging a S3A):
```
import requests, json, time
from pathlib import Path

def fetch_to_s3(endpoint, headers, page_param, page_size, max_pages, s3_path, fs):
    page = 1
    while page <= max_pages:
        r = requests.get(endpoint, headers=headers, params={page_param: page, 'page_size': page_size}, timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"API error {r.status_code}: {r.text}")
        data = r.json()
        # escribir chunk como jsonl
        local = f"/tmp/api_chunk_{page}.jsonl"
        with open(local, 'w', encoding='utf-8') as f:
            for row in data.get('items', []):
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        # subir a S3A
        fs.copyFromLocalFile(local, f"{s3_path}/page={page}/chunk.jsonl")
        page += 1
        time.sleep(0.2)
```
- Lectura JDBC (Spark):
```
reader = (spark.read.format('jdbc')
  .option('url', url)
  .option('dbtable', table)
  .option('user', user)
  .option('password', password)
  .option('fetchsize', 10000))
df = reader.load()
```
- Uso de Pandas UDFs (catálogo):
```
from udf_catalog import normalize_id, standardize_name
df = (df
  .withColumn('normalized_id', normalize_id(F.col('id')))
  .withColumn('full_name_std', standardize_name(F.col('first_name'), F.col('last_name'))))
```
- Aplicación basada en configuración (`spark_job_with_db.py`):
```
# transforms_cfg['udf'] con items: {target_column, function, args, type, on_error}
for t in udf_list:
  udf_func = get_udf(t['function'])
  cols = [F.col(a) if a in df.columns else F.lit(a) for a in t.get('args', [])]
  df = df.withColumn(t['target_column'], udf_func(*cols))
```

Recomendaciones de Optimización
- Pandas UDFs + Arrow:
  - Mantener Arrow activado y versiones compatibles; usar `pandas` para vectorización masiva.
- Particionamiento y paralelismo:
  - `repartition` por columnas de alto cardinalidad; ajustar `spark.sql.shuffle.partitions` según tamaño.
- Pushdown y filtros tempranos:
  - Aplicar filtros en lectura (CSV/JSON: `options`, JDBC: `predicate`/`dbtable` subquery).
- Joins y broadcast:
  - Activar broadcast para dimensiones pequeñas (`spark.sql.autoBroadcastJoinThreshold`).
- Persistencia y formato:
  - Priorizar Parquet/Delta; compresión adecuada (snappy/zstd); particionar por fecha.
- Gestión de memoria:
  - Ajustar `SPARK_WORKER_MEMORY`, `SPARK_WORKER_CORES`; monitorizar en UI 4040.
- Calidad y confiabilidad:
  - Checkpoints en flujos incrementales; ids de ejecución (`run_id`); idempotencia en escrituras.

Checklist de Buenas Prácticas
- Versionar y pinnear dependencias críticas (NumPy/Pandas/PyArrow).
- Mantener UDFs deterministas y con tipos explícitos.
- Registrar metadatos de ejecución y resultados.
- Separar configuración de código; evitar credenciales en repositorio.
- Validar esquemas y manejar casos nulos/errores sin detener la pipeline.

Conclusión
- Con la imagen de Spark preconfigurada y el catálogo de Pandas UDFs, el sistema ofrece rendimiento y flexibilidad. La configuración YAML habilita dinamismo y gobernanza; las estrategias de seguridad y errores aseguran operación robusta en entornos productivos.

Próximo Paso: Ingesta Multi-Fuentes y Conversión a Parquet
- Objetivo: habilitar ingesta desde CSV, JSON/JSONL, JDBC y APIs (incluyendo rutas `s3a://`) de forma configurable y robusta.

- Esquema de configuración propuesto (`dataset.yml`):
```
source:
  type: csv | json | jsonl | parquet | jdbc | api
  input_format: csv | json | jsonl | parquet
  path: s3a://bucket/raw/payments_v1           # csv/json/parquet
  options:                                      # opciones específicas de lectura
    header: true
    inferSchema: false
    multiline: false
    encoding: UTF-8
    sep: ","
  schema_ref: configs/schemas/payments_v1.json  # opcional: esquema explícito
  # JDBC
  jdbc:
    url: jdbc:postgresql://db:5432/maindb
    table: public.payments
    user_ref: db_user
    password_ref: db_password
    fetchsize: 10000
    partitionColumn: id                  # para paralelismo
    lowerBound: 1
    upperBound: 10000000
    numPartitions: 8
  # API
  api:
    endpoint: https://api.example.com/payments
    method: GET
    headers_ref: api_headers
    page_param: page
    page_size: 1000
    max_pages: 100
    staging:
      enabled: true
      path: s3a://data-lake/raw/payments_v1_api
      format: jsonl                       # o parquet (ver recomendaciones)
```

- Lectura unificada (fábrica de fuentes):
  - `csv/json/jsonl/parquet`: `spark.read.options(**opts).<fmt>(path)`.
  - `jdbc`: `spark.read.format("jdbc").options(**jdbc_opts).load()` con particionamiento por `partitionColumn`.
  - `api`: extracción paginada con `requests`, escritura en staging (`jsonl` o `parquet`) y lectura posterior con Spark.
  - Rutas S3: usar `s3a://...` y asegurar configuración de S3A (credenciales y endpoint) antes de leer/escribir.

- Manejo de S3A en ejecución:
  - Configurar `fs.s3a.*` en `spark-submit` (`runner.sh`) y/o via `SparkSession` (ver `maybe_config_s3a`).
  - Validar conectividad y permisos con un listado previo (`spark.read.parquet("s3a://...")` de prueba o `hadoop fs -ls`).

- ¿Convertir a Parquet antes de las validaciones?
  - Ventajas:
    - Formato columnar, compresión y predicate pushdown: menor IO, mayor rendimiento.
    - Esquema explícito: facilita tipado y detección temprana de inconsistencias.
    - Integración natural con Spark y herramientas analíticas.
  - Desventajas:
    - Coste adicional de escritura previa y duplicación de almacenamiento si no se gestionan ciclos de vida.
    - Riesgo de fijar un esquema demasiado pronto si el Raw es altamente variable.
    - En flujos streaming o near-real-time, añade latencia si se hace como paso separado.
  - Recomendación:
    - Para ingestas masivas (APIs paginadas, CSV/JSON grandes) y validaciones complejas, realizar un paso de "Bronze" que convierta de Raw (JSONL/CSV) a Parquet con un esquema controlado. Luego aplicar validaciones en Silver sobre Parquet.
    - Para ingestas pequeñas o exploratorias, validar directamente sobre JSON/CSV y convertir a Parquet al finalizar la limpieza.
    - Umbral práctico: si el dataset supera ~5–10 GB por corrida o si se proyecta reusar los datos en múltiples pipelines, conviene Parquet temprano.

- Ejemplo de pipeline con Bronze→Silver:
```
# Bronze: convertir Raw JSONL a Parquet particionado por fecha
raw_df = spark.read.options(multiline=False).json("s3a://lake/raw/payments_v1_api/")
bronze_df = (raw_df
  .withColumn("ingestion_date", F.to_date(F.col("ingestion_ts")))
  .dropDuplicates(["id"]))
bronze_df.write.mode("overwrite").partitionBy("ingestion_date").parquet("s3a://lake/bronze/payments_v1")

# Silver: leer Parquet y aplicar transformaciones/validaciones
df = spark.read.parquet("s3a://lake/bronze/payments_v1")
df = apply_expr_transforms(df, cfg["transformations"])      # expr
df = apply_udf_transforms(df, cfg["transformations"])       # Pandas UDFs
df, quarantine_df, stats = apply_quality(df, rules, quarantine_path, run_id)
```

- Próximas acciones sugeridas:
  - Definir `pipelines/sources.py` con `load_source(cfg, spark, env)` para la fábrica de fuentes.
  - Extender `dataset.yml` y validadores de configuración para soportar `jdbc` y `api` con `staging`.
  - Añadir jobs de Bronze opcionales (Raw→Parquet) parametrizados por dataset.
  - Documentar políticas de ciclo de vida en S3/MinIO (retención Raw vs Bronze/Silver/Gold).