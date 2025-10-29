# Ejecución por capas en Azure Databricks

La siguiente guía ofrece bloques "copy-paste" para ejecutar `prodi` sobre un
cluster de Azure Databricks almacenando configuraciones en DBFS y leyendo/escribiendo
datos en ABFSS. Todos los ejemplos se basan en las plantillas
`templates/azure/dbfs/` del repositorio.

> **Requisitos previos**
>
> - Workspace de Databricks conectado a un Data Lake Gen2 con el contenedor de
>   ejemplo (`landing`, `bronze`, `silver`, `gold`).
> - Acceso a la cuenta de Git donde vive este repositorio.
> - Un cluster con Databricks Runtime 13.3+ (incluye Python 3.10 y PySpark).

## 1. Instalar `prodi` desde el repositorio

Ejecuta el siguiente comando en un notebook (célula `%pip`). Sustituye
`<REPO_URL>` por la URL HTTPS de tu fork o mirror y verifica que el subdirectorio
`lib/datacore` exista en el repositorio remoto.

```python
%pip install "git+<REPO_URL>#subdirectory=lib/datacore"
```

Reinicia el cluster cuando Databricks lo solicite para que el entrypoint
`prodi` quede disponible en `%sh`.

## 2. Configurar credenciales ABFSS

Asumiendo que la cuenta de almacenamiento es `contosodatalake` y dispones de la
llave en un Secret Scope, ejecuta:

```python
import os

account_key = dbutils.secrets.get(scope="datalake", key="contosodatalake-key")
os.environ["DATABRICKS_ABFSS_KEY"] = account_key

spark.conf.set(
    "fs.azure.account.key.contosodatalake.dfs.core.windows.net",
    account_key,
)
```

> **Nota:** Si utilizas Service Principal, sustituye la sección anterior por las
> claves `tenant_id`, `client_id` y `client_secret` y define los environment
> variables `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_CLIENT_ID`, `AZURE_TENANT_ID` y
> `AZURE_CLIENT_SECRET`. El YAML `env.yml` acepta `credentials.client_id_env`,
> `client_secret_env` y `tenant_id_env` para ese escenario.

## 3. Publicar configuraciones en DBFS

Crea la estructura `/configs/datacore/` en DBFS y copia las plantillas.

```python
base_dir = "dbfs:/configs/datacore"

dbutils.fs.mkdirs(f"{base_dir}/cfg")
dbutils.fs.mkdirs(f"{base_dir}/datasets")
```

### 3.1 Configuración de entorno

```python
dbutils.fs.put(
    f"{base_dir}/cfg/env.yml",
    """timezone: UTC
storage:
  abfss:
    credentials:
      account_name: contosodatalake
      account_key_env: DATABRICKS_ABFSS_KEY
    storage_options:
      use_ssl: true
      azure_cloud: AzurePublicCloud
""",
    overwrite=True,
)
```

### 3.2 Dataset `toy_customers`

```python
dbutils.fs.put(
    f"{base_dir}/datasets/toy_customers.yml",
    """id: toy_customers
description: Toy customer sample configured for DBFS templates.
layers:
  raw:
    compute:
      engine: spark
      options:
        spark.sql.shuffle.partitions: 1
    io:
      source:
        format: csv
        options:
          header: true
        uris:
          abfss: abfss://landing@contosodatalake.dfs.core.windows.net/raw/toy_customers/input/
      sink:
        format: delta
        mode: overwrite
        uris:
          abfss: abfss://landing@contosodatalake.dfs.core.windows.net/raw/toy_customers/landing/
  bronze:
    compute:
      engine: spark
    io:
      sink:
        format: delta
        mode: overwrite
        options:
          checkpointLocation: abfss://bronze@contosodatalake.dfs.core.windows.net/bronze/checkpoints/toy_customers/
        uris:
          abfss: abfss://bronze@contosodatalake.dfs.core.windows.net/bronze/toy_customers/
  silver:
    compute:
      engine: spark
    dq:
      expectations:
        min_row_count: 1
    io:
      sink:
        format: delta
        mode: overwrite
        options:
          checkpointLocation: abfss://silver@contosodatalake.dfs.core.windows.net/silver/checkpoints/toy_customers/
        uris:
          abfss: abfss://silver@contosodatalake.dfs.core.windows.net/silver/toy_customers/
  gold:
    compute:
      engine: spark
    io:
      sink:
        format: delta
        mode: overwrite
        uris:
          abfss: abfss://gold@contosodatalake.dfs.core.windows.net/gold/toy_customers/
""",
    overwrite=True,
)
```

### 3.3 Configuraciones por capa

```python
layer_cfgs = {
    "raw": """layer: raw
environment: development
dry_run: false
io:
  source:
    dataset_config: /dbfs/configs/datacore/datasets/toy_customers.yml
    environment_config: /dbfs/configs/datacore/cfg/env.yml
    preferred_protocol: abfss
  sink:
    kind: spark
    format: delta
    mode: overwrite
    uris:
      abfss: abfss://landing@contosodatalake.dfs.core.windows.net/raw/toy_customers/landing/
storage:
  abfss:
    default_uri: abfss://landing@contosodatalake.dfs.core.windows.net/raw/toy_customers/
""",
    "bronze": """layer: bronze
environment: development
dry_run: false
io:
  source:
    dataset_config: /dbfs/configs/datacore/datasets/toy_customers.yml
    environment_config: /dbfs/configs/datacore/cfg/env.yml
    format: delta
    path: abfss://landing@contosodatalake.dfs.core.windows.net/raw/toy_customers/landing/
  sink:
    kind: spark
    format: delta
    mode: overwrite
    path: abfss://bronze@contosodatalake.dfs.core.windows.net/bronze/toy_customers/
    uris:
      abfss: abfss://bronze@contosodatalake.dfs.core.windows.net/bronze/toy_customers/
    options:
      checkpointLocation: abfss://bronze@contosodatalake.dfs.core.windows.net/bronze/checkpoints/toy_customers/
compute:
  engine: spark
""",
    "silver": """layer: silver
environment: development
dry_run: false
io:
  source:
    dataset_config: /dbfs/configs/datacore/datasets/toy_customers.yml
    environment_config: /dbfs/configs/datacore/cfg/env.yml
    format: delta
    path: abfss://bronze@contosodatalake.dfs.core.windows.net/bronze/toy_customers/
  sink:
    kind: spark
    format: delta
    mode: overwrite
    path: abfss://silver@contosodatalake.dfs.core.windows.net/silver/toy_customers/
    uris:
      abfss: abfss://silver@contosodatalake.dfs.core.windows.net/silver/toy_customers/
    options:
      checkpointLocation: abfss://silver@contosodatalake.dfs.core.windows.net/silver/checkpoints/toy_customers/
compute:
  engine: spark
dq:
  fail_on_error: true
  expectations:
    - type: expect_column_to_exist
      column: customer_id
    - type: expect_column_to_exist
      column: signup_ts
""",
    "gold": """layer: gold
environment: development
dry_run: false
io:
  source:
    dataset_config: /dbfs/configs/datacore/datasets/toy_customers.yml
    environment_config: /dbfs/configs/datacore/cfg/env.yml
    format: delta
    path: abfss://silver@contosodatalake.dfs.core.windows.net/silver/toy_customers/
  sink:
    kind: spark
    format: delta
    mode: overwrite
    path: abfss://gold@contosodatalake.dfs.core.windows.net/gold/toy_customers/
    uris:
      abfss: abfss://gold@contosodatalake.dfs.core.windows.net/gold/toy_customers/
compute:
  engine: spark
""",
}

for name, content in layer_cfgs.items():
    dbutils.fs.put(f"{base_dir}/cfg/{name}.yml", content, overwrite=True)
```

## 4. Sembrar datos de entrada

Carga un CSV mínimo en el contenedor `landing` para que la capa Raw tenga datos
de origen.

```python
input_path = "abfss://landing@contosodatalake.dfs.core.windows.net/raw/toy_customers/input/"

spark.createDataFrame(
    [
        ("CUST-001", "Retail", "active", "2024-01-01T08:00:00Z"),
        ("CUST-002", "SMB", "inactive", "2024-01-05T12:30:00Z"),
    ],
    ["CUSTOMER_ID", "SEGMENT", "STATUS", "SIGNUP_TS"],
).coalesce(1).write.mode("overwrite").option("header", True).csv(input_path)
```

## 5. Validar y ejecutar cada capa

Utiliza `%sh` para invocar la CLI sobre los archivos almacenados en `/dbfs`.

```bash
%sh
prodi validate -c /dbfs/configs/datacore/cfg/raw.yml
prodi validate -c /dbfs/configs/datacore/cfg/bronze.yml
prodi validate -c /dbfs/configs/datacore/cfg/silver.yml
prodi validate -c /dbfs/configs/datacore/cfg/gold.yml
```

```bash
%sh
prodi run-layer raw -c /dbfs/configs/datacore/cfg/raw.yml
prodi run-layer bronze -c /dbfs/configs/datacore/cfg/bronze.yml
prodi run-layer silver -c /dbfs/configs/datacore/cfg/silver.yml
prodi run-layer gold -c /dbfs/configs/datacore/cfg/gold.yml
```

Cada ejecución valida automáticamente el YAML, carga el dataset y procesa los
sinks definidos en ABFSS.

## 6. Verificar la salida Delta

Después de ejecutar `silver` o `gold`, inspecciona las tablas Delta resultantes
con Spark para confirmar que la canalización generó datos.

```python
silver_df = spark.read.format("delta").load(
    "abfss://silver@contosodatalake.dfs.core.windows.net/silver/toy_customers/"
)
silver_df.show()

gold_df = spark.read.format("delta").load(
    "abfss://gold@contosodatalake.dfs.core.windows.net/gold/toy_customers/"
)
gold_df.show()
```

Las rutas coinciden con los `uris.abfss` declarados en el dataset y las
configuraciones por capa, garantizando coherencia entre documentación y código.
