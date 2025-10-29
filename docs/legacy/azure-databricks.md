# Ejecución por capas en Azure Databricks

**Estado:** flujo legacy. Será reemplazado por la CLI `prodi plan/run`; conserva detalles históricos de `/dbfs/configs/datacore/**`.

La siguiente guía ofrece bloques "copy-paste" para ejecutar `prodi` sobre un
cluster de Azure Databricks almacenando configuraciones en DBFS y
leyendo/escribiendo datos en ABFSS. Todos los ejemplos usan las plantillas de
`templates/azure/dbfs/` y una única cuenta `<ACCOUNT>` con el contenedor
`landing` para las capas `raw`, `bronze`, `silver` y `gold`.

> **Requisitos previos**
>
> - Workspace de Databricks conectado a un Data Lake Gen2.
> - Secret Scope (`kv-scope` en el ejemplo) con la llave de la cuenta `<ACCOUNT>`.
> - Un cluster con Databricks Runtime 13.3+ (Python 3.10 y PySpark 3.4+).
> - Acceso al repositorio `Prospect121/mvp-config-driven` o a tu fork.

## 1. Sincronizar el repositorio

Desde la barra lateral de Databricks selecciona **Repos → Add Repo** y apunta al
repositorio. Una vez clonado, abre un notebook en la ruta
`Repos/<user>/mvp-config-driven`.

## 2. Instalar `prodi`

En una celda `%pip`, instala el paquete editable ubicado en `lib/datacore`:

```python
%pip install -e lib/datacore
```

Reinicia el cluster cuando Databricks lo solicite para que el entrypoint `prodi`
quede disponible en celdas `%sh` y en `python`.

## 3. Configurar credenciales ABFSS

Recupera la llave de almacenamiento desde el Secret Scope y propágala tanto a
Spark como a variables de entorno. Sustituye `<ACCOUNT>` por el nombre real de
la cuenta (por ejemplo, `contosodatalake`).

```python
account = "<ACCOUNT>"
scope = "kv-scope"
key = "abfss-key"

secret = dbutils.secrets.get(scope=scope, key=key)

spark.conf.set(
    f"fs.azure.account.key.{account}.dfs.core.windows.net",
    secret,
)

import os
os.environ["AZURE_STORAGE_ACCOUNT_NAME"] = account
os.environ["AZURE_STORAGE_ACCOUNT_KEY"] = secret
```

> **Service Principal:** si en lugar de account key utilizas un principal,
> define los secretos `client_id`, `client_secret` y `tenant_id`, asigna sus
> valores a `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID` y
> mantén `AZURE_STORAGE_ACCOUNT_NAME` con el nombre de la cuenta. El YAML de
> entorno acepta esas variables sin cambios adicionales.

## 4. Publicar configuraciones en DBFS

Crea la estructura `/configs/datacore/` y publica las plantillas.

```python
base_dir = "dbfs:/configs/datacore"

dbutils.fs.mkdirs(f"{base_dir}/cfg")
dbutils.fs.mkdirs(f"{base_dir}/datasets")
```

### 4.1 Configuración de entorno

```python
dbutils.fs.put(
    f"{base_dir}/env.yml",
    """timezone: America/Bogota
storage:
  abfss:
    credentials:
      account_name_env: AZURE_STORAGE_ACCOUNT_NAME
      account_key_env: AZURE_STORAGE_ACCOUNT_KEY
    storage_options: {}
  s3:
    credentials: {}
    storage_options: {}
  gs:
    credentials: {}
    storage_options: {}
""",
    overwrite=True,
)
```

### 4.2 Dataset `toy_customers`

```python
dbutils.fs.put(
    f"{base_dir}/datasets/toy_customers.yml",
    """id: toy_customers
description: Toy customer sample configured for Azure Databricks.
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
          abfss: abfss://landing@<ACCOUNT>.dfs.core.windows.net/raw/toy/customers/source/
        local_fallback: file:/tmp/raw/toy_customers/source/
      sink:
        format: delta
        mode: overwrite
        uris:
          abfss: abfss://landing@<ACCOUNT>.dfs.core.windows.net/raw/toy/customers/
  bronze:
    compute:
      engine: spark
    io:
      sink:
        format: delta
        mode: overwrite
        options:
          checkpointLocation: abfss://landing@<ACCOUNT>.dfs.core.windows.net/_checkpoints/bronze/customers/
        uris:
          abfss: abfss://landing@<ACCOUNT>.dfs.core.windows.net/bronze/customers/
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
          checkpointLocation: abfss://landing@<ACCOUNT>.dfs.core.windows.net/_checkpoints/silver/customers/
        uris:
          abfss: abfss://landing@<ACCOUNT>.dfs.core.windows.net/silver/customers/
  gold:
    compute:
      engine: spark
    io:
      sink:
        format: delta
        mode: overwrite
        uris:
          abfss: abfss://landing@<ACCOUNT>.dfs.core.windows.net/gold/customers/
""",
    overwrite=True,
)
```

### 4.3 Configuraciones por capa

```python
layer_cfgs = {
    "raw": """dry_run: false
environment: development
environment_config: /dbfs/configs/datacore/env.yml
compute:
  engine: spark
io:
  source:
    dataset_config: /dbfs/configs/datacore/datasets/toy_customers.yml
  sink:
    uris:
      abfss: abfss://landing@<ACCOUNT>.dfs.core.windows.net/raw/toy/customers/
    mode: overwrite
""",
    "bronze": """dry_run: false
environment: development
environment_config: /dbfs/configs/datacore/env.yml
compute:
  engine: spark
io:
  source:
    dataset_config: /dbfs/configs/datacore/datasets/toy_customers.yml
    format: parquet
    path: abfss://landing@<ACCOUNT>.dfs.core.windows.net/raw/toy/customers/
  sink:
    uris:
      abfss: abfss://landing@<ACCOUNT>.dfs.core.windows.net/bronze/customers/
    options:
      checkpointLocation: abfss://landing@<ACCOUNT>.dfs.core.windows.net/_checkpoints/bronze/customers/
    mode: overwrite
    coalesce: 1
""",
    "silver": """dry_run: false
environment: development
environment_config: /dbfs/configs/datacore/env.yml
compute:
  engine: spark
io:
  source:
    dataset_config: /dbfs/configs/datacore/datasets/toy_customers.yml
    format: delta
    path: abfss://landing@<ACCOUNT>.dfs.core.windows.net/bronze/customers/
  sink:
    uris:
      abfss: abfss://landing@<ACCOUNT>.dfs.core.windows.net/silver/customers/
    options:
      checkpointLocation: abfss://landing@<ACCOUNT>.dfs.core.windows.net/_checkpoints/silver/customers/
    mode: overwrite
""",
    "gold": """dry_run: false
environment: development
environment_config: /dbfs/configs/datacore/env.yml
compute:
  engine: spark
io:
  source:
    dataset_config: /dbfs/configs/datacore/datasets/toy_customers.yml
    format: delta
    path: abfss://landing@<ACCOUNT>.dfs.core.windows.net/silver/customers/
  sink:
    uris:
      abfss: abfss://landing@<ACCOUNT>.dfs.core.windows.net/gold/customers/
    mode: overwrite
""",
}

for name, payload in layer_cfgs.items():
    dbutils.fs.put(
        f"{base_dir}/cfg/{name}.yml",
        payload,
        overwrite=True,
    )
```

## 5. Validar y ejecutar las capas

Usa celdas `%sh` para invocar `prodi` directamente desde el driver. Cada comando
trabaja con rutas `/dbfs/...` que apuntan a los archivos publicados en el paso
anterior.

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
Delta en la cuenta `<ACCOUNT>`.

## 6. Verificar la salida Gold

Con las cuatro capas finalizadas, lee y muestra el Delta consolidado:

```python
gold_path = "abfss://landing@<ACCOUNT>.dfs.core.windows.net/gold/customers/"
df = spark.read.format("delta").load(gold_path)
display(df)
print(f"Total de registros: {df.count()}")
```

## 7. Smoke test resumido

Coloca las celdas siguientes al final del notebook para realizar un smoke test
rápido después de un cambio en configuraciones o plantillas.

```python
# %pip install -e lib/datacore
# dbutils.fs.put(...)
# spark.conf.set(...)
```

```bash
%sh
prodi validate -c /dbfs/configs/datacore/cfg/raw.yml
prodi validate -c /dbfs/configs/datacore/cfg/bronze.yml
prodi validate -c /dbfs/configs/datacore/cfg/silver.yml
prodi validate -c /dbfs/configs/datacore/cfg/gold.yml
prodi run-layer raw -c /dbfs/configs/datacore/cfg/raw.yml
prodi run-layer bronze -c /dbfs/configs/datacore/cfg/bronze.yml
prodi run-layer silver -c /dbfs/configs/datacore/cfg/silver.yml
prodi run-layer gold -c /dbfs/configs/datacore/cfg/gold.yml
```

```python
df = spark.read.format("delta").load(
    "abfss://landing@<ACCOUNT>.dfs.core.windows.net/gold/customers/"
)
display(df)
df.count()
```

Si todos los comandos se ejecutan sin errores y `df.count()` devuelve un valor
mayor a cero, el flujo por capas quedó correctamente configurado en Databricks.
