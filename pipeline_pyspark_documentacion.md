# Pipeline de estandarización y calidad con PySpark — Documentación para presentación

> **Objetivo:** ejecutar un pipeline **config-driven** (impulsado por configuración) que ingiere archivos (CSV/JSON/Parquet), **estandariza**, **valida** (calidad de datos) con cuarentena, **enriquece con auditoría**, y escribe datos “silver” con **particionado dinámico**; soporta S3A/MinIO.

---

## 1) Resumen ejecutivo (para la primera diapositiva)

- **¿Qué hace?**  
  Ingiere datos brutos, aplica **renombres, casts, defaults, deduplicación**, valida reglas de **calidad** (fail/quarantine/drop/warn), agrega **auditoría**, y persiste en formato definido (por defecto, **Parquet**) con **particiones**.

- **¿Por qué así?**  
  Todo el comportamiento es **declarativo** vía archivos de **configuración** y **JSON Schema**, lo que reduce cambios de código ante nuevas columnas, tipos o reglas.

- **Dónde se ejecuta:**  
  Local, Docker (Bitnami Spark 3.5.x) o cluster Spark (on-prem / cloud). Compatible con **S3A/MinIO** para lectura/escritura y cuarentena.

---

## 2) Diagrama de flujo (alto nivel)

```
┌────────┐    read     ┌───────────┐   rename   ┌───────────┐   enforce    ┌───────────┐   casts/     ┌───────────┐
│ Source │ ───────────▶│ DataFrame │───────────▶│ estandar. │─────────────▶│  schema   │─────────────▶│ defaults  │
└────────┘             └───────────┘            └───────────┘              └───────────┘              └───────────┘
                                                                                          deduplicate    │
                                                                                                         ▼
                                                                                                 ┌─────────────┐
                                                                                                 │ auditoría   │ (_run_id, _ingestion_ts)
                                                                                                 └─────────────┘
                                                                                                         │
                                                                                                         ▼
                                                                                               ┌─────────────────┐
                                                                                               │ calidad (rules) │
                                                                                               │ fail/quarantine │──┐
                                                                                               │ drop/warn       │  │
                                                                                               └─────────────────┘  │
                                                                                                         │           │ write bad_df
                                                                                                         ▼           ▼
                                                                                                   ┌─────────┐  ┌───────────┐
                                                                                                   │  write  │  │ quarantine│
                                                                                                   └─────────┘  └───────────┘
```

---

## 3) Entradas y salidas

- **Entradas del script (CLI):**  
  `python script.py <cfg.yml> <env.yml>`

- **Archivos de configuración:**
  - `cfg.yml`: comportamiento del pipeline (origen, formato, renombres, casts, defaults, deduplicate, schema, quality, salida).
  - `env.yml`: parámetros de entorno (por ejemplo, `timezone`, `s3a_endpoint`).

- **Datos de soporte (opcionales):**
  - `schema.json`: **JSON Schema** para tipado/orden/required.
  - `expectations.yml`: reglas de **calidad**.

- **Salida principal:**  
  Dataset **silver** (por defecto Parquet) en ruta y **particiones** configuradas.

- **Salida secundaria:**  
  Dataset de **cuarentena** (Parquet) con `_quarantine_reason`, `_run_id`, `_ingestion_ts` cuando existan registros inválidos.

---

## 4) Estructura de configuración (plantillas)

### 4.1 `cfg.yml` (ejemplo)

```yaml
id: payroll_v1

source:
  input_format: csv            # csv | json | jsonl | parquet
  path: s3a://bronze/payroll/2025/10/*.csv
  options:
    header: "true"
    delimiter: ","
    encoding: "utf-8"

standardization:
  rename:
    - { from: "emp_id", to: "employee_id" }
  casts:
    - { column: "payment_date", to: "timestamp", format_hint: "yyyy-MM-dd" }
    - { column: "amount", to: "decimal(18,2)", on_error: "null" }  # null|fail
  defaults:
    - { column: "currency", value: "CLP" }
  deduplicate:
    key: ["employee_id", "payment_date"]
    order_by: ["updated_at desc", "ingestion_ts desc"]

schema:
  ref: schemas/payroll.schema.json   # JSON Schema
  mode: strict                       # strict|lenient

quality:
  expectations_ref: quality/payroll.rules.yml
  quarantine: s3a://quarantine/payroll/

output:
  silver:
    path: s3a://silver/payroll/
    format: parquet
    merge_schema: true
    partition_by: ["year", "month"]   # se crean si existe payment_date
    mode: overwrite_dynamic            # append|overwrite_dynamic
    partition_from: payment_date  
```

### 4.2 `env.yml` (ejemplo)

```yaml
timezone: "UTC"
s3a_endpoint: "http://minio:9000"
```

> **Credenciales**: se toman de variables de entorno `MINIO_ROOT_USER/MINIO_ROOT_PASSWORD` o `AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY`.

### 4.3 `schemas/payroll.schema.json` (fragmento)

```json
{
  "type": "object",
  "required": ["employee_id", "payment_date", "amount"],
  "additionalProperties": false,
  "properties": {
    "employee_id": { "type": ["string", "null"] },
    "payment_date": { "type": ["string", "null"], "format": "date-time" },
    "amount": { "type": ["number", "null"] },
    "currency": { "type": ["string", "null"] }
  }
}
```

### 4.4 `quality/payroll.rules.yml` (ejemplo)

```yaml
rules:
  - name: amount_positive
    expr: "amount > 0"
    on_fail: quarantine   # fail|quarantine|drop|warn
  - name: has_employee
    expr: "employee_id IS NOT NULL AND employee_id <> ''"
    on_fail: fail
  - name: date_recent
    expr: "payment_date >= date_sub(current_date(), 365)"
    on_fail: warn
```

---

## 5) Flujo de ejecución (paso a paso)

1. **SparkSession**  
   Configura `appName`, zona horaria (`env.timezone`) y `partitionOverwriteMode=dynamic` para **overwrite** por partición.

2. **Autoconfig S3A (MinIO/AWS)**  
   Si la ruta empieza por `s3a://`, aplica endpoint/credenciales con `HadoopConfiguration`.

3. **Lectura**  
   Según `source.input_format`: `csv/json/jsonl/parquet` con `options`.

4. **Estandarización**  
   - **rename**: renombra columnas antes de schema.
   - **schema.enforce**: usa **JSON Schema** para:
     - Verificar **required** y `additionalProperties`.
     - Crear **columnas ausentes** con tipo aproximado (según `type`).
     - **Orden de columnas** como en el schema (modo `strict`).
     - Auto-cast si `format: "date-time"` → `timestamp`.

   - **casts**: `safe_cast()` soporta:
     - Date/Timestamp con `format_hint`.
     - Tipos primitivos y `decimal(p,s)` con validación (escala ≤ precisión).
     - `on_error: "null"` usa `try_cast()` (Spark 3.5+) para **null-on-error**.

   - **defaults**: reemplaza `NULL` por un valor literal configurable.

   - **deduplicate**: `row_number()` sobre ventana `partitionBy(key)` y `order_by`. Mantiene la primera fila por clave.

5. **Particiones por fecha (opcional)**  
   Si existe `partition_from: payment_date`, crea `year` y `month` (para `partition_by`).

6. **Auditoría**  
   Agrega `_run_id` (UTC) y `_ingestion_ts` (`current_timestamp()`).

7. **Calidad de datos**  
   - **fail**: si se viola, se aborta la corrida.  
   - **quarantine**: separa registros inválidos; concatena reglas fallidas en `_quarantine_reason` y escribe a `quality.quarantine`.  
   - **drop**: descarta registros que no cumplen.  
   - **warn**: solo contabiliza y reporta.  
   Imprime estadísticas: `quarantine_count`, `dropped_count`, `warn_count`.

8. **Write**  
   Configura formato (`parquet` por defecto), `mergeSchema`, `partition_by`.  
   - `mode: overwrite_dynamic` → `writer.mode('overwrite')` con overwrite **particionado**.  
   - `mode: append` → append normal.

---

## 6) Módulos y funciones clave

- **Tipado**
  - `norm_type(t)`: normaliza alias (`int`, `varchar`, `numeric`, etc.) y valida `decimal(p,s)`.
  - `safe_cast(df,col,to,fmt,on_error)`: casteo seguro; para `timestamp`/`date` usa `to_timestamp/to_date`; con `on_error:null` aplica `try_cast()`.

- **Esquema**
  - `load_json_schema(path)`: carga JSON Schema.
  - `spark_type_from_json(t)`: mapea tipos JSON Schema (`number→double`, `integer→bigint`, `array/object→string`).
  - `enforce_schema(df,jsch,mode)`: controla **required**, **orden**, **additionalProperties**, y castea por `format: date-time`.

- **Calidad**
  - `load_expectations(path)`: lee YAML de reglas.
  - `apply_quality(df,rules,quarantine_path,run_id)`: implementa `fail/quarantine/drop/warn`, genera estadísticas y escribe cuarentena.

- **Otros**
  - `parse_order(exprs)`: convierte expresiones `"col [desc]"` a `F.col(...).asc/desc`.
  - `maybe_config_s3a(path)`: configura S3A solo si la ruta es `s3a://`.

---

## 7) Cómo ejecutarlo

### 7.1 spark-submit (local)

```bash
spark-submit   --master local[*]   --conf spark.sql.session.timeZone=UTC   script.py cfg.yml env.yml
```

### 7.2 Docker (Bitnami Spark 3.5.1)

```yaml
runner:
  image: bitnami/spark:3.5.1
  container_name: cfg-runner
  working_dir: /mvp
  command: ["bash", "-lc", "python script.py cfg.yml env.yml"]
  environment:
    - MINIO_ROOT_USER=minio
    - MINIO_ROOT_PASSWORD=minio12345
    - S3A_ENDPOINT=http://minio:9000
    - JAVA_TOOL_OPTIONS=-Duser.home=/tmp -Divy.home=/tmp/.ivy2 -Duser.name=spark
  volumes:
    - ./:/mvp
  depends_on:
    - spark-master
    - spark
```

> **Nota:** si el **master** de Spark está fuera del contenedor, reemplaza el comando por `spark-submit ...`.

---

## 8) Buenas prácticas y performance

- **Determinismo en deduplicación:** define **siempre** `order_by` (por ejemplo, `updated_at desc`). Sin orden, el resultado puede ser no determinista.
- **Casts masivos:** aplica `safe_cast` después de `enforce_schema` para minimizar re-casteos.
- **Acciones múltiples (`count`, `printSchema`)**: generan jobs. Para datasets grandes, considera **`df.cache()`** antes de contadores múltiple uso (calidad + write).
- **Particionado eficaz:** usa **particiones** (`year`, `month`) para lecturas predictivas y sobrescritura dinámica.
- **Escritura idempotente:** preferir `overwrite_dynamic` cuando re-procesas particiones existentes; **append** si solo agregas.
- **S3A/MinIO:** usa **path-style** (`fs.s3a.path.style.access=true`), define `endpoint`, y credenciales por **variables de entorno** (no hardcodear).
- **Logs y observabilidad:** hoy hay `print(...)`. En producción, preferir `log4j` o Structured Logging + métricas (contadores por regla, filas procesadas, etc.).

---

## 9) Seguridad

- **Secretos por entorno:** `MINIO_ROOT_USER/MINIO_ROOT_PASSWORD` o `AWS_*` solo como variables de entorno. No guardarlos en Git.
- **Cifrado en reposo y tránsito:** si el destino soporta, habilitar **encryption at rest** (bucket/server-side) y **TLS** en el endpoint.
- **Cuarentena**: contiene datos potencialmente sensibles/erróneos; restringir acceso y ciclo de vida (políticas de retención).

---

## 10) Problemas conocidos y mejoras recomendadas

1) **Doble `save` al final**  
   El script ejecuta:
   ```python
   writer.mode('append').save(out['path'])
   ...
   writer.save(out['path'])
   ```
   Esto **escribe dos veces**. **Solución**: elimina el `writer.save(out['path'])` final y conserva solo el bloque controlado por `mode_cfg`:

   ```python
   if mode_cfg == 'overwrite_dynamic':
       writer.mode('overwrite').save(out['path'])
   else:
       writer.mode('append').save(out['path'])
   print(f"OK :: wrote to {out['path']}")
   ```

2) **Deduplicación sin `order_by`**  
   `row_number()` sin orden es no determinista. Define una métrica de “reciente” (p.ej., `updated_at desc, _ingestion_ts desc`).

3) **Cuentas innecesarias**  
   `bad_df.count()`, `stats` con múltiples `count()`, y `df.count()` para logging pueden disparar **varios jobs**. Optimiza con `cache()` o calcula métricas derivadas tras una única acción si el volumen es alto.

4) **Types de JSON Schema complejos**  
   `array/object` se **stringifican**. Si se necesita anidado real, extender `spark_type_from_json` y `enforce_schema` para `StructType/ArrayType`.

5) **Particiones por fecha**  
   Se infieren solo si existe `payment_date`. Si particionas por otras fechas, agrega la lógica o define `partition_by` acordes.

6) **Formato de salida**  
   Hoy usa `parquet` por defecto. Para **ACID**/updates/upserts a futuro, evaluar **Delta Lake / Apache Hudi / Apache Iceberg** y ajustar `writer.format(...)`.

---

## 11) Guion de diapositivas (versión corta)

1. **Título & Objetivo**: pipeline config-driven para estandarizar + validar + particionar.  
2. **Arquitectura**: lectura → estandarización → schema → calidad → auditoría → escritura (+ cuarentena).  
3. **Configuración**: `cfg.yml`, `env.yml`, `schema.json`, `expectations.yml`.  
4. **Estandarización**: rename, casts (safe_cast), defaults, deduplicate (window).  
5. **Schema**: JSON Schema (required, orden, additionalProperties, date-time).  
6. **Calidad**: fail/quarantine/drop/warn; estadísticas y cuarentena con razones.  
7. **Particionado & Auditoría**: `year/month` + `_run_id` `_ingestion_ts`.  
8. **S3A/MinIO**: endpoint, path-style, credenciales por env.  
9. **Ejecución**: `spark-submit` / Docker Bitnami Spark.  
10. **Seguridad & Performance**: secretos, cifrado, counts, cache, overwrite_dynamic.  
11. **Riesgos & Fixes**: doble save, dedupe sin orden, tipos complejos.  
12. **Q&A**.

---

## 12) Apéndices útiles

### 12.1 Aliases de tipos soportados (extracto de `ALIASES`)
- `str|string|varchar|char → string`
- `int|integer → int`
- `bigint|long → bigint`
- `double|float|number|numeric → double`
- `boolean|bool → boolean`
- `date → date`
- `datetime|timestamp|timestamptz|date-time → timestamp`
- `decimal → decimal(38,18)` (por defecto)

### 12.2 Reglas de calidad — semántica
- **fail**: si alguna fila viola la regla, **aborta** el job.
- **quarantine**: filas violatorias van a **parquet** de cuarentena, con motivo agregado.
- **drop**: filas violatorias se **descartan** del dataset final.
- **warn**: solo **conteo**; no altera datos.

### 12.3 Variables de entorno típicas (MinIO/S3A)

```bash
export MINIO_ROOT_USER=minio
export MINIO_ROOT_PASSWORD=minio12345
export S3A_ENDPOINT=http://minio:9000
# Alternativa AWS:
# export AWS_ACCESS_KEY_ID=...
# export AWS_SECRET_ACCESS_KEY=...
```

---

## 13) Checklist de pre-producción

- [ ] `cfg.yml` validado (rutas, formato, particiones, modo de escritura).  
- [ ] `schema.json` al día (required, orden correcto, formatos).  
- [ ] `expectations.yml` revisado (reglas y severidades).  
- [ ] **Deduplicación** con `order_by` determinista.  
- [ ] **Removido** el doble `writer.save(...)`.  
- [ ] Credenciales por **env**; sin secretos en repositorio.  
- [ ] MinIO/S3A con endpoint correcto y **TLS** si aplica.  
- [ ] Pruebas con muestras “buenas” y “malas” (ver cuarentena).  
- [ ] Métricas/logs disponibles (conteos y tiempos).  

---

## 14) Preguntas frecuentes

- **¿Qué pasa si el archivo trae columnas extra y `additionalProperties=false`?**  
  En `strict`, se **avisa** por consola; el `select` final mantiene solo las del schema.

- **¿Cómo manejo fechas con diferentes formatos?**  
  Usa `standardization.casts` con `format_hint`. Si varían por archivo, considera una **etapa previa** de normalización.

- **¿Se puede escribir en otros formatos?**  
  Sí, define `output.silver.format` (ej. `delta`, `iceberg`, `hudi`) y configura el runtime (dependencias).

---

### Cierre

Este script implementa un **núcleo robusto y extensible** para pipelines declarativos en Spark. Con configuraciones claras (schema/calidad) y buenas prácticas (particionado, auditoría, overwrite dinámico), queda listo para **producción** tras resolver el **doble save** y asegurar **deduplicación determinista**.  

**Siguiente paso sugerido:** Integrar formato ACID (Delta/Hudi/Iceberg) y métricas de calidad en un **dashboard** operativo.
