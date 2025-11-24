# Arquitectura Datacore

La solución sigue el patrón hexagonal (puertos y adaptadores) para mantener el núcleo desacoplado de las integraciones con cada
proveedor cloud.

```mermaid
graph TD
    CLI[CLI prodi] --> Engine
    Config[Config YAML + JSON Schema] --> Engine
    Engine[Core Engine\\n(raw/bronze/silver/gold)] --> Validation
    Engine --> Incremental
    Engine --> Transforms
    Engine --> Readers
    Engine --> Writers
    Readers --> StorageConnectors
    Readers --> APIConnectors
    Readers --> DBConnectors
    Readers --> StreamingConnectors
    Writers --> StorageConnectors
    Writers --> DWConnectors
    Writers --> NoSQLConnectors
    Writers --> StreamingSinks
    StorageConnectors --> Platforms
    APIConnectors --> Platforms
    DBConnectors --> Platforms
    NoSQLConnectors --> Platforms
    StreamingConnectors --> Platforms
    Platforms --> SparkSession
```

## Componentes principales
- **CLI (`datacore.cli`)**: expone comandos `prodi validate|run|plan` con soporte para `--dry-run` y `--fail-fast`.
- **Core (`datacore.core`)**: motor de ejecución por capas, registro de transformaciones, validaciones, incremental y métrica.
- **Connectores (`datacore.connectors`)**: adaptadores para almacenamiento, APIs, bases de datos, NoSQL y streaming.
- **Plataformas (`datacore.platforms`)**: inicialización de Spark, secretos y servicios auxiliares según nube.
- **IO (`datacore.io`)**: lectura/escritura batch y streaming con inferencia de esquemas cacheada.

## Flujo general
1. La CLI carga un archivo YAML y lo valida contra los esquemas JSON.
2. El motor determina las capas a ejecutar y construye un plan declarativo con las transformaciones y validaciones.
3. Cada dataset construye un DAG declarativo `sources → federate → ops → validations → incremental → sink`. Cada `source`
   queda materializado como vista temporal `_src_<id>` y puede provenir de storage, JDBC, endpoints REST/GraphQL, Kafka/Event
   Hubs, NoSQL o shortcuts declarados en `.prodi/shortcuts.yml`.
4. El bloque `transform.federate` habilita `join`/`union` multi-fuente sin ingestas redundantes y respeta `allow_missing_columns`
   y proyecciones selectivas.
5. Los conectores traducen los parámetros a llamadas de Spark o APIs externas, aplicando `pushdown` seguro, cache/TTL declarativo
   y particiones físicas cuando corresponde.
6. Las reglas de validación, normalización, cuarentena y los contratos incrementales (incluyendo CDC por timestamp o columna de
   versión) se aplican de forma uniforme sin importar la nube.
7. Se generan métricas por dataset y, en caso de errores, los registros rechazados se escriben en `_rejects` junto con métricas
   JSON por regla.

## Esquema e inferencia
- Los formatos soportados incluyen `csv`, `json`, `parquet`, `avro` y `orc`, con opciones específicas por formato.
- La inferencia de esquema es opcional; cuando está habilitada se cachea por dataset en el checkpoint de plataforma para evitar
  recalcular tipos en ejecuciones futuras.

## Transformaciones declarativas
- El motor registra operaciones puras en `datacore.core.ops` (renombrar, castear, limpiar whitespace, normalizar fechas,
  deduplicar, explotar arrays, flatten JSON, etc.).
- Los pipelines pueden combinar `transform.sql`, `transform.udf` y `transform.ops`, manteniendo compatibilidad hacia atrás.
- Se añade `transform.add_ingestion_ts` (true por defecto) y se soporta `merge_strategy` para múltiples fuentes.
- **Breaking change**: el bloque `transform.validation` fue retirado; las reglas deben declararse en `dataset.validation`.

## Incremental, CDC y streaming
- `incremental.mode` soporta `full`, `append` y `merge` genérico para cualquier formato, aprovechando Delta Lake cuando está disponible.
- Las fuentes JDBC con bloque `source.cdc` realizan lecturas incrementales por timestamp o columna de versión y persisten el watermark en el checkpoint del dataset.
- Para sinks JDBC el motor realiza upserts por etapas (tablas temporales + transacciones) cuando se definen `keys`.
- Las cargas streaming manejan `watermark` (`column` + `delay_threshold`), triggers configurables y checkpoints por dataset.

## Observabilidad
- El motor genera métricas JSON en `<sink>/_metrics/<run_id>.json` y archivos `_rejects` con motivos.
- `prodi plan` y `prodi run --dry-run` devuelven un contrato JSON estable con `{run_id, datasets: [...]}` incluyendo readers, transformaciones, incremental, streaming y sinks.
- El flag `--fail-fast` permite abortar al primer error manteniendo logs contextualizados por capa/dataset.
