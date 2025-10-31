# Procesamiento incremental

El módulo `datacore.core.incremental` proporciona utilidades para ejecutar cargas incrementales mediante operaciones `append` o `merge`. El modo se define por dataset dentro del bloque `incremental`.

## Modo append
- Añade registros nuevos sin deduplicar.
- Compatible con cualquier sink soportado.
- En streaming se recomienda definir `watermark_column` para controlar la retención de estados.

## Modo merge
- Requiere claves (`keys`) y opcionalmente `order_by` para priorizar registros (por defecto `_ingestion_ts DESC`).
- Si el sink es Delta Lake se utiliza `MERGE INTO` nativo.
- Para otros formatos se ejecuta un merge genérico: se escribe un staging temporal, se deduplica por `keys`+`order_by` y se reemplaza la tabla destino de forma atómica.
- Para sinks JDBC el motor crea una tabla temporal y ejecuta un merge por etapas dentro de una transacción, respetando `isolationLevel`.

## Upserts JDBC
1. Se escribe el DataFrame en una tabla temporal con `batchsize` configurable.
2. Se ejecuta el `MERGE`/`UPSERT` mediante SQL específico del motor (`postgres`, `mysql`, `sqlserver/synapse`, `redshift`).
3. Se limpia la tabla temporal y se registran métricas.

## Watermarks y streaming
- `watermark_column` puede definirse en `source`, `incremental` o `streaming`; el motor la unifica.
- `streaming.trigger` acepta expresiones `processingTime=5 minutes`.
- Los checkpoints se almacenan en `platform.checkpoint_dir(layer, dataset, env)`.
- Para fuentes Kafka/Event Hubs se soporta `readStream` con parseo JSON/CSV y watermark.

## Métricas y rejects
- Cada ejecución produce métricas `{input_rows, valid_rows, invalid_rows, reglas...}` en `<sink.uri>/_metrics/<timestamp>.json`.
- Los registros que no superan las reglas de validación se escriben en `<sink.uri>/_rejects/` con columna `_reject_reason`.

Consulta `examples/` para pipelines con `append`, `merge` y streaming.
