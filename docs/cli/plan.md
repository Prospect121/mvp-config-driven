# Contrato JSON de `prodi plan` y `prodi run --dry-run`

Ambos comandos exponen un contrato JSON estable que se valida contra `datacore/config/schemas/plan.schema.json` para evitar rupturas involuntarias.

## Estructura principal

```json
{
  "run_id": "<uuid>",
  "datasets": [
    {
      "name": "orders_silver",
      "layer": "silver",
      "status": "planned",
      "run_id": "<uuid>",
      "source_plan": {
        "sources": [
          {"type": "storage", "uri": "abfss://landing/orders/", "format": "csv"}
        ],
        "merge_strategy": {"keys": ["order_id"], "prefer": "newest"}
      },
      "transform_plan": {
        "sql": [],
        "ops": [
          {"rename": {"orderId": "order_id"}},
          {"cast": {"amount": "double"}}
        ],
        "udf": [],
        "add_ingestion_ts": true
      },
      "validation_plan": {
        "rules": [
          {"check": "expect_not_null", "columns": ["order_id"], "severity": "error"}
        ]
      },
      "incremental_plan": {
        "mode": "merge",
        "keys": ["order_id"],
        "order_by": ["_ingestion_ts DESC"],
        "watermark": {"column": "order_date", "delay_threshold": "1 day"}
      },
      "streaming_plan": {
        "enabled": false,
        "trigger": null,
        "checkpoint_location": null,
        "watermark": null
      },
      "sink_plan": {
        "type": "storage",
        "format": "parquet",
        "uri": "abfss://silver/orders/",
        "partition_by": ["order_date"],
        "merge_schema": true,
        "compression": "snappy"
      },
      "issues": []
    }
  ]
}
```

Cada dataset puede incluir propiedades adicionales (`metrics` en ejecuciones reales) pero el núcleo anterior se mantiene estable para CI y tooling externo.

## Validación en pruebas

`tests/unit/test_engine_plan.py` valida el contrato contra `plan.schema.json`. Cualquier cambio estructural debe acompañarse de una actualización explícita del esquema y de la documentación.
