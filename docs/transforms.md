# Transformaciones declarativas y federación

El motor ejecuta las transformaciones en un orden determinista para garantizar resultados reproducibles:

1. `exclude`
2. `rename`
3. `cast`
4. `normalize`
5. `filter`
6. `dedupe`
7. `flatten`
8. `explode`
9. `sql`

Cada operación se agrupa por tipo y se ejecuta respetando alias históricos (`drop_columns`, `deduplicate`, `flatten_json`, etc.).
Las sentencias SQL declaradas en `transform.sql` o dentro de operaciones `sql` se aplican al final sobre una vista temporal.

## Federate

Cuando se definen múltiples fuentes (`source` como lista) el motor crea vistas `_src_<id>` y aplica el bloque `transform.federate`
antes de las operaciones declarativas. Ejemplo de unión entre API y JDBC:

```yaml
transform:
  federate:
    strategy: join
    join:
      left: api
      right: crm
      on:
        - {left: email, right: email}
      join_type: left
      select:
        - {expr: "crm.id", as: customer_id}
        - {expr: "api.email", as: email}
        - {expr: "nvl(crm.segment,'UNKNOWN')", as: segment}
  ops:
    - normalize: {lower: [email]}
    - dedupe: {keys: [email], order_by: ["_ingestion_ts DESC"]}
```

Si no se define `federate`, las fuentes se combinan mediante `unionByName` y opcionalmente `merge_strategy` para resolver
conflictos por `keys`.
