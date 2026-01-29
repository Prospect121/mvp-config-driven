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
antes de las operaciones declarativas.

### Join simple (2 tablas)

Usa los **nombres de las fuentes** como alias en las expresiones `select`:

```yaml
transform:
  federate:
    strategy: join
    join:
      left: api
      right: crm
      on:
        - { left: email, right: email }
      join_type: left
      select:
        - { expr: "crm.id", as: customer_id }
        - { expr: "api.email", as: email }
        - { expr: "nvl(crm.segment,'UNKNOWN')", as: segment }
  ops:
    - normalize: { lower: [email] }
    - dedupe: { keys: [email], order_by: ["_ingestion_ts DESC"] }
```

### Joins encadenados (3+ tablas)

Para unir más de dos tablas, usa la sintaxis `chain`. Cada paso puede referenciar fuentes originales o resultados intermedios usando el campo `as`:

```yaml
transform:
  federate:
    strategy: join
    join:
      chain:
        # Paso 1: unir orders con customers
        - left: orders
          right: customers
          on:
            - { left: customer_id, right: id }
          join_type: inner
          as: orders_customers # Nombre del resultado intermedio

        # Paso 2: unir el resultado anterior con products
        - left: orders_customers
          right: products
          on:
            - { left: product_id, right: id }
          join_type: left
          # Sin 'as', es el resultado final

      # Select final aplicado al último join
      select:
        - { expr: "orders.order_id", as: order_id }
        - { expr: "customers.name", as: customer_name }
        - { expr: "products.name", as: product_name }
        - { expr: "orders.quantity * products.price", as: total }
```

**Importante:**

- Los nombres en `left`/`right` de la configuración son **nombres de fuentes**, no aliases genéricos
- En expresiones `select`, usa `nombre_fuente.columna` (ej: `orders.order_id`, `customers.name`)
- El campo `as` en cada paso del chain define el nombre del resultado intermedio para usarlo en pasos siguientes
- El `select` final del bloque `join` se aplica al resultado del último paso

Si no se define `federate`, las fuentes se combinan mediante `unionByName` y opcionalmente `merge_strategy` para resolver
conflictos por `keys`.
