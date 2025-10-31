# Validaciones de datos

Las validaciones se definen dentro de `validation.rules` (o en `transform.validation` para configuraciones heredadas). Cada regla genera métricas, puede marcar registros inválidos y opcionalmente enviarlos a cuarentena.

## Reglas soportadas
- `expect_not_null`: asegura que las columnas listadas no contengan nulos.
- `expect_unique` / `expect_column_values_to_be_unique`: valida unicidad sobre columnas o conjuntos de columnas.
- `range` (`expect_between` legado): `{column|col, min?, max?}` valida que el valor se encuentre en el rango.
- `regex`: `{column|col, pattern}` aplica expresiones regulares.
- `values_in_set` (`expect_set`): `{column|col, allowed: [...]}` limita los valores permitidos.
- `expect_email`, `expect_length`, `expect_foreign_key`: compatibles con versiones anteriores.

## Ejemplo YAML
```yaml
validation:
  rules:
    - check: expect_not_null
      columns: [order_id, order_date]
      severity: error
    - check: regex
      column: country_code
      pattern: "^[A-Z]{2}$"
      severity: warn
      threshold: 0.05   # tolera 5% de incumplimientos
    - check: values_in_set
      column: status
      allowed: [pending, shipped, delivered]
      on_fail: quarantine
  quarantine_sink:
    type: storage
    uri: abfs://silver/quarantine/orders/
```

## Rejects y métricas
- Registros inválidos se escriben en `<sink.uri>/_rejects/` con las columnas originales más `_reject_reason`.
- Las reglas con `on_fail: quarantine` se escriben adicionalmente en `validation.quarantine_sink` (si se configura) con `_quarantine_reason`.
- El motor devuelve métricas estructuradas `{input_rows, valid_rows, invalid_rows, rules: {...}}`, añade `quarantine_rows` y un `run_id`, y escribe un JSON por ejecución en `<sink.uri>/_metrics/<timestamp>.json`.

## Buenas prácticas
- Definir validaciones clave en capas `silver`/`gold` para asegurar contratos aguas arriba.
- Combinar `deduplicate` + `expect_unique` cuando existan merges incrementales.
- Usar `prodi run --dry-run` para revisar qué reglas aplicarán antes de ejecutar.
