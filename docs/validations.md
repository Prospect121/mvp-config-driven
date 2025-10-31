# Validaciones de datos

Las validaciones se definen dentro de `transform.validation` o en el bloque `validation` del dataset (compatibilidad hacia atrás). Cada regla genera métricas y puede producir registros rechazados.

## Reglas soportadas
- `expect_not_null`: asegura que las columnas listadas no contengan nulos.
- `expect_unique` / `expect_column_values_to_be_unique`: valida unicidad sobre columnas o conjuntos de columnas.
- `expect_between`: `{col, min, max}` valida que el valor se encuentre en el rango.
- `expect_regex`: `{col, pattern}` aplica expresiones regulares.
- `expect_email`: lista de columnas a validar con patrón RFC 5322 simplificado.
- `expect_length`: `{col, min?, max?}` controla la longitud de cadenas o arrays.
- `expect_set`: `{col, allowed: [...]}` limita los valores permitidos.
- `expect_foreign_key`: `{col, ref_table, ref_col}` valida referencias externas si se proporciona el DataFrame de referencia.
- `expect_domain`: alias legado para `expect_set`.

## Ejemplo YAML
```yaml
validation:
  expect_not_null: [order_id, order_date]
  expect_unique:
    - [order_id, order_date]
  expect_between:
    col: total_amount
    min: 0
  expect_regex:
    col: country_code
    pattern: "^[A-Z]{2}$"
  expect_email: [customer_email]
  expect_length:
    col: customer_name
    min: 3
  expect_set:
    col: status
    allowed: [pending, shipped, delivered]
```

## Rejects y métricas
- Registros inválidos se escriben en `<sink.uri>/_rejects/` con las columnas originales más `_reject_reason`.
- El motor devuelve métricas estructuradas `{input_rows, valid_rows, invalid_rows, reglas...}` y además escribe un JSON por ejecución en `<sink.uri>/_metrics/<timestamp>.json`.
- Las métricas agregan contadores por cada regla (`expect_not_null`, `expect_regex`, etc.).

## Buenas prácticas
- Definir validaciones clave en capas `silver`/`gold` para asegurar contratos aguas arriba.
- Combinar `deduplicate` + `expect_unique` cuando existan merges incrementales.
- Usar `prodi run --dry-run` para revisar qué reglas aplicarán antes de ejecutar.
