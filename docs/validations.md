# Validaciones de datos

Las validaciones se definen en el bloque `transform.validation` de cada dataset.

## Reglas soportadas
- `expect_not_null`: asegura que las columnas listadas no contengan nulos.
- `expect_unique`: valida unicidad (opcional) sobre columnas.
- `expect_domain`: verifica que los valores pertenezcan a un conjunto permitido.

Durante la ejecución, `datacore.core.validation` aplica las reglas y genera métricas de registros válidos e inválidos. Los registros rechazados se escriben en `_rejects` según configuración.

## Contratos
Los contratos se describen en YAML y se validan con `layer.schema.json`. Se pueden combinar múltiples reglas por dataset.
