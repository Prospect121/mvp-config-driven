# Procesamiento incremental

El módulo `datacore.core.incremental` proporciona utilidades para ejecutar cargas incrementales mediante operaciones `append` o `merge`.

## Modo append
- Añade registros nuevos sin deduplicar.
- Requiere `watermark_column` para streaming.

## Modo merge
- Realiza un `MERGE` sobre tablas Delta cuando está disponible.
- Alternativamente, implementa un `merge` genérico escribiendo datos temporales y realizando un reemplazo atómico.
- Requiere definir claves (`keys`).

## Watermarks
El motor calcula un watermark por dataset para evitar reprocesos en modo streaming. El valor se almacena en el directorio de checkpoints proporcionado por la plataforma.
