## Objetivo
- Incorporar cifrado y descifrado tanto a nivel de archivos (PGP/SSE-KMS) como a nivel de registros/campos (AES-GCM/determinístico), manteniendo el modelo config-driven y la compatibilidad con batch/streaming.

## Puntos de Integración en el código
- Transformaciones antes de validar/escribir: `datacore/core/engine.py:117` (`_apply_transformations`), con `transform.udf` y `transform.ops` invocados desde `datacore/core/transforms.py:18` (`apply_registered`) y `datacore/core/transforms.py:27`/`datacore/core/ops.py:227` (`apply_ops`).
- Validación: `datacore/core/validation.py:273` (`apply_validation`) para asegurar que las columnas cifradas cumplan reglas (p.ej., no nulas, longitud).
- Escritura: `datacore/io/writers.py:43` (`_merge_write_options`) y `datacore/io/writers.py:75` (`write_batch`) para opciones de cifrado del conector (SSE-KMS, CMEK).
- Lectura: `datacore/io/readers.py:293` (`read_batch`) para aplicar descifrado de campos inmediatamente después de leer.
- Secretos/KMS: `datacore/utils/secrets.py:22` y `datacore/platforms/base.py:50` para resolver claves (`${SECRET:...}` / `secret://...`).

## Cifrado de Archivos
- **Server-side (recomendado por rendimiento):** habilitar SSE en `sink.options` según backend:
  - `s3`: `serverSideEncryption: aws:kms | AES256`, `sseKmsKeyId: ${SECRET:KMS_ARN}`.
  - `gcs`: `encryptionKey`/CMEK vía opciones Hadoop.
  - `abfs/fabric`: cifrado en reposo gestionado; exponer banderas si se requiere cliente.
- **Cliente (PGP):** añadir soporte `sink.encryption: {type: "pgp", recipient: "<key_id>", scope: "file"}` para post-proceso: escribir a ruta temporal y luego encriptar cada objeto con OpenPGP antes de mover al destino.

## Cifrado de Campos
- **Nueva op declarativa:** `encrypt_fields` y `decrypt_fields` en `datacore/core/ops.py`, consumidas por `apply_ops`.
  - `algo: aes-gcm` (confidencialidad), `algo: aes-siv` (determinístico para joins), `format: base64|binary`.
  - `key_ref: ${SECRET:...}` o `secret://...` resuelto por `PlatformBase`.
- **UDFs registradas:** alternativa rápida vía `transform.udf` usando `register(name, func)` (`datacore/core/transforms.py:14`).

## Gestión de Claves
- Envelope encryption: generar data-keys por dataset/micro-lote; cifrar data-key con KMS (referencia en metadatos del sink).
- Resolución de secretos: reusar `resolve_secret_reference` (`datacore/platforms/base.py:50`) y `resolve_secret` (`datacore/utils/secrets.py:22`).

## Lectura y Descifrado
- Con SSE-KMS: lectura transparente.
- Con PGP (cliente): descargar/decrypt a staging antes de `read_batch` o introducir `source.pre_read: {pgp: ...}` como hook.
- Con campos cifrados: aplicar `decrypt_fields` en `transform.ops` inmediatamente después de lectura.

## Configuración (YAML) propuesta
- **Archivo (S3 + SSE-KMS):**
  - `sink: {type: storage, backend: aws, format: parquet, uri: s3://bucket/path, options: {serverSideEncryption: aws:kms, sseKmsKeyId: ${SECRET:KMS_KEY_ARN}}}`
- **Archivo (PGP cliente):**
  - `sink: {type: storage, backend: aws, format: parquet, encryption: {type: pgp, recipient: ${SECRET:PGP_RECIPIENT}}, uri: s3://bucket/path}`
- **Campos:**
  - `transform: {ops: [{encrypt_fields: {cols: [email, ssn], algo: aes-gcm, key_ref: ${SECRET:APP_DATA_KEY}, format: base64}}]}`

## Observabilidad y Validación
- Métricas y rejects: mantener sin cifrar o cifrados con otra clave para auditoría.
- Validaciones: añadir reglas sobre columnas cifradas en `datacore/core/validation.py:273` (`apply_validation`).

## Rendimiento y compatibilidad
- Preferir SSE-KMS para archivos por costo/latencia.
- Para campos, evitar UDFs pesadas; evaluar `pandas UDF` vectorizadas o implementar UDF JVM si el rendimiento de PySpark no es suficiente.

## Entregables Técnicos
- Nuevas ops `encrypt_fields`/`decrypt_fields`.
- Soporte `sink.encryption` (PGP cliente) y documentación de `sink.options` para SSE-KMS/CMEK.
- Ejemplos YAML y pruebas unitarias/integra ción.

## Próximos pasos
- Confirmar alcance (S3/GCS/ABFS) y algoritmos preferidos.
- Implementar ops y opción `sink.encryption`.
- Añadir casos de prueba y ejemplos de configuración.
