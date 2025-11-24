# Guía de migración desde el MVP anterior

El repositorio original no contenía código activo, por lo que la migración consiste en adoptar la nueva estructura `datacore`.

## Pasos
1. Instalar el paquete `datacore` y validar configuraciones YAML con `prodi validate`.
2. Migrar datasets existentes a los nuevos esquemas `project.yml` y `layers/*.yml`.
3. Configurar los parámetros específicos de la nube en `configs/platforms/<cloud>.yml`.
4. Ajustar jobs de orquestación (Databricks, Glue, Dataproc) para invocar `prodi run`.

No se requiere compatibilidad hacia atrás con scripts previos.
