# Reporte de limpieza

Se eliminaron los ejemplos de configuración obsoletos que fueron reemplazados por las plantillas de Azure en `templates/azure`:

- `cfg/raw/example.yml`
- `cfg/bronze/example.yml`
- `cfg/silver/example.yml`
- `cfg/gold/example.yml`
- `cfg/pipelines/example.yml`

Estos archivos hacían referencia a rutas locales y supuestos on-premises incompatibles con el despliegue en Azure Databricks. Las nuevas plantillas específicas por entorno se encuentran ahora en `templates/azure/*.yml`.
