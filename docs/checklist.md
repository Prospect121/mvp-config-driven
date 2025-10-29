# Checklist de criterios de aceptación

- [x] `prodi validate` y `prodi run` disponibles con soporte para raw/bronze/silver/gold.
- [x] Configuraciones YAML por entorno/capa/plataforma validadas con JSON Schema.
- [x] Arquitectura modular con plataformas Azure/AWS/GCP y conectores de storage/JDBC.
- [x] Pipelines de ejemplo raw→gold por nube documentados en `configs/` y `examples/`.
- [x] Empaquetado wheel listo vía `pyproject.toml`.
- [x] Guías de despliegue para Databricks, Glue y Dataproc en `docs/deploy/`.
- [x] Tests unitarios e integración (Spark local) incluidos.
- [x] CI con linting y pytest configurado (`.github/workflows/ci.yml`).
