# Multicloud DataCore

`datacore` es un paquete Python diseñado para ejecutar pipelines ETL/ELT configurables en múltiples nubes utilizando Apache Spark. El proyecto replantea el MVP original con una arquitectura modular, portable y enfocada en configuraciones YAML por entorno, capa y plataforma.

## Índice
- [Quickstart](#quickstart)
- [Arquitectura](docs/architecture.md)
- [Configuración](docs/configuration.md)
- [Conectores](docs/connectors.md)
- [Validaciones](docs/validations.md)
- [Incremental](docs/incremental.md)
- [Guías de despliegue](#guías-de-despliegue)
- [Documentación adicional](#documentación-adicional)

## Quickstart

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .

# Validar configuración
prodi validate --config configs/envs/dev/project.yml

# Ejecutar capa bronze en entorno local
dataenv=dev layer=bronze
prodi run --layer bronze --config configs/envs/dev/layers/bronze.yml
```

## Guías de despliegue
- [Azure Databricks](docs/deploy/azure_databricks.md)
- [AWS Glue](docs/deploy/aws_glue.md)
- [GCP Dataproc](docs/deploy/gcp_dataproc.md)

## Documentación adicional
Toda la documentación vive en `docs/`. Consulta `docs/architecture.md` para comprender la nueva arquitectura hexagonal y los puertos/adaptadores implementados.
