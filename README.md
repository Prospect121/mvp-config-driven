# Multicloud DataCore

`datacore` es un paquete Python diseñado para ejecutar pipelines ETL/ELT configurables en múltiples nubes utilizando Apache Spark. El proyecto replantea el MVP original con una arquitectura modular, portable y enfocada en configuraciones YAML por entorno, capa y plataforma.

## Índice
- [Quickstart](#quickstart)
- [Arquitectura](docs/architecture.md)
- [Configuración](docs/configuration.md)
- [Conectores](docs/connectors.md)
- [Validaciones](docs/validations.md)
- [Incremental](docs/incremental.md)
- **[Instalación](docs/installation.md)** ← Nuevo
- **[Casos de Uso](docs/use_cases.md)** ← Nuevo
- **[Solución de Problemas](docs/troubleshooting.md)** ← Nuevo
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

## Microsoft Fabric Lakehouse

Para leer y escribir tablas administradas de Fabric sin generar carpetas "Unidentified", utiliza el backend `fabric` junto con una URI `lakehouse://<lakehouse>/tables/<esquema>/<tabla>` y el motor resolverá automáticamente el catálogo de Fabric (`saveAsTable`/`spark.table`).

```yaml
source:
  type: storage
  backend: fabric
  format: delta
  uri: "lakehouse://contoso-lh/tables/dbo/customers_raw"

sink:
  type: storage
  backend: fabric
  format: delta
  uri: "lakehouse://contoso-lh/tables/dbo/customers_curated"
  mode: overwrite
```

Para trabajar con archivos crudos en `Files/`, también puedes usar el esquema `lakehouse://`. El motor los convertirá a la ruta física del Lakehouse adjunto antes de llamar a `.load`/`.save`:

```yaml
sink:
  type: storage
  backend: fabric
  format: delta
  uri: "lakehouse://contoso-lh/files/raw/sales/customers_delta"
  mode: append
```

> ⚠️ Si apuntas explícitamente a `https://onelake.dfs.fabric.microsoft.com/.../Tables/...` se realizará una escritura estilo archivos, lo que puede producir carpetas "Unidentified/Sin identificar" en Fabric. Usa `lakehouse://.../tables/...` para tablas administradas.
