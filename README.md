# Multicloud DataCore

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![PySpark 3.4+](https://img.shields.io/badge/pyspark-3.4+-orange.svg)](https://spark.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

`datacore` es un framework ETL/ELT basado en Python que permite ejecutar pipelines de datos configurables mediante YAML en múltiples plataformas cloud (Azure Databricks, AWS Glue, GCP Dataproc, Microsoft Fabric).

## Características Principales

- **Configuración declarativa** - Pipelines definidos en YAML, sin código
- **Multi-cloud** - Azure, AWS, GCP, Fabric, Local
- **Arquitectura hexagonal** - Núcleo desacoplado de integraciones
- **Validación de datos** - Reglas configurables con métricas y cuarentena
- **Cargas incrementales** - Append, Merge, CDC
- **Federación de fuentes** - Join/Union de múltiples orígenes

## Índice

- [Quickstart](#quickstart)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Conectores Disponibles](#conectores-disponibles)
- [Documentación](#documentación)
- [Ejemplos](#ejemplos)

## Quickstart

### Instalación

```bash
# Crear entorno virtual
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# o en Windows: .venv\Scripts\activate

# Instalar el paquete
pip install -e .

# Con dependencias de Fabric
pip install -e ".[fabric]"

# Con dependencias de desarrollo
pip install -e ".[dev]"
```

### Uso básico

```bash
# Validar configuración
prodi validate --config examples/azure/orders_pipeline.yaml

# Ver plan de ejecución (dry-run)
prodi run --layer bronze --config examples/local_test_pipeline.yaml --platform local --dry-run

# Ejecutar capa
prodi run --layer bronze --config examples/local_test_pipeline.yaml --platform local

# Generar plan completo del proyecto
prodi plan --config examples/example-infra.yml
```

## Estructura del Proyecto

```
mvp-config-driven/
├── datacore/
│   ├── cli/                  # CLI (comando prodi)
│   ├── config/               # Validación de schemas JSON
│   ├── connectors/
│   │   ├── api/              # REST, GraphQL
│   │   ├── db/               # JDBC
│   │   ├── storage/          # S3, ABFS, GCS, Local, Fabric
│   │   └── http.py           # HTTP genérico
│   ├── core/                 # Motor de ejecución
│   │   ├── engine.py         # Orquestador principal
│   │   ├── validation.py     # Sistema de validaciones
│   │   ├── ops.py            # Operaciones declarativas
│   │   ├── federation.py     # Join/Union de fuentes
│   │   ├── cdc.py            # Change Data Capture
│   │   └── incremental.py    # Cargas incrementales
│   ├── io/                   # Readers y Writers
│   ├── platforms/            # Azure, AWS, GCP, Fabric, Local
│   ├── providers/            # Extensiones (Fabric)
│   └── utils/                # Logging, paths, secrets
├── docs/                     # Documentación completa
├── examples/                 # Ejemplos funcionales
├── tests/                    # Suite de pruebas
└── pyproject.toml            # Configuración del proyecto
```

## Conectores Disponibles

### Storage
| Conector | Tipo | Formatos |
|----------|------|----------|
| **S3** | `storage` | csv, json, parquet, avro, orc, delta |
| **ABFS** (Azure Data Lake) | `storage` | csv, json, parquet, avro, orc, delta |
| **GCS** | `storage` | csv, json, parquet, avro, orc, delta |
| **Local/MinIO** | `storage` | csv, json, parquet, avro, orc |
| **Fabric Lakehouse** | `storage` | delta (tablas administradas) |

### APIs
| Conector | Tipo | Características |
|----------|------|-----------------|
| **REST** | `api_rest` | Paginación (cursor/offset/page), auth bearer/basic |
| **GraphQL** | `api_graphql` | Queries parametrizadas |
| **HTTP Endpoint** | `endpoint` | Reintentos, flatten automático |

### Bases de Datos
| Conector | Tipo | Engines |
|----------|------|---------|
| **JDBC** | `jdbc` | PostgreSQL, SQL Server, MySQL, Oracle, Redshift |
| **Warehouse** | `warehouse` | Synapse, BigQuery, Redshift |
| **NoSQL** | `nosql` | CosmosDB, DynamoDB |

### Streaming
| Conector | Tipo | Características |
|----------|------|-----------------|
| **Kafka** | `kafka` | readStream/writeStream, watermark |
| **Event Hubs** | `event_hubs` | readStream/writeStream, watermark |

## Documentación

| Documento | Descripción |
|-----------|-------------|
| [Arquitectura](docs/architecture.md) | Diagramas, flujos, componentes |
| [Configuración](docs/configuration.md) | Estructura YAML, variables, secretos |
| [Conectores](docs/connectors.md) | Referencia de todos los conectores |
| [Validaciones](docs/validations.md) | Reglas de calidad, métricas |
| [Incremental](docs/incremental.md) | CDC, append, merge |
| [Casos de Uso](docs/use_cases.md) | Ejemplos prácticos |
| [Instalación](docs/installation.md) | Guía de instalación |
| [Troubleshooting](docs/troubleshooting.md) | Solución de problemas |

### Guías de Despliegue
- [Azure Databricks](docs/deploy/azure_databricks.md)
- [AWS Glue](docs/deploy/aws_glue.md)
- [GCP Dataproc](docs/deploy/gcp_dataproc.md)
- [Microsoft Fabric](docs/fabric.md)

## Ejemplos

El directorio `examples/` contiene configuraciones funcionales:

```
examples/
├── azure/              # Azure Databricks + ABFS
├── aws/                # AWS Glue + S3/Redshift
├── gcp/                # GCP Dataproc + GCS/BigQuery
├── fabric/             # Microsoft Fabric Lakehouse
├── cdc/                # Change Data Capture
├── streaming/          # Kafka, Event Hubs
├── federation/         # Multi-source join
├── local_test_pipeline.yaml  # Pipeline local de prueba
└── data/               # Datos de prueba (CSV)
```

### Ejecutar ejemplo local

```bash
# Validar
prodi validate --config examples/local_test_pipeline.yaml

# Ejecutar cada capa
prodi run --layer raw --config examples/local_test_pipeline.yaml --platform local
prodi run --layer bronze --config examples/local_test_pipeline.yaml --platform local
prodi run --layer silver --config examples/local_test_pipeline.yaml --platform local
prodi run --layer gold --config examples/local_test_pipeline.yaml --platform local
```

## Microsoft Fabric Lakehouse

Para leer/escribir tablas administradas de Fabric:

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

> **Nota**: Usa `lakehouse://` para tablas administradas. Las URIs explícitas a `https://onelake.dfs.fabric.microsoft.com/` pueden producir carpetas "Unidentified".

## Tests

```bash
# Instalar dependencias de desarrollo
pip install -e ".[dev]"

# Ejecutar tests unitarios
pytest tests/unit -v

# Con cobertura
pytest tests/unit --cov=datacore --cov-report=html
```

## Licencia

MIT License - Ver [LICENSE](LICENSE) para más detalles.
