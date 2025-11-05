# Provider opcional Microsoft Fabric

El soporte para Microsoft Fabric es un plugin opcional del proyecto. El núcleo multicloud continúa funcionando sin dependencias de Azure, y solo es necesario instalar los extras cuando se desee operar sobre Fabric.

## Instalación

```bash
pip install .[fabric]
```

Este extra instala las bibliotecas necesarias (`azure-identity`, `delta-spark`, `azure-kusto-*`, `pyodbc`, entre otras) para interactuar con los servicios de Fabric.

## Autenticación

El provider utiliza `DefaultAzureCredential`. En entornos de Microsoft Fabric, se recomienda configurar una identidad administrada o un Service Principal con permisos sobre el workspace. Las variables habituales son:

- `AZURE_CLIENT_ID`
- `AZURE_TENANT_ID`
- `AZURE_CLIENT_SECRET`

Si se ejecuta dentro de un entorno gestionado (por ejemplo, un Environment de Fabric), basta con asignar la identidad adecuada.

## Configuración YAML

La sección `providers.fabric` es opcional y no modifica las claves existentes. Un ejemplo mínimo:

```yaml
providers:
  fabric:
    workspace_id: "00000000-0000-0000-0000-000000000000"
    environment_id: "env-01"
    lakehouse_id: "lk-01"
```

Para datasets que utilicen shortcuts:

```yaml
source:
  type: shortcut
  name: ventas_onelake
  target: "abfss://contoso@onelake.dfs.fabric.microsoft.com/fabric"
  subpath: "/Ventas/"
  labels: ["bronze", "externo"]
```

## Uso de la CLI

El CLI mantiene la compatibilidad con todos los providers. Para habilitar la provisión de recursos Fabric durante un `plan` o un `run`, utilice `--provision` o defina `orchestration.fabric.create_items` dentro del dataset.

```bash
python -m datacore.cli plan --config examples/fabric/bronze_shortcut.yaml --provision
```

Cuando se ejecuta con un provider Fabric disponible, el plan incluirá un bloque `fabric.items` con los artefactos creados o asegurados (shortcuts, Spark Job Definitions, Data Pipelines, etc.).

## Lakehouse Delta

El módulo `datacore.providers.fabric.lakehouse` utiliza `delta-spark` para leer y escribir tablas Delta en OneLake. Puede emplearse en pipelines existentes sin cambios en AWS, GCP o Databricks; en esos entornos el provider simplemente no se carga.

## Limitaciones actuales

- Las operaciones de Warehouse, KQL/Eventhouse y Mirroring se exponen como stubs para iteraciones futuras.
- Las pruebas e2e dependen de credenciales reales y están marcadas como opcionales (`pytest -m fabric_e2e`).

## Roadmap

- Implementar vistas y cargas incrementales sobre Fabric Warehouse.
- Integración con Eventhouse/KQL para escenarios de streaming.
- Orquestación avanzada (Data Activator, pipelines compuestas) y gestión de mirroring como CDC.
