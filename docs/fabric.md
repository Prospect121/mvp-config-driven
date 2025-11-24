# Provider opcional Microsoft Fabric

El soporte para Microsoft Fabric es un plugin opcional del proyecto. El núcleo multicloud continúa funcionando sin dependencias de Azure, y solo es necesario instalar los extras cuando se desee operar sobre Fabric.

## Instalación

```bash
pip install .[fabric]
```

El extra instala las bibliotecas necesarias (`azure-identity`, `azure-keyvault-secrets`, `delta-spark`, `azure-kusto-*`, `pyodbc`, entre otras) para interactuar con los servicios de Fabric. El resto del proyecto continúa funcionando sin Fabric instalado.

## Autenticación

El provider utiliza `DefaultAzureCredential`. En entornos de Microsoft Fabric, se recomienda configurar una identidad administrada o un Service Principal con permisos sobre el workspace. Las variables habituales son:

- `AZURE_CLIENT_ID`
- `AZURE_TENANT_ID`
- `AZURE_CLIENT_SECRET`

Si se ejecuta dentro de un entorno gestionado (por ejemplo, un Environment de Fabric), basta con asignar la identidad adecuada.

## Configuración YAML

La sección `providers.fabric` es opcional y no modifica las claves existentes. Puede resolverse dinámicamente con variables de entorno permitidas (`${FABRIC_WORKSPACE_ID}`, `${FABRIC_LAKEHOUSE_ID}`, etc.). Un ejemplo mínimo:

```yaml
providers:
  fabric:
    workspace_id: "00000000-0000-0000-0000-000000000000"
    environment_id: "env-01"
    lakehouse_id: "${FABRIC_LAKEHOUSE_ID}"

Los placeholders están restringidos a una lista blanca y deben estar definidos como variables de entorno antes de ejecutar la CLI. Los valores se registran sin exponer el secreto.
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

Al final de cada `run`, la CLI emite un resumen JSON con métricas agregadas (datasets procesados, filas de entrada/salida, bytes escritos y duración total) junto con el `run_id` y `created_at`. Estos datos también se incluyen en el plan generado.

> Nota: para pruebas locales sin credenciales de Azure puede habilitarse el modo offline exportando `FABRIC_HTTP_OFFLINE=1`; en ese caso las llamadas HTTP se simulan y no se envían a Fabric.

## Lakehouse Delta

El módulo `datacore.providers.fabric.lakehouse` utiliza `delta-spark` para leer y escribir tablas Delta en OneLake. Ofrece idempotencia y mantenimiento opcional (`OPTIMIZE`, `Z-ORDER`, `VACUUM`) mediante `options` en los sinks. Puede emplearse en pipelines existentes sin cambios en AWS, GCP o Databricks; en esos entornos el provider simplemente no se carga.

Los secretos referenciados como `secret://kv/<vault>/<secret>` se resuelven automáticamente mediante Azure Key Vault (`DefaultAzureCredential`). Para otros backends (`secret://sm/`, `secret://gcp/`) se delega en AWS Secrets Manager o Google Secret Manager respectivamente, devolviendo errores claros si las dependencias no están instaladas.

### Tablas administradas

Cuando un `source` o `sink` utiliza el backend `fabric` y la ruta `lakehouse://<lakehouse>/tables/<esquema>/<tabla>`, el motor resuelve automáticamente el catálogo de Fabric. Las escrituras se realizan con `saveAsTable` y las lecturas con `spark.table`, evitando el acceso directo a `Tables/` y las carpetas "Unidentified/Sin identificar".

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
  uri: "lakehouse://contoso-lh/tables/silver/customers_enriched"
  mode: overwrite
```

Si el esquema no se especifica en la URI se utilizará `dbo` por defecto o el valor definido en la variable de entorno `FABRIC_DEFAULT_SCHEMA`.

### Archivos en `Files`

Para trabajar con archivos crudos basta con apuntar a `lakehouse://<lakehouse>/files/<ruta>`. El conector traduce la URI al montaje del Lakehouse adjunto antes de invocar `.load`/`.save`.

```yaml
source:
  type: storage
  backend: fabric
  format: csv
  uri: "lakehouse://contoso-lh/files/raw/customers/customers.csv"
  options:
    header: true

sink:
  type: storage
  backend: fabric
  format: delta
  uri: "lakehouse://contoso-lh/files/staging/customers_delta"
  mode: append
```

> ⚠️ Las rutas `https://onelake.dfs.fabric.microsoft.com/.../Tables/...` se tratan como paths físicos. Usarlas puede volver a generar carpetas "Unidentified" en Fabric, por lo que se recomienda migrar a `lakehouse://.../tables/...`.

## Warehouse y SQL Endpoint

El módulo `datacore.providers.fabric.warehouse` incluye utilidades para crear o actualizar vistas (`CREATE OR ALTER VIEW` apuntando a tablas Delta) y ejecutar operaciones `COPY INTO` con opciones de tolerancia (`MAXERRORS`, patrones de archivos, particiones). Las peticiones se envían al endpoint REST del Warehouse y registran acción y destino en los logs.

## KQL/Eventhouse

`datacore.providers.fabric.kql` expone `kql_read` y `kql_ingest`, que encapsulan `azure-kusto-data` y `azure-kusto-ingest` con autenticación AAD basada en `DefaultAzureCredential`. Ambos métodos son seguros para entornos sin las dependencias: generan errores guiados que recuerdan instalar el extra `fabric`.

## Observabilidad y CI

Los logs se emiten en formato JSON con contexto (`run_id`, dataset, layer, provider). Las métricas de observabilidad se agregan y se imprimen como resumen al finalizar un `run`. El pipeline de CI genera un wheel (`python -m build`), lo instala y publica el plan resultante de los ejemplos como artefacto por cada perfil (`base` y `fabric`).

## Limitaciones actuales

- El soporte de Mirroring continúa como stub; su implementación se planificará en iteraciones posteriores.
- Las pruebas e2e dependen de credenciales reales y están marcadas como opcionales (`pytest -m fabric_e2e`).
