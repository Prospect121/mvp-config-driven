# Despliegue mínimo en Azure para prueba de humo de prodi

Este módulo de Terraform aprovisiona los recursos mínimos de Azure y Databricks necesarios para ejecutar el cuaderno de prueba de humo `prodi` RAW→BRONZE→SILVER→GOLD. Implementa los siguientes componentes:

* (Opcional) Resource group de Azure y workspace de Databricks.
* Cuenta de almacenamiento ADLS Gen2 con los contenedores `raw`, `bronze`, `silver`, `gold`, `checkpoints`.
* Secret scope de Databricks `prodi-scope` con el nombre y la llave de la cuenta de almacenamiento.
* Activos del workspace de Databricks (cuaderno y job de ejecución inmediata).

## Prerrequisitos

* Suscripción de Azure con permisos para crear resource groups, cuentas de almacenamiento y workspaces de Databricks.
* Token de acceso personal (PAT) de Databricks con permisos para gestionar secretos, cuadernos y jobs.
* [Terraform 1.5+](https://developer.hashicorp.com/terraform/downloads).
* [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli) autenticada con `az login`.

## Variables de entrada

| Variable | Descripción |
| --- | --- |
| `resource_group_name` | Resource group donde se crearán los recursos. Se crea automáticamente cuando `create_databricks_workspace` es `true`. |
| `location` | Región de Azure para el despliegue (por ejemplo, `eastus2`). |
| `workspace_name` | Workspace de Databricks a crear o reutilizar. |
| `storage_account_name` | Nombre globalmente único para la cuenta ADLS Gen2 (3-24 caracteres alfanuméricos en minúscula). |
| `databricks_host` | URL del workspace de Databricks existente. Déjalo vacío cuando Terraform cree el workspace. |
| `databricks_token` | PAT de Databricks usado por el proveedor de Databricks. Expórtalo como `TF_VAR_databricks_token` para mantenerlo fuera del historial de la CLI. |
| `create_databricks_workspace` | Establécelo en `true` para crear un nuevo workspace de Databricks en el resource group indicado. |
| `tags` | Mapa opcional de tags de Azure. |

## Uso

### 1. Configura las variables de entorno

Exporta las variables antes de ejecutar Terraform. Para un workspace existente:

```bash
export TF_VAR_resource_group_name="rg-prodi"
export TF_VAR_location="eastus2"
export TF_VAR_workspace_name="dbw-prodi"
export TF_VAR_storage_account_name="prodilake123"
export TF_VAR_databricks_host="https://adb-xxxxxxxx.x.azuredatabricks.net"
export TF_VAR_databricks_token="<PAT>"
```

Al aprovisionar un nuevo workspace, omite `TF_VAR_databricks_host` y define:

```bash
export TF_VAR_create_databricks_workspace=true
```

El despliegue creará el resource group (si es necesario), el workspace, la cuenta de almacenamiento y los contenedores. Tras el primer `apply` exitoso, podrás obtener la URL del workspace desde los outputs de Terraform.

### 2. Inicializa y aplica

```bash
cd infra/azure/minimal
terraform init
terraform plan
terraform apply
```

### 3. Actualiza el secreto con la llave de la cuenta de almacenamiento

El módulo envía automáticamente la llave de la cuenta de almacenamiento al secret scope de Databricks. Si la llave rota o prefieres inyectarla manualmente:

1. Obtén la llave con Azure CLI:
   ```bash
   az storage account keys list \
     --resource-group "$TF_VAR_resource_group_name" \
     --account-name "$TF_VAR_storage_account_name" \
     --query "[0].value" -o tsv
   ```
2. Ejecuta nuevamente `terraform apply -refresh-only` para actualizar el secreto de Databricks sin recrear otros recursos.

### 4. Ejecuta la prueba de humo

1. Abre el workspace de Databricks en tu navegador (output `workspace_url`).
2. Navega a **Workflows → Jobs → prodi-smoke** y haz clic en **Run now**.
3. Supervisa la ejecución para confirmar que el cuaderno finaliza correctamente. La última celda imprime la cantidad de filas escritas en la capa GOLD.

### Limpieza

Al destruir el stack se eliminan el job, el cuaderno, el secret scope y los contenedores de la cuenta de almacenamiento. Ejecuta:

```bash
terraform destroy
```

> **Advertencia:** `terraform destroy` elimina de forma permanente la cuenta de almacenamiento y todos los datos guardados en los contenedores `raw`, `bronze`, `silver`, `gold` y `checkpoints`.

### Resolución de problemas

* **Errores de autenticación al acceder a ADLS** – Verifica que el secret scope `prodi-scope` contenga la llave de la cuenta de almacenamiento y que el clúster del job establezca `fs.azure.account.key.<account>.dfs.core.windows.net`.
* **Fallos al instalar paquetes** – Sube un wheel a `dbfs:/FileStore/prodi/wheels/` y vuelve a ejecutar el job; el cuaderno usará la alternativa automáticamente.
* **Faltan salidas** – Confirma que las plantillas YAML y la configuración del cuaderno apuntan a las URIs `abfss://` correctas y que los contenedores existen en la cuenta de almacenamiento.
