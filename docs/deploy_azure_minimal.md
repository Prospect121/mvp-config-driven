# Despliegue del entorno mínimo de prueba de humo de prodi en Azure

Esta guía describe cómo aprovisionar la huella mínima de Azure + Databricks necesaria para ejecutar la prueba de humo `prodi` RAW→BRONZE→SILVER→GOLD. La automatización se basa en la configuración de Terraform ubicada en `infra/azure/minimal` y en el cuaderno de Databricks `notebooks/azure/prodi_smoke_test.py`.

## Panorama de arquitectura

| Componente | Propósito |
| --- | --- |
| Cuenta de almacenamiento ADLS Gen2 | Aloja las zonas landing, refined y curated (`raw`, `bronze`, `silver`, `gold`) más los checkpoints de Delta. |
| Workspace de Databricks | Ejecuta el cuaderno y el job que orquestan la validación end-to-end. |
| Secret scope de Databricks | Almacena el nombre y la llave de la cuenta de almacenamiento, inyectados como variables de entorno en el clúster del job. |
| Job de Databricks | Ejecuta el cuaderno `/Shared/prodi_smoke_test` bajo demanda para validaciones rápidas. |

El cuaderno instala `datacore/prodi`, genera un dataset sintético, crea archivos YAML por capa y ejecuta `prodi run-layer` de forma secuencial.

## Prerrequisitos

* Suscripción de Azure con permisos para crear resource groups, cuentas de almacenamiento y workspaces de Databricks.
* Token de acceso personal (PAT) de Databricks con alcance en el workspace objetivo.
* Terraform 1.5 o superior.
* Azure CLI autenticada mediante `az login`.

## 1. Configura las variables de entorno

Define las variables de Terraform en el entorno para mantener los valores sensibles fuera del historial de la CLI.

```bash
export TF_VAR_resource_group_name="rg-prodi"
export TF_VAR_location="eastus2"
export TF_VAR_workspace_name="dbw-prodi"
export TF_VAR_storage_account_name="prodilake123"
export TF_VAR_databricks_token="<PAT>"
```

Si deseas que Terraform cree el workspace de Databricks, agrega:

```bash
export TF_VAR_create_databricks_workspace=true
```

Para workspaces existentes, proporciona también la URL del workspace:

```bash
export TF_VAR_databricks_host="https://adb-xxxxxxxx.x.azuredatabricks.net"
```

> **Sugerencia:** Usa `az storage account check-name --name <account>` para confirmar que el nombre de la cuenta de almacenamiento está disponible antes de ejecutar Terraform.

## 2. Despliega la infraestructura

```bash
cd infra/azure/minimal
terraform init
terraform plan
terraform apply
```

Revisa el plan para confirmar que se crearán el resource group, la cuenta de almacenamiento, los contenedores, el secret scope, el cuaderno y el job. Después de que `terraform apply` finalice, conserva los siguientes outputs:

* `workspace_url` – URL base del workspace de Databricks.
* `databricks_job_url` – Enlace directo al job de la prueba de humo.
* `storage_account_name` – Cuenta de ADLS Gen2 que respalda el data lake.
* `storage_containers` – Mapa con los contenedores de capas creados por Terraform.

## 3. Refresca la llave de la cuenta de almacenamiento (opcional)

El módulo inyecta la llave primaria de la cuenta de almacenamiento en el secret scope de Databricks. Cuando la llave rote, ejecuta un apply en modo refresh-only para sincronizar Terraform con el valor más reciente:

```bash
az storage account keys list \
  --resource-group "$TF_VAR_resource_group_name" \
  --account-name "$TF_VAR_storage_account_name" \
  --query "[0].value" -o tsv

terraform apply -refresh-only
```

## 4. Lanza la prueba de humo

1. Abre el workspace de Databricks usando el output `workspace_url`.
2. Navega a **Workflows → Jobs** y selecciona **prodi-smoke**.
3. Haz clic en **Run now**. El job lanzará un clúster con DBR 14.3 LTS y las variables de entorno necesarias preconfiguradas.
4. Supervisa la ejecución. El cuaderno imprime:
   * Logs de instalación de paquetes (`%pip` + alternativa con wheel).
   * URIs de contenedores descubiertas desde las variables de entorno.
   * Salida de `prodi validate` y `prodi run-layer` por capa.
   * Una vista previa de la tabla Delta curada y el resumen del conteo de filas.

Las ejecuciones exitosas muestran `Rows: <n>` y `Validated path: abfss://gold...` al final del cuaderno.

## 5. Verifica los resultados

Usa Databricks Data Explorer o Spark SQL para inspeccionar las tablas Delta. También puedes montar la cuenta de almacenamiento localmente usando URIs `abfss://` para confirmar que existen las siguientes rutas:

* `abfss://bronze@<account>.dfs.core.windows.net/tables/customers_raw/`
* `abfss://silver@<account>.dfs.core.windows.net/tables/customers/`
* `abfss://gold@<account>.dfs.core.windows.net/marts/customers/`

## Resolución de problemas

| Síntoma | Solución |
| --- | --- |
| `ModuleNotFoundError: No module named 'datacore'` | Sube un wheel a `dbfs:/FileStore/prodi/wheels/` y vuelve a ejecutar el job; el cuaderno utilizará la alternativa automáticamente. |
| `AnalysisException` con referencia a `fs.azure.account.key` | Confirma que el clúster del job expone las variables `AZURE_STORAGE_ACCOUNT_*` y que el secret scope contiene valores vigentes. |
| `Path does not exist` para `abfss://...` | Verifica los nombres de contenedor y los reemplazos del placeholder de cuenta en las plantillas YAML bajo `templates/azure`. |
| Errores de autenticación del proveedor de Databricks | Asegúrate de que `TF_VAR_databricks_token` contenga un PAT válido. Para service principals de Azure AD, configura el proveedor de Databricks en consecuencia antes de ejecutar Terraform. |

## Limpieza

Para eliminar todos los recursos aprovisionados (incluyendo la cuenta de almacenamiento y el cuaderno), ejecuta:

```bash
cd infra/azure/minimal
terraform destroy
```

> **Advertencia:** Este comando elimina de forma permanente todos los datos almacenados en los contenedores `raw`, `bronze`, `silver`, `gold` y `checkpoints`.
