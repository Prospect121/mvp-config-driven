# Auditoría de Recursos Azure - Pre-Eliminación

## Fecha de Auditoría
**Fecha:** $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")

## Resumen Ejecutivo
Esta auditoría documenta todos los recursos Azure existentes antes de proceder con la optimización y reorganización del entorno.

## Grupos de Recursos Identificados

### 1. Grupo de Recursos Antiguo: `mvp-data-pipeline-dev-rg`
**Ubicación:** West US 2
**Estado:** Succeeded

#### Recursos en el Grupo Antiguo (9 recursos):
1. **Action Group**
   - Nombre: `mvp-data-pipeline-dev-ag`
   - Tipo: `Microsoft.Insights/actiongroups`
   - Ubicación: Global

2. **Event Hub Namespace**
   - Nombre: `mvp-data-pipeline-dev-eh`
   - Tipo: `Microsoft.EventHub/namespaces`
   - Ubicación: West US 2

3. **Data Factory**
   - Nombre: `mvp-data-pipeline-dev-df`
   - Tipo: `Microsoft.DataFactory/factories`
   - Ubicación: West US 2

4. **SQL Server**
   - Nombre: `mvp-data-pipeline-dev-sql-v2`
   - Tipo: `Microsoft.Sql/servers`
   - Ubicación: West US 2

5. **Log Analytics Workspace**
   - Nombre: `mvp-data-pipeline-dev-law`
   - Tipo: `Microsoft.OperationalInsights/workspaces`
   - Ubicación: West US 2

6. **SQL Database - Master**
   - Nombre: `mvp-data-pipeline-dev-sql-v2/master`
   - Tipo: `Microsoft.Sql/servers/databases`
   - Ubicación: West US 2

7. **SQL Database - Principal**
   - Nombre: `mvp-data-pipeline-dev-sql-v2/mvp-data-pipeline-dev-db`
   - Tipo: `Microsoft.Sql/servers/databases`
   - Ubicación: West US 2

8. **Key Vault**
   - Nombre: `mvp-dev-kv-v3`
   - Tipo: `Microsoft.KeyVault/vaults`
   - Ubicación: West US 2

9. **Storage Account**
   - Nombre: `mvpdevsa`
   - Tipo: `Microsoft.Storage/storageAccounts`
   - Ubicación: West US 2

### 2. Grupo de Recursos Nuevo: `rg-mvp-data-pipeline-dev-2024`
**Ubicación:** West US 2
**Estado:** Succeeded

#### Recursos en el Grupo Nuevo (12 recursos):
1. **Action Group**
   - Nombre: `mvp-data-pipeline-dev-ag`
   - Tipo: `Microsoft.Insights/actiongroups`
   - Ubicación: Global

2. **Data Factory**
   - Nombre: `mvp-data-pipeline-dev-df-2024`
   - Tipo: `Microsoft.DataFactory/factories`
   - Ubicación: West US 2

3. **Log Analytics Workspace**
   - Nombre: `mvp-data-pipeline-dev-law`
   - Tipo: `Microsoft.OperationalInsights/workspaces`
   - Ubicación: West US 2

4. **Event Hub Namespace**
   - Nombre: `mvp-data-pipeline-dev-eh-2024`
   - Tipo: `Microsoft.EventHub/namespaces`
   - Ubicación: West US 2

5. **Key Vault**
   - Nombre: `mvp-dev-kv-2024`
   - Tipo: `Microsoft.KeyVault/vaults`
   - Ubicación: West US 2

6. **SQL Server**
   - Nombre: `mvp-data-pipeline-dev-sql-2024`
   - Tipo: `Microsoft.Sql/servers`
   - Ubicación: West US 2

7. **Application Insights**
   - Nombre: `mvp-data-pipeline-dev-ai`
   - Tipo: `Microsoft.Insights/components`
   - Ubicación: West US 2

8. **SQL Database - Master**
   - Nombre: `mvp-data-pipeline-dev-sql-2024/master`
   - Tipo: `Microsoft.Sql/servers/databases`
   - Ubicación: West US 2

9. **SQL Database - Principal**
   - Nombre: `mvp-data-pipeline-dev-sql-2024/mvp-data-pipeline-dev-db`
   - Tipo: `Microsoft.Sql/servers/databases`
   - Ubicación: West US 2

10. **Storage Account**
    - Nombre: `mvpdevsa2024`
    - Tipo: `Microsoft.Storage/storageAccounts`
    - Ubicación: West US 2

11. **Metric Alert**
    - Nombre: `sql-high-dtu`
    - Tipo: `Microsoft.Insights/metricalerts`
    - Ubicación: Global

12. **Smart Detection Action Group**
    - Nombre: `Application Insights Smart Detection`
    - Tipo: `microsoft.insights/actiongroups`
    - Ubicación: Global

## Análisis de Duplicación

### Recursos Duplicados Identificados:
1. **Action Groups:** Duplicado en ambos grupos
2. **Log Analytics Workspace:** Duplicado en ambos grupos
3. **Data Factory:** Versiones diferentes (sin sufijo vs -2024)
4. **Event Hub:** Versiones diferentes (sin sufijo vs -2024)
5. **Key Vault:** Versiones diferentes (-v3 vs -2024)
6. **SQL Server:** Versiones diferentes (-v2 vs -2024)
7. **Storage Account:** Versiones diferentes (sin sufijo vs 2024)

## Problemas de Nomenclatura Identificados

### Inconsistencias en Nombres:
1. **Prefijos Mixtos:** Algunos recursos usan "mvp-" otros no
2. **Sufijos Inconsistentes:** -v2, -v3, -2024, sin sufijo
3. **Convenciones Mixtas:** Algunos usan guiones, otros no
4. **Longitud Variable:** Nombres muy largos vs cortos

### Estándares Corporativos Recomendados:
- **Formato:** `{company}-{project}-{environment}-{resource}-{version}`
- **Ejemplo:** `pdg-datapipe-dev-sql-001`
- **Longitud Máxima:** 24 caracteres para compatibilidad
- **Caracteres:** Solo letras minúsculas, números y guiones

## Recursos Terraform Gestionados

### Estado Actual de Terraform:
- **Grupo Gestionado:** `rg-mvp-data-pipeline-dev-2024`
- **Recursos en Estado:** 38 recursos
- **Archivo de Estado:** Presente y actualizado

### Recursos No Gestionados por Terraform:
- **Grupo:** `mvp-data-pipeline-dev-rg`
- **Todos los recursos en este grupo** (9 recursos)

## Recomendaciones para Optimización

### 1. Eliminación Prioritaria:
- Eliminar completamente el grupo `mvp-data-pipeline-dev-rg`
- Consolidar todos los recursos en un solo grupo

### 2. Normalización de Nombres:
- Implementar convención corporativa consistente
- Usar prefijo "pdg" (Prodigio)
- Formato: `pdg-datapipe-dev-{resource}-001`

### 3. Recursos a Mantener:
- Mantener recursos del grupo `rg-mvp-data-pipeline-dev-2024`
- Renombrar según estándares corporativos

### 4. Optimizaciones Adicionales:
- Activar storage containers
- Implementar pipelines de Data Factory
- Configurar monitoreo avanzado

## Próximos Pasos

1. ✅ **Auditoría Completada**
2. 🔄 **Eliminación de Recursos Duplicados**
3. 🔄 **Normalización de Nombres**
4. 🔄 **Activación de Storage Containers**
5. 🔄 **Implementación de Pipelines**
6. 🔄 **Auditoría Final**

---
**Nota:** Esta auditoría sirve como línea base para la optimización del entorno Azure. Todos los cambios posteriores serán documentados y rastreados.