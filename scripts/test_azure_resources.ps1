# Script de PowerShell para probar recursos de Azure
# Archivo: scripts/test_azure_resources.ps1

param(
    [Parameter(Mandatory=$false)]
    [string]$SubscriptionId = "d6a71f50-d4ae-463a-9b56-e4a54988c47e",
    
    [Parameter(Mandatory=$false)]
    [string]$ResourceGroupName = "mvp-config-driven-pipeline-dev-rg",
    
    [Parameter(Mandatory=$false)]
    [string]$StorageAccountName = "mvpdevsa",
    
    [Parameter(Mandatory=$false)]
    [string]$TestDataPath = "..\test_data\sample_data.csv"
)

# Configuración de colores para output
$ErrorColor = "Red"
$SuccessColor = "Green"
$InfoColor = "Cyan"
$WarningColor = "Yellow"

function Write-ColorOutput {
    param(
        [string]$Message,
        [string]$Color = "White"
    )
    Write-Host $Message -ForegroundColor $Color
}

function Test-AzureLogin {
    Write-ColorOutput "=== VERIFICANDO AUTENTICACIÓN DE AZURE ===" $InfoColor
    
    try {
        $context = Get-AzContext
        if ($null -eq $context) {
            Write-ColorOutput "No hay sesión activa de Azure. Iniciando login..." $WarningColor
            Connect-AzAccount
            $context = Get-AzContext
        }
        
        Write-ColorOutput "✅ Autenticado como: $($context.Account.Id)" $SuccessColor
        Write-ColorOutput "✅ Suscripción: $($context.Subscription.Name)" $SuccessColor
        return $true
    }
    catch {
        Write-ColorOutput "❌ Error en autenticación: $($_.Exception.Message)" $ErrorColor
        return $false
    }
}

function Test-ResourceGroup {
    Write-ColorOutput "`n=== VERIFICANDO GRUPO DE RECURSOS ===" $InfoColor
    
    try {
        $rg = Get-AzResourceGroup -Name $ResourceGroupName -ErrorAction SilentlyContinue
        if ($rg) {
            Write-ColorOutput "✅ Grupo de recursos encontrado: $($rg.ResourceGroupName)" $SuccessColor
            Write-ColorOutput "   Ubicación: $($rg.Location)" $InfoColor
            Write-ColorOutput "   Estado: $($rg.ProvisioningState)" $InfoColor
            return $true
        }
        else {
            Write-ColorOutput "❌ Grupo de recursos no encontrado: $ResourceGroupName" $ErrorColor
            return $false
        }
    }
    catch {
        Write-ColorOutput "❌ Error verificando grupo de recursos: $($_.Exception.Message)" $ErrorColor
        return $false
    }
}

function Test-StorageAccount {
    Write-ColorOutput "`n=== VERIFICANDO STORAGE ACCOUNT ===" $InfoColor
    
    try {
        $storageAccount = Get-AzStorageAccount -ResourceGroupName $ResourceGroupName -Name $StorageAccountName -ErrorAction SilentlyContinue
        if ($storageAccount) {
            Write-ColorOutput "✅ Storage Account encontrado: $($storageAccount.StorageAccountName)" $SuccessColor
            Write-ColorOutput "   Tipo: $($storageAccount.Kind)" $InfoColor
            Write-ColorOutput "   SKU: $($storageAccount.Sku.Name)" $InfoColor
            Write-ColorOutput "   Estado: $($storageAccount.ProvisioningState)" $InfoColor
            
            # Obtener contexto del storage
            $ctx = $storageAccount.Context
            
            # Verificar contenedores
            $containers = Get-AzStorageContainer -Context $ctx
            Write-ColorOutput "   Contenedores disponibles:" $InfoColor
            foreach ($container in $containers) {
                Write-ColorOutput "     - $($container.Name)" $InfoColor
            }
            
            return $ctx
        }
        else {
            Write-ColorOutput "❌ Storage Account no encontrado: $StorageAccountName" $ErrorColor
            return $null
        }
    }
    catch {
        Write-ColorOutput "❌ Error verificando Storage Account: $($_.Exception.Message)" $ErrorColor
        return $null
    }
}

function Test-SqlServer {
    Write-ColorOutput "`n=== VERIFICANDO SQL SERVER ===" $InfoColor
    
    try {
        $sqlServers = Get-AzSqlServer -ResourceGroupName $ResourceGroupName
        if ($sqlServers.Count -gt 0) {
            foreach ($server in $sqlServers) {
                Write-ColorOutput "✅ SQL Server encontrado: $($server.ServerName)" $SuccessColor
                Write-ColorOutput "   Versión: $($server.ServerVersion)" $InfoColor
                Write-ColorOutput "   Estado: $($server.State)" $InfoColor
                Write-ColorOutput "   Admin: $($server.SqlAdministratorLogin)" $InfoColor
                
                # Verificar bases de datos
                $databases = Get-AzSqlDatabase -ResourceGroupName $ResourceGroupName -ServerName $server.ServerName
                Write-ColorOutput "   Bases de datos:" $InfoColor
                foreach ($db in $databases) {
                    if ($db.DatabaseName -ne "master") {
                        Write-ColorOutput "     - $($db.DatabaseName) (Tier: $($db.CurrentServiceObjectiveName))" $InfoColor
                    }
                }
            }
            return $true
        }
        else {
            Write-ColorOutput "❌ No se encontraron SQL Servers" $ErrorColor
            return $false
        }
    }
    catch {
        Write-ColorOutput "❌ Error verificando SQL Server: $($_.Exception.Message)" $ErrorColor
        return $false
    }
}

function Test-DataFactory {
    Write-ColorOutput "`n=== VERIFICANDO DATA FACTORY ===" $InfoColor
    
    try {
        $dataFactories = Get-AzDataFactoryV2 -ResourceGroupName $ResourceGroupName
        if ($dataFactories.Count -gt 0) {
            foreach ($df in $dataFactories) {
                Write-ColorOutput "✅ Data Factory encontrado: $($df.DataFactoryName)" $SuccessColor
                Write-ColorOutput "   Ubicación: $($df.Location)" $InfoColor
                Write-ColorOutput "   Estado: $($df.ProvisioningState)" $InfoColor
                
                # Verificar pipelines
                $pipelines = Get-AzDataFactoryV2Pipeline -ResourceGroupName $ResourceGroupName -DataFactoryName $df.DataFactoryName
                Write-ColorOutput "   Pipelines ($($pipelines.Count)):" $InfoColor
                foreach ($pipeline in $pipelines) {
                    Write-ColorOutput "     - $($pipeline.Name)" $InfoColor
                }
                
                # Verificar linked services
                $linkedServices = Get-AzDataFactoryV2LinkedService -ResourceGroupName $ResourceGroupName -DataFactoryName $df.DataFactoryName
                Write-ColorOutput "   Linked Services ($($linkedServices.Count)):" $InfoColor
                foreach ($ls in $linkedServices) {
                    Write-ColorOutput "     - $($ls.Name)" $InfoColor
                }
            }
            return $true
        }
        else {
            Write-ColorOutput "❌ No se encontraron Data Factories" $ErrorColor
            return $false
        }
    }
    catch {
        Write-ColorOutput "❌ Error verificando Data Factory: $($_.Exception.Message)" $ErrorColor
        return $false
    }
}

function Test-EventHub {
    Write-ColorOutput "`n=== VERIFICANDO EVENT HUB ===" $InfoColor
    
    try {
        $eventHubNamespaces = Get-AzEventHubNamespace -ResourceGroupName $ResourceGroupName
        if ($eventHubNamespaces.Count -gt 0) {
            foreach ($namespace in $eventHubNamespaces) {
                Write-ColorOutput "✅ Event Hub Namespace encontrado: $($namespace.Name)" $SuccessColor
                Write-ColorOutput "   Ubicación: $($namespace.Location)" $InfoColor
                Write-ColorOutput "   Estado: $($namespace.ProvisioningState)" $InfoColor
                Write-ColorOutput "   SKU: $($namespace.Sku.Name)" $InfoColor
                
                # Verificar Event Hubs
                $eventHubs = Get-AzEventHub -ResourceGroupName $ResourceGroupName -NamespaceName $namespace.Name
                Write-ColorOutput "   Event Hubs ($($eventHubs.Count)):" $InfoColor
                foreach ($eh in $eventHubs) {
                    Write-ColorOutput "     - $($eh.Name) (Particiones: $($eh.PartitionCount))" $InfoColor
                }
            }
            return $true
        }
        else {
            Write-ColorOutput "❌ No se encontraron Event Hub Namespaces" $ErrorColor
            return $false
        }
    }
    catch {
        Write-ColorOutput "❌ Error verificando Event Hub: $($_.Exception.Message)" $ErrorColor
        return $false
    }
}

function Test-KeyVault {
    Write-ColorOutput "`n=== VERIFICANDO KEY VAULT ===" $InfoColor
    
    try {
        $keyVaults = Get-AzKeyVault -ResourceGroupName $ResourceGroupName
        if ($keyVaults.Count -gt 0) {
            foreach ($kv in $keyVaults) {
                Write-ColorOutput "✅ Key Vault encontrado: $($kv.VaultName)" $SuccessColor
                Write-ColorOutput "   URI: $($kv.VaultUri)" $InfoColor
                Write-ColorOutput "   Ubicación: $($kv.Location)" $InfoColor
                
                # Verificar secretos (requiere permisos)
                try {
                    $secrets = Get-AzKeyVaultSecret -VaultName $kv.VaultName
                    Write-ColorOutput "   Secretos ($($secrets.Count)):" $InfoColor
                    foreach ($secret in $secrets) {
                        Write-ColorOutput "     - $($secret.Name)" $InfoColor
                    }
                }
                catch {
                    Write-ColorOutput "   ⚠️  No se pueden listar secretos (permisos insuficientes)" $WarningColor
                }
            }
            return $true
        }
        else {
            Write-ColorOutput "❌ No se encontraron Key Vaults" $ErrorColor
            return $false
        }
    }
    catch {
        Write-ColorOutput "❌ Error verificando Key Vault: $($_.Exception.Message)" $ErrorColor
        return $false
    }
}

function Upload-TestData {
    param(
        [Microsoft.Azure.Commands.Management.Storage.Models.PSStorageAccount]$StorageContext
    )
    
    Write-ColorOutput "`n=== SUBIENDO DATOS DE PRUEBA ===" $InfoColor
    
    if (-not $StorageContext) {
        Write-ColorOutput "❌ No hay contexto de Storage Account disponible" $ErrorColor
        return $false
    }
    
    try {
        # Verificar si existe el archivo de datos de prueba
        $testDataFullPath = Join-Path $PSScriptRoot $TestDataPath
        if (-not (Test-Path $testDataFullPath)) {
            Write-ColorOutput "❌ Archivo de datos de prueba no encontrado: $testDataFullPath" $ErrorColor
            return $false
        }
        
        Write-ColorOutput "📁 Archivo de datos encontrado: $testDataFullPath" $InfoColor
        
        # Crear contenedor si no existe
        $containerName = "test-data"
        $container = Get-AzStorageContainer -Name $containerName -Context $StorageContext -ErrorAction SilentlyContinue
        if (-not $container) {
            Write-ColorOutput "📦 Creando contenedor: $containerName" $InfoColor
            New-AzStorageContainer -Name $containerName -Context $StorageContext -Permission Blob
        }
        
        # Subir archivo
        $blobName = "sample_data_$(Get-Date -Format 'yyyyMMdd_HHmmss').csv"
        Write-ColorOutput "⬆️  Subiendo archivo como: $blobName" $InfoColor
        
        $blob = Set-AzStorageBlobContent -File $testDataFullPath -Container $containerName -Blob $blobName -Context $StorageContext
        
        Write-ColorOutput "✅ Archivo subido exitosamente" $SuccessColor
        Write-ColorOutput "   URL: $($blob.ICloudBlob.StorageUri.PrimaryUri)" $InfoColor
        
        return $true
    }
    catch {
        Write-ColorOutput "❌ Error subiendo datos de prueba: $($_.Exception.Message)" $ErrorColor
        return $false
    }
}

function Test-LogAnalytics {
    Write-ColorOutput "`n=== VERIFICANDO LOG ANALYTICS ===" $InfoColor
    
    try {
        $workspaces = Get-AzOperationalInsightsWorkspace -ResourceGroupName $ResourceGroupName
        if ($workspaces.Count -gt 0) {
            foreach ($workspace in $workspaces) {
                Write-ColorOutput "✅ Log Analytics Workspace encontrado: $($workspace.Name)" $SuccessColor
                Write-ColorOutput "   Ubicación: $($workspace.Location)" $InfoColor
                Write-ColorOutput "   SKU: $($workspace.Sku)" $InfoColor
                Write-ColorOutput "   Estado: $($workspace.ProvisioningState)" $InfoColor
            }
            return $true
        }
        else {
            Write-ColorOutput "❌ No se encontraron Log Analytics Workspaces" $ErrorColor
            return $false
        }
    }
    catch {
        Write-ColorOutput "❌ Error verificando Log Analytics: $($_.Exception.Message)" $ErrorColor
        return $false
    }
}

function Show-ResourceSummary {
    param(
        [hashtable]$TestResults
    )
    
    Write-ColorOutput "`n=== RESUMEN DE RECURSOS ===" $InfoColor
    Write-ColorOutput "================================" $InfoColor
    
    foreach ($resource in $TestResults.Keys) {
        $status = if ($TestResults[$resource]) { "✅ OK" } else { "❌ ERROR" }
        $color = if ($TestResults[$resource]) { $SuccessColor } else { $ErrorColor }
        Write-ColorOutput "$resource : $status" $color
    }
    
    $successCount = ($TestResults.Values | Where-Object { $_ -eq $true }).Count
    $totalCount = $TestResults.Count
    
    Write-ColorOutput "`nRecursos funcionando: $successCount/$totalCount" $InfoColor
    
    if ($successCount -eq $totalCount) {
        Write-ColorOutput "🎉 ¡Todos los recursos están funcionando correctamente!" $SuccessColor
    }
    elseif ($successCount -gt 0) {
        Write-ColorOutput "⚠️  Algunos recursos tienen problemas" $WarningColor
    }
    else {
        Write-ColorOutput "❌ Ningún recurso está funcionando" $ErrorColor
    }
}

# FUNCIÓN PRINCIPAL
function Main {
    Write-ColorOutput "🚀 INICIANDO PRUEBAS DE RECURSOS DE AZURE" $InfoColor
    Write-ColorOutput "==========================================" $InfoColor
    Write-ColorOutput "Suscripción: $SubscriptionId" $InfoColor
    Write-ColorOutput "Grupo de Recursos: $ResourceGroupName" $InfoColor
    Write-ColorOutput "Fecha: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" $InfoColor
    
    # Verificar módulos de Azure PowerShell
    $requiredModules = @('Az.Accounts', 'Az.Resources', 'Az.Storage', 'Az.Sql', 'Az.DataFactory', 'Az.EventHub', 'Az.KeyVault', 'Az.OperationalInsights')
    foreach ($module in $requiredModules) {
        if (-not (Get-Module -ListAvailable -Name $module)) {
            Write-ColorOutput "❌ Módulo requerido no encontrado: $module" $ErrorColor
            Write-ColorOutput "   Instalar con: Install-Module -Name $module" $InfoColor
            return
        }
    }
    
    # Establecer suscripción
    try {
        Set-AzContext -SubscriptionId $SubscriptionId | Out-Null
        Write-ColorOutput "✅ Suscripción establecida correctamente" $SuccessColor
    }
    catch {
        Write-ColorOutput "❌ Error estableciendo suscripción: $($_.Exception.Message)" $ErrorColor
        return
    }
    
    # Ejecutar pruebas
    $testResults = @{}
    
    # Autenticación
    $testResults['Autenticación'] = Test-AzureLogin
    
    if ($testResults['Autenticación']) {
        # Recursos principales
        $testResults['Grupo de Recursos'] = Test-ResourceGroup
        $storageContext = Test-StorageAccount
        $testResults['Storage Account'] = $storageContext -ne $null
        $testResults['SQL Server'] = Test-SqlServer
        $testResults['Data Factory'] = Test-DataFactory
        $testResults['Event Hub'] = Test-EventHub
        $testResults['Key Vault'] = Test-KeyVault
        $testResults['Log Analytics'] = Test-LogAnalytics
        
        # Prueba de carga de datos
        if ($storageContext) {
            $testResults['Carga de Datos'] = Upload-TestData -StorageContext $storageContext
        }
        else {
            $testResults['Carga de Datos'] = $false
        }
    }
    
    # Mostrar resumen
    Show-ResourceSummary -TestResults $testResults
    
    Write-ColorOutput "`n🏁 PRUEBAS COMPLETADAS" $InfoColor
}

# Ejecutar script principal
Main