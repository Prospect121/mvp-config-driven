# Script de prueba para validar la integración completa del pipeline
# Requiere Azure CLI y permisos adecuados

param(
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroupName = "rg-pdg-datapipe-dev-001",
    
    [Parameter(Mandatory=$true)]
    [string]$DataFactoryName = "pdg-datapipe-dev-df-001",
    
    [Parameter(Mandatory=$true)]
    [string]$StorageAccountName = "pdgdevsa001",
    
    [string]$PipelineName = "MainProcessingPipeline"
)

Write-Host "=== Iniciando pruebas de integración del pipeline ===" -ForegroundColor Green

# 1. Verificar que Azure CLI esté instalado y autenticado
Write-Host "1. Verificando Azure CLI..." -ForegroundColor Yellow
try {
    $account = az account show --query "name" -o tsv
    Write-Host "✓ Conectado a Azure: $account" -ForegroundColor Green
} catch {
    Write-Error "Azure CLI no está instalado o no está autenticado. Ejecute 'az login' primero."
    exit 1
}

# 2. Crear datos de prueba
Write-Host "2. Creando datos de prueba..." -ForegroundColor Yellow
$testData = @"
id,timestamp,value,category
1,2024-01-01T10:00:00Z,100.5,A
2,2024-01-01T10:01:00Z,200.3,B
3,2024-01-01T10:02:00Z,150.7,A
4,2024-01-01T10:03:00Z,300.2,C
5,2024-01-01T10:04:00Z,250.8,B
"@

$tempFile = [System.IO.Path]::GetTempFileName() + ".csv"
$testData | Out-File -FilePath $tempFile -Encoding UTF8
Write-Host "✓ Archivo de prueba creado: $tempFile" -ForegroundColor Green

# 3. Subir archivo de prueba a Azure Storage
Write-Host "3. Subiendo archivo de prueba a Azure Storage..." -ForegroundColor Yellow
try {
    az storage blob upload `
        --account-name $StorageAccountName `
        --container-name "raw" `
        --name "csv/test_data_$(Get-Date -Format 'yyyyMMdd_HHmmss').csv" `
        --file $tempFile `
        --auth-mode login
    Write-Host "✓ Archivo subido exitosamente" -ForegroundColor Green
} catch {
    Write-Error "Error al subir archivo a Azure Storage: $_"
    exit 1
}

# 4. Verificar que el pipeline existe
Write-Host "4. Verificando que el pipeline existe..." -ForegroundColor Yellow
try {
    $pipeline = az datafactory pipeline show `
        --resource-group $ResourceGroupName `
        --factory-name $DataFactoryName `
        --name $PipelineName `
        --query "name" -o tsv
    Write-Host "✓ Pipeline encontrado: $pipeline" -ForegroundColor Green
} catch {
    Write-Error "Pipeline no encontrado: $_"
    exit 1
}

# 5. Ejecutar el pipeline
Write-Host "5. Ejecutando el pipeline..." -ForegroundColor Yellow
try {
    $runId = az datafactory pipeline create-run `
        --resource-group $ResourceGroupName `
        --factory-name $DataFactoryName `
        --name $PipelineName `
        --query "runId" -o tsv
    Write-Host "✓ Pipeline ejecutado. Run ID: $runId" -ForegroundColor Green
} catch {
    Write-Error "Error al ejecutar pipeline: $_"
    exit 1
}

# 6. Monitorear la ejecución del pipeline
Write-Host "6. Monitoreando la ejecución del pipeline..." -ForegroundColor Yellow
$maxWaitTime = 300 # 5 minutos
$waitTime = 0
$status = "InProgress"

while ($status -eq "InProgress" -and $waitTime -lt $maxWaitTime) {
    Start-Sleep -Seconds 10
    $waitTime += 10
    
    try {
        $status = az datafactory pipeline-run show `
            --resource-group $ResourceGroupName `
            --factory-name $DataFactoryName `
            --run-id $runId `
            --query "status" -o tsv
        
        Write-Host "Estado actual: $status (Tiempo transcurrido: $waitTime segundos)" -ForegroundColor Cyan
    } catch {
        Write-Warning "Error al obtener estado del pipeline: $_"
    }
}

# 7. Verificar resultado final
Write-Host "7. Verificando resultado final..." -ForegroundColor Yellow
if ($status -eq "Succeeded") {
    Write-Host "✓ Pipeline ejecutado exitosamente!" -ForegroundColor Green
    
    # Verificar archivos de salida
    Write-Host "8. Verificando archivos de salida..." -ForegroundColor Yellow
    try {
        $outputFiles = az storage blob list `
            --account-name $StorageAccountName `
            --container-name "silver" `
            --prefix "processed/" `
            --auth-mode login `
            --query "[].name" -o tsv
        
        if ($outputFiles) {
            Write-Host "✓ Archivos de salida encontrados:" -ForegroundColor Green
            $outputFiles | ForEach-Object { Write-Host "  - $_" -ForegroundColor White }
        } else {
            Write-Warning "No se encontraron archivos de salida en el contenedor 'silver/processed/'"
        }
    } catch {
        Write-Warning "Error al verificar archivos de salida: $_"
    }
} elseif ($status -eq "Failed") {
    Write-Error "Pipeline falló. Verificar logs en Azure Data Factory."
    
    # Obtener detalles del error
    try {
        $errorDetails = az datafactory pipeline-run show `
            --resource-group $ResourceGroupName `
            --factory-name $DataFactoryName `
            --run-id $runId `
            --query "message" -o tsv
        Write-Host "Detalles del error: $errorDetails" -ForegroundColor Red
    } catch {
        Write-Warning "No se pudieron obtener detalles del error"
    }
} else {
    Write-Warning "Pipeline aún en ejecución o estado desconocido: $status"
}

# 9. Limpiar archivo temporal
Remove-Item -Path $tempFile -Force
Write-Host "✓ Archivo temporal eliminado" -ForegroundColor Green

Write-Host "=== Pruebas de integración completadas ===" -ForegroundColor Green
Write-Host "Para más detalles, revisar Azure Data Factory en el portal: https://portal.azure.com" -ForegroundColor Cyan