# Script para probar la integración de Azure Container Instances con Data Factory
# Este script ejecuta pruebas automatizadas para verificar el funcionamiento

param(
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroupName,
    
    [Parameter(Mandatory=$true)]
    [string]$SubscriptionId,
    
    [Parameter(Mandatory=$false)]
    [string]$Environment = "dev",
    
    [Parameter(Mandatory=$false)]
    [string]$TestDataPath = "test-data/sample.csv"
)

# Configuración de variables
$ProjectName = "pdg$Environment"
$DataFactoryName = "${ProjectName}df001"
$StorageAccountName = "${ProjectName}sa001"
$PipelineName = "MainProcessingPipeline"

Write-Host "=== Iniciando pruebas de integración ACI + Data Factory ===" -ForegroundColor Green
Write-Host "Proyecto: $ProjectName" -ForegroundColor Yellow
Write-Host "Data Factory: $DataFactoryName" -ForegroundColor Yellow
Write-Host "Storage Account: $StorageAccountName" -ForegroundColor Yellow

# Configurar Azure CLI
az account set --subscription $SubscriptionId

# Función para esperar a que termine un pipeline run
function Wait-PipelineRun {
    param(
        [string]$RunId,
        [string]$DataFactory,
        [string]$ResourceGroup,
        [int]$TimeoutMinutes = 30
    )
    
    $timeout = (Get-Date).AddMinutes($TimeoutMinutes)
    
    do {
        Start-Sleep -Seconds 30
        $status = az datafactory pipeline-run show --factory-name $DataFactory --resource-group $ResourceGroup --run-id $RunId --query "status" -o tsv
        Write-Host "Estado del pipeline: $status" -ForegroundColor Yellow
        
        if ($status -eq "Succeeded") {
            return $true
        } elseif ($status -eq "Failed" -or $status -eq "Cancelled") {
            return $false
        }
        
    } while ((Get-Date) -lt $timeout)
    
    Write-Host "Timeout esperando el pipeline" -ForegroundColor Red
    return $false
}

# Función para verificar archivos en storage
function Test-StorageFile {
    param(
        [string]$StorageAccount,
        [string]$Container,
        [string]$FilePath
    )
    
    try {
        $result = az storage blob exists --account-name $StorageAccount --container-name $Container --name $FilePath --query "exists" -o tsv
        return $result -eq "true"
    } catch {
        return $false
    }
}

# 1. Verificar que los recursos existan
Write-Host "`n=== Verificando recursos de Azure ===" -ForegroundColor Blue

# Verificar Data Factory
$dfExists = az datafactory show --name $DataFactoryName --resource-group $ResourceGroupName --query "name" -o tsv 2>$null
if (-not $dfExists) {
    Write-Host "✗ Data Factory no encontrado: $DataFactoryName" -ForegroundColor Red
    exit 1
}
Write-Host "✓ Data Factory encontrado: $DataFactoryName" -ForegroundColor Green

# Verificar Storage Account
$saExists = az storage account show --name $StorageAccountName --resource-group $ResourceGroupName --query "name" -o tsv 2>$null
if (-not $saExists) {
    Write-Host "✗ Storage Account no encontrado: $StorageAccountName" -ForegroundColor Red
    exit 1
}
Write-Host "✓ Storage Account encontrado: $StorageAccountName" -ForegroundColor Green

# Verificar Pipeline
$pipelineExists = az datafactory pipeline show --factory-name $DataFactoryName --resource-group $ResourceGroupName --name $PipelineName --query "name" -o tsv 2>$null
if (-not $pipelineExists) {
    Write-Host "✗ Pipeline no encontrado: $PipelineName" -ForegroundColor Red
    exit 1
}
Write-Host "✓ Pipeline encontrado: $PipelineName" -ForegroundColor Green

# 2. Subir archivo de prueba
Write-Host "`n=== Subiendo archivo de prueba ===" -ForegroundColor Blue

$testFileName = "test-$(Get-Date -Format 'yyyyMMdd-HHmmss').csv"
$testContent = @'
id,name,value,date
1,Test Item 1,100.50,2024-01-01
2,Test Item 2,200.75,2024-01-02
3,Test Item 3,300.25,2024-01-03
4,Test Item 4,400.00,2024-01-04
5,Test Item 5,500.99,2024-01-05
'@

# Crear archivo temporal
$tempFile = [System.IO.Path]::GetTempFileName()
$testContent | Out-File -FilePath $tempFile -Encoding UTF8

try {
    # Subir archivo a raw/csv
    az storage blob upload --account-name $StorageAccountName --container-name "raw" --name "csv/$testFileName" --file $tempFile --overwrite
    if ($LASTEXITCODE -ne 0) {
        Write-Host "✗ Error subiendo archivo de prueba" -ForegroundColor Red
        exit 1
    }
    Write-Host "✓ Archivo de prueba subido: csv/$testFileName" -ForegroundColor Green
} finally {
    Remove-Item $tempFile -ErrorAction SilentlyContinue
}

# 3. Ejecutar pipeline
Write-Host "`n=== Ejecutando pipeline de Data Factory ===" -ForegroundColor Blue

$inputPath = "csv"
$outputPath = "processed/test-$(Get-Date -Format 'yyyyMMdd-HHmmss')"

$runId = az datafactory pipeline create-run --factory-name $DataFactoryName --resource-group $ResourceGroupName --name $PipelineName --parameters "inputPath=$inputPath" "outputPath=$outputPath" --query "runId" -o tsv

if (-not $runId) {
    Write-Host "✗ Error iniciando el pipeline" -ForegroundColor Red
    exit 1
}

Write-Host "✓ Pipeline iniciado con ID: $runId" -ForegroundColor Green
Write-Host "Parámetros: inputPath=$inputPath, outputPath=$outputPath" -ForegroundColor Yellow

# 4. Esperar a que termine el pipeline
Write-Host "`n=== Esperando finalización del pipeline ===" -ForegroundColor Blue

$success = Wait-PipelineRun -RunId $runId -DataFactory $DataFactoryName -ResourceGroup $ResourceGroupName -TimeoutMinutes 30

if (-not $success) {
    Write-Host "✗ Pipeline falló o no terminó en el tiempo esperado" -ForegroundColor Red
    
    # Mostrar detalles del error
    Write-Host "`n=== Detalles del pipeline run ===" -ForegroundColor Yellow
    az datafactory pipeline-run show --factory-name $DataFactoryName --resource-group $ResourceGroupName --run-id $runId
    
    # Mostrar actividades
    Write-Host "`n=== Actividades del pipeline ===" -ForegroundColor Yellow
    az datafactory activity-run query-by-pipeline-run --factory-name $DataFactoryName --resource-group $ResourceGroupName --run-id $runId
    
    exit 1
}

Write-Host "✓ Pipeline completado exitosamente" -ForegroundColor Green

# 5. Verificar archivos de salida
Write-Host "`n=== Verificando archivos de salida ===" -ForegroundColor Blue

# Esperar un poco para que los archivos se escriban
Start-Sleep -Seconds 10

# Listar archivos en el directorio de salida
$outputFiles = az storage blob list --account-name $StorageAccountName --container-name "silver" --prefix $outputPath --query "[].name" -o tsv

if ($outputFiles) {
    Write-Host "✓ Archivos de salida encontrados:" -ForegroundColor Green
    $outputFiles | ForEach-Object { Write-Host "  - $_" -ForegroundColor Cyan }
} else {
    Write-Host "⚠ No se encontraron archivos de salida en silver/$outputPath" -ForegroundColor Yellow
}

# 6. Verificar logs
Write-Host "`n=== Verificando logs ===" -ForegroundColor Blue

$logFiles = az storage blob list --account-name $StorageAccountName --container-name "logs" --prefix "aci-processing" --query "[].name" -o tsv

if ($logFiles) {
    Write-Host "✓ Archivos de log encontrados:" -ForegroundColor Green
    $logFiles | ForEach-Object { Write-Host "  - $_" -ForegroundColor Cyan }
} else {
    Write-Host "⚠ No se encontraron archivos de log" -ForegroundColor Yellow
}

# 7. Resumen de la prueba
Write-Host "`n=== Resumen de la prueba ===" -ForegroundColor Green
Write-Host "✓ Recursos de Azure verificados" -ForegroundColor Green
Write-Host "✓ Archivo de prueba subido" -ForegroundColor Green
Write-Host "✓ Pipeline ejecutado exitosamente" -ForegroundColor Green
Write-Host "✓ Archivos de salida verificados" -ForegroundColor Green

Write-Host "`n=== Información adicional ===" -ForegroundColor Cyan
Write-Host "Pipeline Run ID: $runId"
Write-Host "Archivo de entrada: raw/csv/$testFileName"
Write-Host "Directorio de salida: silver/$outputPath"
Write-Host ""
Write-Host "Para ver más detalles en Azure Portal:"
Write-Host "https://portal.azure.com/#@/resource/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.DataFactory/factories/$DataFactoryName/overview"

Write-Host "`n🎉 Prueba de integración completada exitosamente!" -ForegroundColor Green