# Script para desplegar Azure Function manualmente
# Este script crea una Function App usando Azure CLI para evitar problemas de cuota

param(
    [string]$ResourceGroupName = "pdg-datapipe-dev-rg-001",
    [string]$StorageAccountName = "pdgdevsa001",
    [string]$FunctionAppName = "pdg-datapipe-dev-func-001",
    [string]$Location = "East US"
)

Write-Host "Desplegando Azure Function App..." -ForegroundColor Green

# Crear Function App usando Azure CLI
Write-Host "Creando Function App: $FunctionAppName" -ForegroundColor Yellow
az functionapp create `
    --resource-group $ResourceGroupName `
    --consumption-plan-location $Location `
    --runtime python `
    --runtime-version 3.11 `
    --functions-version 4 `
    --name $FunctionAppName `
    --storage-account $StorageAccountName `
    --os-type Linux

if ($LASTEXITCODE -eq 0) {
    Write-Host "Function App creada exitosamente!" -ForegroundColor Green
    
    # Configurar variables de entorno
    Write-Host "Configurando variables de entorno..." -ForegroundColor Yellow
    az functionapp config appsettings set `
        --name $FunctionAppName `
        --resource-group $ResourceGroupName `
        --settings `
        "STORAGE_ACCOUNT_NAME=$StorageAccountName" `
        "CONFIG_PATH=config/data_factory_config.yml" `
        "EXPECTATIONS_PATH=config/expectations.yml" `
        "ENABLE_LOGGING=true"
    
    Write-Host "Configuración completada!" -ForegroundColor Green
    Write-Host "Ahora puedes desplegar el código de la función usando:" -ForegroundColor Cyan
    Write-Host "func azure functionapp publish $FunctionAppName" -ForegroundColor Cyan
} else {
    Write-Host "Error al crear la Function App" -ForegroundColor Red
    exit 1
}