# Script para desplegar la integración de Azure Container Instances con Data Factory
# Este script automatiza el proceso de construcción, push de imagen y despliegue

param(
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroupName,
    
    [Parameter(Mandatory=$true)]
    [string]$SubscriptionId,
    
    [Parameter(Mandatory=$false)]
    [string]$Environment = "dev",
    
    [Parameter(Mandatory=$false)]
    [switch]$SkipImageBuild = $false,
    
    [Parameter(Mandatory=$false)]
    [switch]$DestroyFirst = $false
)

# Configuración de variables
$ProjectName = "pdg$Environment"
$ContainerRegistryName = "${ProjectName}acr001"
$ImageName = "data-pipeline"
$ImageTag = "latest"

Write-Host "=== Iniciando despliegue de integración ACI + Data Factory ===" -ForegroundColor Green
Write-Host "Proyecto: $ProjectName" -ForegroundColor Yellow
Write-Host "Suscripción: $SubscriptionId" -ForegroundColor Yellow
Write-Host "Grupo de recursos: $ResourceGroupName" -ForegroundColor Yellow

# Función para verificar si un comando existe
function Test-Command($cmdname) {
    return [bool](Get-Command -Name $cmdname -ErrorAction SilentlyContinue)
}

# Verificar herramientas requeridas
Write-Host "`n=== Verificando herramientas requeridas ===" -ForegroundColor Blue

$requiredTools = @("az", "terraform", "docker")
foreach ($tool in $requiredTools) {
    if (Test-Command $tool) {
        Write-Host "✓ $tool está disponible" -ForegroundColor Green
    } else {
        Write-Host "✗ $tool no está disponible" -ForegroundColor Red
        exit 1
    }
}

# Configurar Azure CLI
Write-Host "`n=== Configurando Azure CLI ===" -ForegroundColor Blue
az account set --subscription $SubscriptionId
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error configurando la suscripción de Azure" -ForegroundColor Red
    exit 1
}

# Cambiar al directorio del proyecto
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

# Destruir infraestructura existente si se solicita
if ($DestroyFirst) {
    Write-Host "`n=== Destruyendo infraestructura existente ===" -ForegroundColor Yellow
    Set-Location "terraform"
    terraform destroy -auto-approve -var="environment=$Environment"
    Set-Location $projectRoot
}

# Construir y subir imagen Docker si no se omite
if (-not $SkipImageBuild) {
    Write-Host "`n=== Construyendo imagen Docker ===" -ForegroundColor Blue
    
    # Verificar si el Dockerfile existe
    if (-not (Test-Path "Dockerfile.pipeline")) {
        Write-Host "Error: Dockerfile.pipeline no encontrado" -ForegroundColor Red
        exit 1
    }
    
    # Construir imagen
    docker build -f Dockerfile.pipeline -t "${ImageName}:${ImageTag}" .
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error construyendo la imagen Docker" -ForegroundColor Red
        exit 1
    }
    
    Write-Host "✓ Imagen Docker construida exitosamente" -ForegroundColor Green
}

# Desplegar infraestructura con Terraform
Write-Host "`n=== Desplegando infraestructura con Terraform ===" -ForegroundColor Blue
Set-Location "terraform"

# Inicializar Terraform
terraform init
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error inicializando Terraform" -ForegroundColor Red
    exit 1
}

# Planificar despliegue
terraform plan -var="environment=$Environment" -out="tfplan"
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error planificando el despliegue" -ForegroundColor Red
    exit 1
}

# Aplicar despliegue
terraform apply "tfplan"
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error aplicando el despliegue" -ForegroundColor Red
    exit 1
}

# Obtener outputs de Terraform
$acrLoginServer = terraform output -raw container_registry_login_server
$acrName = terraform output -raw container_registry_name

Set-Location $projectRoot

# Subir imagen al Container Registry si no se omite la construcción
if (-not $SkipImageBuild) {
    Write-Host "`n=== Subiendo imagen al Container Registry ===" -ForegroundColor Blue
    
    # Login al ACR
    az acr login --name $acrName
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error haciendo login al Container Registry" -ForegroundColor Red
        exit 1
    }
    
    # Etiquetar imagen para ACR
    docker tag "${ImageName}:${ImageTag}" "${acrLoginServer}/${ImageName}:${ImageTag}"
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error etiquetando la imagen" -ForegroundColor Red
        exit 1
    }
    
    # Subir imagen
    docker push "${acrLoginServer}/${ImageName}:${ImageTag}"
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error subiendo la imagen al registry" -ForegroundColor Red
        exit 1
    }
    
    Write-Host "✓ Imagen subida exitosamente al Container Registry" -ForegroundColor Green
}

# Mostrar información de despliegue
Write-Host "`n=== Despliegue completado exitosamente ===" -ForegroundColor Green
Write-Host "Container Registry: $acrLoginServer" -ForegroundColor Yellow
Write-Host "Imagen: ${acrLoginServer}/${ImageName}:${ImageTag}" -ForegroundColor Yellow

# Obtener información adicional de Terraform
Set-Location "terraform"
$dataFactoryName = terraform output -raw data_factory_name
$aciContainerGroupName = terraform output -raw aci_container_group_name

Write-Host "Data Factory: $dataFactoryName" -ForegroundColor Yellow
Write-Host "Container Group: $aciContainerGroupName" -ForegroundColor Yellow

Set-Location $projectRoot

Write-Host "`n=== Próximos pasos ===" -ForegroundColor Cyan
Write-Host "1. Verificar que la imagen esté disponible en ACR"
Write-Host "2. Probar el pipeline de Data Factory manualmente"
Write-Host "3. Revisar logs en Azure Portal"
Write-Host "4. Configurar triggers automáticos si es necesario"

Write-Host "`nPara probar el pipeline, ejecuta:"
Write-Host "az datafactory pipeline create-run --factory-name $dataFactoryName --resource-group $ResourceGroupName --name MainProcessingPipeline --parameters inputPath=raw/csv outputPath=silver/processed" -ForegroundColor Cyan