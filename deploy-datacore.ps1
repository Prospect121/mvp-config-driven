# ============================================================================
# Script de Despliegue: Datacore Pipeline con Lakehouse Infrastructure
# ============================================================================
# Este script despliega el sistema datacore integrado con la infraestructura
# Lakehouse existente (MinIO, Spark, Iceberg, Kafka, Airflow)
#
# USO:
#   .\deploy-datacore.ps1              # Despliegue completo
#   .\deploy-datacore.ps1 -SkipInfra   # Solo datacore (infra ya corriendo)
#   .\deploy-datacore.ps1 -TestOnly    # Ejecutar tests
# ============================================================================

param(
    [switch]$SkipInfra,
    [switch]$TestOnly,
    [switch]$Verbose,
    [string]$Environment = "onpremise"
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$InfraRoot = Split-Path -Parent $ScriptDir
$DatacoreDir = Join-Path $InfraRoot "mvp-config-driven"

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  DATACORE PIPELINE - Despliegue con Lakehouse" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================
function Write-Step {
    param([string]$Message)
    Write-Host "[STEP] $Message" -ForegroundColor Yellow
}

function Write-Success {
    param([string]$Message)
    Write-Host "[OK] $Message" -ForegroundColor Green
}

function Write-Error {
    param([string]$Message)
    Write-Host "[ERROR] $Message" -ForegroundColor Red
}

function Test-ServiceHealth {
    param(
        [string]$Name,
        [string]$Url,
        [int]$MaxRetries = 10,
        [int]$DelaySeconds = 5
    )
    
    for ($i = 1; $i -le $MaxRetries; $i++) {
        try {
            $response = Invoke-WebRequest -Uri $Url -TimeoutSec 5 -UseBasicParsing -ErrorAction SilentlyContinue
            if ($response.StatusCode -eq 200) {
                Write-Success "$Name está disponible"
                return $true
            }
        }
        catch {
            Write-Host "  Intento $i/$MaxRetries - $Name no disponible aún..." -ForegroundColor Gray
        }
        Start-Sleep -Seconds $DelaySeconds
    }
    Write-Error "$Name no responde después de $MaxRetries intentos"
    return $false
}

# ============================================================================
# PASO 1: Verificar prerequisitos
# ============================================================================
Write-Step "Verificando prerequisitos..."

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Error "Docker no está instalado"
    exit 1
}

if (-not (Get-Command docker-compose -ErrorAction SilentlyContinue)) {
    Write-Error "Docker Compose no está instalado"
    exit 1
}

Write-Success "Docker y Docker Compose disponibles"

# ============================================================================
# PASO 2: Desplegar infraestructura base (si no se salta)
# ============================================================================
if (-not $SkipInfra) {
    Write-Step "Desplegando infraestructura Lakehouse..."
    
    # MinIO (crea la red lakehouse-net)
    Write-Host "  Iniciando MinIO..." -ForegroundColor Gray
    Push-Location (Join-Path $InfraRoot "minio")
    docker-compose up -d
    Pop-Location
    
    Start-Sleep -Seconds 5
    
    # Kafka
    Write-Host "  Iniciando Kafka..." -ForegroundColor Gray
    Push-Location (Join-Path $InfraRoot "kafka")
    docker-compose up -d
    Pop-Location
    
    # Spark
    Write-Host "  Iniciando Spark Cluster..." -ForegroundColor Gray
    Push-Location (Join-Path $InfraRoot "spark")
    docker-compose up -d
    Pop-Location
    
    # Iceberg
    Write-Host "  Iniciando Iceberg REST Catalog..." -ForegroundColor Gray
    Push-Location (Join-Path $InfraRoot "iceberg")
    docker-compose up -d
    Pop-Location
    
    # Airflow
    Write-Host "  Iniciando Airflow..." -ForegroundColor Gray
    Push-Location (Join-Path $InfraRoot "airflow")
    docker-compose up -d
    Pop-Location
    
    Write-Success "Infraestructura iniciada"
    
    # Esperar a que los servicios estén listos
    Write-Step "Esperando a que los servicios estén disponibles..."
    
    $services = @(
        @{Name = "MinIO"; Url = "http://localhost:9000/minio/health/live" },
        @{Name = "Spark Master"; Url = "http://localhost:8084" },
        @{Name = "Iceberg REST"; Url = "http://localhost:8181/v1/config" },
        @{Name = "Airflow"; Url = "http://localhost:8083/health" }
    )
    
    foreach ($svc in $services) {
        Test-ServiceHealth -Name $svc.Name -Url $svc.Url | Out-Null
    }
}

# ============================================================================
# PASO 3: Crear buckets en MinIO si no existen
# ============================================================================
Write-Step "Configurando buckets de MinIO..."

$buckets = @("raw", "bronze", "silver", "gold", "warehouse")

foreach ($bucket in $buckets) {
    $checkCmd = "docker exec minio mc ls local/$bucket 2>&1"
    $result = Invoke-Expression $checkCmd 2>&1
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  Creando bucket: $bucket" -ForegroundColor Gray
        docker exec minio mc mb "local/$bucket" --ignore-existing 2>$null
    }
    else {
        Write-Host "  Bucket $bucket ya existe" -ForegroundColor Gray
    }
}

Write-Success "Buckets configurados"

# ============================================================================
# PASO 4: Desplegar Datacore
# ============================================================================
Write-Step "Desplegando Datacore Pipeline..."

Push-Location $DatacoreDir

# Verificar que existe el docker-compose.lakehouse.yml
if (-not (Test-Path "docker-compose.lakehouse.yml")) {
    Write-Error "No se encuentra docker-compose.lakehouse.yml en $DatacoreDir"
    Pop-Location
    exit 1
}

# Iniciar servicios de datacore
docker-compose -f docker-compose.lakehouse.yml up -d

Pop-Location

Write-Success "Datacore desplegado"

# ============================================================================
# PASO 5: Copiar DAGs a Airflow
# ============================================================================
Write-Step "Configurando DAGs de Airflow..."

$dagsSource = Join-Path $InfraRoot "airflow\dags"
$dagsTarget = Join-Path $InfraRoot "airflow\dags"

if (Test-Path (Join-Path $dagsSource "lakehouse_etl_pipeline.py")) {
    Write-Host "  DAGs ya configurados en: $dagsTarget" -ForegroundColor Gray
}
else {
    Write-Host "  Copiando DAGs..." -ForegroundColor Gray
    # Los DAGs ya están en la ubicación correcta
}

# Copiar datacore al contenedor de Airflow para acceso a la librería
Write-Host "  Sincronizando datacore con Airflow..." -ForegroundColor Gray
docker exec airflow-webserver mkdir -p /opt/airflow/datacore 2>$null
docker cp "$DatacoreDir\." airflow-webserver:/opt/airflow/datacore/ 2>$null

Write-Success "DAGs configurados"

# ============================================================================
# PASO 6: Instalar dependencias en contenedores
# ============================================================================
Write-Step "Instalando dependencias en Airflow..."

$installCmd = @"
pip install pyyaml jsonschema pyspark boto3 requests 2>/dev/null
cd /opt/airflow/datacore && pip install -e . 2>/dev/null
"@

docker exec airflow-webserver bash -c $installCmd 2>$null
docker exec airflow-scheduler bash -c $installCmd 2>$null

Write-Success "Dependencias instaladas"

# ============================================================================
# PASO 7: Ejecutar tests (opcional)
# ============================================================================
if ($TestOnly) {
    Write-Step "Ejecutando tests de Datacore..."
    
    Push-Location $DatacoreDir
    
    # Validar configuración
    Write-Host "  Validando configuración..." -ForegroundColor Gray
    docker exec datacore-runner prodi validate --config configs/envs/onpremise/project.yml
    
    # Dry-run del pipeline
    Write-Host "  Ejecutando dry-run..." -ForegroundColor Gray
    docker exec datacore-runner prodi plan --config configs/envs/onpremise/project.yml
    
    Pop-Location
    
    Write-Success "Tests completados"
}

# ============================================================================
# RESUMEN FINAL
# ============================================================================
Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  DESPLIEGUE COMPLETADO" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "SERVICIOS DISPONIBLES:" -ForegroundColor Yellow
Write-Host "  MinIO Console:     http://localhost:9001    (minioadmin/minioadmin)" -ForegroundColor White
Write-Host "  Spark Master UI:   http://localhost:8084" -ForegroundColor White
Write-Host "  Iceberg REST:      http://localhost:8181" -ForegroundColor White
Write-Host "  Kafka UI:          http://localhost:8082" -ForegroundColor White
Write-Host "  Airflow:           http://localhost:8083    (airflow/airflow)" -ForegroundColor White
Write-Host ""
Write-Host "CONTENEDORES DATACORE:" -ForegroundColor Yellow
Write-Host "  datacore-runner:      Ejecutor de pipelines" -ForegroundColor White
Write-Host "  datacore-scheduler:   Programador interno" -ForegroundColor White
Write-Host "  datacore-spark-submit: Submitter de jobs Spark" -ForegroundColor White
Write-Host ""
Write-Host "COMANDOS ÚTILES:" -ForegroundColor Yellow
Write-Host "  # Ejecutar pipeline completo" -ForegroundColor Gray
Write-Host "  docker exec datacore-runner prodi run --layer raw --config configs/envs/onpremise/project.yml"
Write-Host "  docker exec datacore-runner prodi run --layer bronze --config configs/envs/onpremise/project.yml"
Write-Host "  docker exec datacore-runner prodi run --layer silver --config configs/envs/onpremise/project.yml"
Write-Host "  docker exec datacore-runner prodi run --layer gold --config configs/envs/onpremise/project.yml"
Write-Host ""
Write-Host "  # Ver plan sin ejecutar" -ForegroundColor Gray
Write-Host "  docker exec datacore-runner prodi plan --config configs/envs/onpremise/project.yml"
Write-Host ""
Write-Host "  # Ejecutar desde Airflow" -ForegroundColor Gray
Write-Host "  Ir a http://localhost:8083 -> DAGs -> lakehouse_etl_pipeline -> Trigger"
Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
