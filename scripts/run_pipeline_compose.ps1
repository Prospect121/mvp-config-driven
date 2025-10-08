# Script para ejecutar el pipeline usando docker-compose
# Uso: .\run_pipeline_compose.ps1 -InputFile "sample_data.csv" -OutputFile "output.parquet"

param(
    [Parameter(Mandatory=$true)]
    [string]$InputFile,
    
    [Parameter(Mandatory=$true)]
    [string]$OutputFile,
    
    [string]$LogLevel = "INFO",
    [switch]$TestMode,
    [switch]$BuildImage,
    [switch]$CleanUp
)

$ErrorActionPreference = "Stop"

# Colores para output
function Write-Success { param($Message) Write-Host "✅ $Message" -ForegroundColor Green }
function Write-Error { param($Message) Write-Host "❌ $Message" -ForegroundColor Red }
function Write-Info { param($Message) Write-Host "ℹ️  $Message" -ForegroundColor Blue }
function Write-Warning { param($Message) Write-Host "⚠️  $Message" -ForegroundColor Yellow }

# Obtener directorio del proyecto
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$TestOutputDir = Join-Path $ProjectRoot "test_output"
$LogsDir = Join-Path $ProjectRoot "logs"

Write-Info "Directorio del proyecto: $ProjectRoot"

# Crear directorios necesarios
@($TestOutputDir, $LogsDir) | ForEach-Object {
    if (-not (Test-Path $_)) {
        New-Item -ItemType Directory -Path $_ -Force | Out-Null
        Write-Info "Directorio creado: $_"
    }
}

function Test-DockerCompose {
    Write-Info "Verificando docker-compose..."
    try {
        $composeVersion = docker-compose --version
        Write-Success "Docker Compose encontrado: $composeVersion"
        return $true
    }
    catch {
        Write-Error "Docker Compose no está instalado o no está en el PATH"
        return $false
    }
}

function Build-Services {
    Write-Info "Construyendo servicios con docker-compose..."
    
    try {
        Set-Location $ProjectRoot
        docker-compose build pipeline
        
        if ($LASTEXITCODE -eq 0) {
            Write-Success "Servicios construidos exitosamente"
            return $true
        } else {
            Write-Error "Error construyendo servicios"
            return $false
        }
    }
    catch {
        Write-Error "Error ejecutando docker-compose build: $_"
        return $false
    }
}

function Start-Dependencies {
    Write-Info "Iniciando servicios de dependencias..."
    
    try {
        Set-Location $ProjectRoot
        
        # Iniciar servicios de infraestructura
        $services = @("minio", "spark-master", "spark-worker-1")
        
        foreach ($service in $services) {
            Write-Info "Iniciando $service..."
            docker-compose up -d $service
            
            if ($LASTEXITCODE -ne 0) {
                Write-Error "Error iniciando $service"
                return $false
            }
        }
        
        # Esperar a que los servicios estén listos
        Write-Info "Esperando a que los servicios estén listos..."
        Start-Sleep -Seconds 30
        
        Write-Success "Servicios de dependencias iniciados"
        return $true
    }
    catch {
        Write-Error "Error iniciando dependencias: $_"
        return $false
    }
}

function Run-Pipeline {
    Write-Info "Ejecutando pipeline..."
    
    # Preparar rutas
    $inputPath = "/app/input/$InputFile"
    $outputPath = "/app/output/$OutputFile"
    
    # Verificar archivo de entrada
    $localInputPath = Join-Path $ProjectRoot "test_data" $InputFile
    if (-not (Test-Path $localInputPath)) {
        Write-Error "Archivo de entrada no encontrado: $localInputPath"
        return $false
    }
    
    try {
        Set-Location $ProjectRoot
        
        # Construir comando del pipeline
        $pipelineArgs = @($inputPath, $outputPath, "--log-level", $LogLevel)
        
        if ($TestMode) {
            $pipelineArgs += "--test-mode"
        }
        
        # Ejecutar pipeline usando docker-compose run
        $dockerComposeCmd = @(
            "docker-compose", "run", "--rm",
            "--name", "mvp-pipeline-execution",
            "pipeline"
        ) + $pipelineArgs
        
        Write-Info "Ejecutando: $($dockerComposeCmd -join ' ')"
        
        & $dockerComposeCmd[0] $dockerComposeCmd[1..($dockerComposeCmd.Length-1)]
        
        if ($LASTEXITCODE -eq 0) {
            Write-Success "Pipeline ejecutado exitosamente"
            
            # Verificar archivo de salida
            $localOutputPath = Join-Path $TestOutputDir $OutputFile
            if (Test-Path $localOutputPath) {
                Write-Success "Archivo de salida generado: $localOutputPath"
                
                # Mostrar información del archivo
                $fileInfo = Get-Item $localOutputPath
                Write-Info "Tamaño del archivo: $($fileInfo.Length) bytes"
                Write-Info "Fecha de creación: $($fileInfo.CreationTime)"
                
                return $true
            } else {
                Write-Warning "Pipeline exitoso pero archivo de salida no encontrado"
                return $false
            }
        } else {
            Write-Error "Pipeline falló con código: $LASTEXITCODE"
            return $false
        }
    }
    catch {
        Write-Error "Error ejecutando pipeline: $_"
        return $false
    }
}

function Stop-Services {
    Write-Info "Deteniendo servicios..."
    
    try {
        Set-Location $ProjectRoot
        docker-compose down
        
        Write-Success "Servicios detenidos"
    }
    catch {
        Write-Warning "Error deteniendo servicios: $_"
    }
}

function Show-Logs {
    Write-Info "Mostrando logs del pipeline..."
    
    $logFile = Join-Path $LogsDir "pipeline.log"
    if (Test-Path $logFile) {
        Write-Info "Últimas 20 líneas del log:"
        Get-Content $logFile -Tail 20 | ForEach-Object { Write-Host $_ }
    } else {
        Write-Warning "Archivo de log no encontrado: $logFile"
    }
}

# Función principal
function Main {
    Write-Info "🚀 Ejecutando pipeline con docker-compose"
    Write-Info "Archivo de entrada: $InputFile"
    Write-Info "Archivo de salida: $OutputFile"
    Write-Info "Nivel de log: $LogLevel"
    
    if ($TestMode) {
        Write-Info "Modo de prueba: Activado"
    }
    
    # Verificar docker-compose
    if (-not (Test-DockerCompose)) {
        exit 1
    }
    
    try {
        # Construir imagen si se solicita
        if ($BuildImage) {
            if (-not (Build-Services)) {
                exit 1
            }
        }
        
        # Iniciar dependencias
        if (-not (Start-Dependencies)) {
            exit 1
        }
        
        # Ejecutar pipeline
        $success = Run-Pipeline
        
        # Mostrar logs
        Show-Logs
        
        if ($success) {
            Write-Success "🎉 Pipeline completado exitosamente!"
            exit 0
        } else {
            Write-Error "❌ Pipeline falló"
            exit 1
        }
    }
    finally {
        # Limpiar si se solicita
        if ($CleanUp) {
            Stop-Services
        }
    }
}

# Ejecutar función principal
Main