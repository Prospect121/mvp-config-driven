# Script de PowerShell para probar el pipeline Docker
# Uso: .\test_docker_pipeline.ps1

param(
    [string]$ImageName = "mvp-pipeline:test",
    [string]$TestMode = "all"
)

$ErrorActionPreference = "Stop"

# Colores para output
function Write-Success { param($Message) Write-Host "✅ $Message" -ForegroundColor Green }
function Write-Error { param($Message) Write-Host "❌ $Message" -ForegroundColor Red }
function Write-Info { param($Message) Write-Host "ℹ️  $Message" -ForegroundColor Blue }
function Write-Warning { param($Message) Write-Host "⚠️  $Message" -ForegroundColor Yellow }

# Obtener directorio del proyecto
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$DockerDir = Join-Path $ProjectRoot "docker"
$TestDataDir = Join-Path $ProjectRoot "test_data"
$TestOutputDir = Join-Path $ProjectRoot "test_output"

Write-Info "Directorio del proyecto: $ProjectRoot"

# Crear directorio de salida de pruebas
if (-not (Test-Path $TestOutputDir)) {
    New-Item -ItemType Directory -Path $TestOutputDir -Force | Out-Null
}

function Test-DockerInstallation {
    Write-Info "Verificando instalación de Docker..."
    try {
        $dockerVersion = docker --version
        Write-Success "Docker encontrado: $dockerVersion"
        return $true
    }
    catch {
        Write-Error "Docker no está instalado o no está en el PATH"
        return $false
    }
}

function Build-DockerImage {
    Write-Info "Construyendo imagen Docker: $ImageName"
    
    $dockerfilePath = Join-Path $DockerDir "Dockerfile.pipeline"
    
    if (-not (Test-Path $dockerfilePath)) {
        Write-Error "Dockerfile no encontrado en: $dockerfilePath"
        return $false
    }
    
    try {
        $buildCmd = "docker build -f `"$dockerfilePath`" -t $ImageName `"$ProjectRoot`""
        Write-Info "Ejecutando: $buildCmd"
        
        Invoke-Expression $buildCmd
        
        if ($LASTEXITCODE -eq 0) {
            Write-Success "Imagen construida exitosamente"
            return $true
        } else {
            Write-Error "Error construyendo la imagen (código: $LASTEXITCODE)"
            return $false
        }
    }
    catch {
        Write-Error "Error ejecutando docker build: $_"
        return $false
    }
}

function Test-BasicExecution {
    Write-Info "Ejecutando prueba básica del contenedor..."
    
    $inputFile = "test_data/sample_data.csv"
    $outputFile = "test_output/basic_test_output.parquet"
    
    # Verificar que existe el archivo de entrada
    $fullInputPath = Join-Path $ProjectRoot $inputFile
    if (-not (Test-Path $fullInputPath)) {
        Write-Error "Archivo de entrada no encontrado: $fullInputPath"
        return $false
    }
    
    try {
        $dockerCmd = @(
            "docker", "run", "--rm",
            "--name", "mvp-pipeline-test",
            "-v", "${ProjectRoot}:/app/host_data",
            $ImageName,
            "/app/host_data/$inputFile",
            "/app/host_data/$outputFile",
            "--test-mode",
            "--log-level", "INFO"
        )
        
        Write-Info "Ejecutando contenedor..."
        $result = & $dockerCmd[0] $dockerCmd[1..($dockerCmd.Length-1)]
        
        if ($LASTEXITCODE -eq 0) {
            Write-Success "Contenedor ejecutado exitosamente"
            
            # Verificar archivo de salida
            $fullOutputPath = Join-Path $ProjectRoot $outputFile
            if (Test-Path $fullOutputPath) {
                Write-Success "Archivo de salida generado: $outputFile"
                return $true
            } else {
                Write-Warning "Contenedor exitoso pero archivo de salida no encontrado"
                return $false
            }
        } else {
            Write-Error "Contenedor falló con código: $LASTEXITCODE"
            Write-Error "Salida: $result"
            return $false
        }
    }
    catch {
        Write-Error "Error ejecutando contenedor: $_"
        return $false
    }
}

function Test-CustomData {
    Write-Info "Ejecutando prueba con datos personalizados..."
    
    # Crear archivo CSV de prueba
    $testCsvPath = Join-Path $TestOutputDir "custom_test_data.csv"
    $testOutputPath = Join-Path $TestOutputDir "custom_test_output.parquet"
    
    $csvContent = @'
id,name,age,city
1,Alice,25,Madrid
2,Bob,30,Barcelona
3,Charlie,35,Valencia
4,Diana,28,Sevilla
5,Eve,32,Bilbao
'@
    
    $csvContent | Out-File -FilePath $testCsvPath -Encoding UTF8
    Write-Info "Archivo de prueba creado: $testCsvPath"
    
    try {
        $dockerCmd = @(
            "docker", "run", "--rm",
            "--name", "mvp-pipeline-test-custom",
            "-v", "${ProjectRoot}:/app/host_data",
            $ImageName,
            "/app/host_data/test_output/custom_test_data.csv",
            "/app/host_data/test_output/custom_test_output.parquet",
            "--log-level", "DEBUG"
        )
        
        $result = & $dockerCmd[0] $dockerCmd[1..($dockerCmd.Length-1)]
        
        if ($LASTEXITCODE -eq 0) {
            if (Test-Path $testOutputPath) {
                Write-Success "Prueba con datos personalizados exitosa"
                return $true
            } else {
                Write-Warning "Contenedor exitoso pero archivo de salida no encontrado"
                return $false
            }
        } else {
            Write-Error "Prueba con datos personalizados falló"
            return $false
        }
    }
    catch {
        Write-Error "Error en prueba con datos personalizados: $_"
        return $false
    }
}

function Test-ErrorHandling {
    Write-Info "Ejecutando prueba de manejo de errores..."
    
    try {
        $dockerCmd = @(
            "docker", "run", "--rm",
            "--name", "mvp-pipeline-test-error",
            "-v", "${ProjectRoot}:/app/host_data",
            $ImageName,
            "/app/host_data/nonexistent_file.csv",
            "/app/host_data/test_output/error_test.parquet",
            "--log-level", "ERROR"
        )
        
        $result = & $dockerCmd[0] $dockerCmd[1..($dockerCmd.Length-1)]
        
        # Debe fallar
        if ($LASTEXITCODE -ne 0) {
            Write-Success "Manejo de errores funciona correctamente (falló como esperado)"
            return $true
        } else {
            Write-Error "Debería haber fallado con archivo inexistente"
            return $false
        }
    }
    catch {
        Write-Success "Manejo de errores funciona correctamente (excepción como esperado)"
        return $true
    }
}

function Cleanup-TestResources {
    Write-Info "Limpiando recursos de prueba..."
    
    # Detener contenedores de prueba si están corriendo
    $containers = @("mvp-pipeline-test", "mvp-pipeline-test-custom", "mvp-pipeline-test-error")
    foreach ($container in $containers) {
        try {
            docker rm -f $container 2>$null
        }
        catch {
            # Ignorar errores si el contenedor no existe
        }
    }
    
    Write-Success "Limpieza completada"
}

# Función principal
function Main {
    Write-Info "🚀 Iniciando pruebas del pipeline Docker..."
    Write-Info "Imagen: $ImageName"
    Write-Info "Modo: $TestMode"
    
    # Verificar Docker
    if (-not (Test-DockerInstallation)) {
        exit 1
    }
    
    # Limpiar recursos previos
    Cleanup-TestResources
    
    # Construir imagen
    if (-not (Build-DockerImage)) {
        exit 1
    }
    
    # Ejecutar pruebas según el modo
    $tests = @()
    
    if ($TestMode -eq "all" -or $TestMode -eq "basic") {
        $tests += @{ Name = "Prueba básica"; Function = { Test-BasicExecution } }
    }
    
    if ($TestMode -eq "all" -or $TestMode -eq "custom") {
        $tests += @{ Name = "Datos personalizados"; Function = { Test-CustomData } }
    }
    
    if ($TestMode -eq "all" -or $TestMode -eq "error") {
        $tests += @{ Name = "Manejo de errores"; Function = { Test-ErrorHandling } }
    }
    
    # Ejecutar pruebas
    $results = @()
    foreach ($test in $tests) {
        Write-Info "`n--- $($test.Name) ---"
        try {
            $success = & $test.Function
            $results += @{ Name = $test.Name; Success = $success }
        }
        catch {
            Write-Error "Error en $($test.Name): $_"
            $results += @{ Name = $test.Name; Success = $false }
        }
        finally {
            Cleanup-TestResources
        }
    }
    
    # Resumen de resultados
    Write-Info "`n" + "="*50
    Write-Info "📊 RESUMEN DE PRUEBAS"
    Write-Info "="*50
    
    $passed = 0
    foreach ($result in $results) {
        if ($result.Success) {
            Write-Success "$($result.Name)"
            $passed++
        } else {
            Write-Error "$($result.Name)"
        }
    }
    
    Write-Info "`nResultado: $passed/$($results.Count) pruebas exitosas"
    
    if ($passed -eq $results.Count) {
        Write-Success "🎉 ¡Todas las pruebas pasaron!"
        exit 0
    } else {
        Write-Warning "⚠️  Algunas pruebas fallaron"
        exit 1
    }
}

# Ejecutar función principal
Main