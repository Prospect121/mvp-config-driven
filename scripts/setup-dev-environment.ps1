# =============================================================================
# MVP Config-Driven Data Pipeline - Development Environment Setup (Windows)
# =============================================================================
# This script sets up the complete development environment for the project on Windows

param(
    [switch]$SkipValidation = $false,
    [switch]$Help = $false
)

# Configuration
$ProjectName = "mvp-config-driven"
$PythonVersion = "3.9"
$VenvName = "venv"

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

function Write-Header {
    param([string]$Message)
    Write-Host ""
    Write-Host "==============================================================================" -ForegroundColor Blue
    Write-Host $Message -ForegroundColor Blue
    Write-Host "==============================================================================" -ForegroundColor Blue
    Write-Host ""
}

function Write-Success {
    param([string]$Message)
    Write-Host "✓ $Message" -ForegroundColor Green
}

function Write-Warning {
    param([string]$Message)
    Write-Host "⚠ $Message" -ForegroundColor Yellow
}

function Write-Error {
    param([string]$Message)
    Write-Host "✗ $Message" -ForegroundColor Red
}

function Write-Info {
    param([string]$Message)
    Write-Host "ℹ $Message" -ForegroundColor Cyan
}

function Test-Command {
    param([string]$Command)
    try {
        Get-Command $Command -ErrorAction Stop | Out-Null
        Write-Success "$Command is installed"
        return $true
    }
    catch {
        Write-Error "$Command is not installed"
        return $false
    }
}

# =============================================================================
# HELP FUNCTION
# =============================================================================

function Show-Help {
    Write-Host "MVP Config-Driven Data Pipeline - Development Setup" -ForegroundColor Blue
    Write-Host ""
    Write-Host "USAGE:" -ForegroundColor Yellow
    Write-Host "  .\setup-dev-environment.ps1 [OPTIONS]"
    Write-Host ""
    Write-Host "OPTIONS:" -ForegroundColor Yellow
    Write-Host "  -SkipValidation    Skip validation tests during setup"
    Write-Host "  -Help              Show this help message"
    Write-Host ""
    Write-Host "EXAMPLES:" -ForegroundColor Yellow
    Write-Host "  .\setup-dev-environment.ps1"
    Write-Host "  .\setup-dev-environment.ps1 -SkipValidation"
    Write-Host ""
}

# =============================================================================
# PREREQUISITE CHECKS
# =============================================================================

function Test-Prerequisites {
    Write-Header "Checking Prerequisites"
    
    $allGood = $true
    
    # Check Python
    if (Test-Command "python") {
        $pythonVersion = python --version 2>&1
        Write-Success "Python $pythonVersion is installed"
    }
    elseif (Test-Command "python3") {
        $pythonVersion = python3 --version 2>&1
        Write-Success "Python $pythonVersion is installed"
    }
    else {
        Write-Error "Python is not installed"
        $allGood = $false
    }
    
    # Check Docker
    if (-not (Test-Command "docker")) {
        $allGood = $false
    }
    
    # Check Docker Compose
    if (-not (Test-Command "docker-compose")) {
        $allGood = $false
    }
    
    # Check Git
    if (-not (Test-Command "git")) {
        $allGood = $false
    }
    
    # Check PowerShell version
    if ($PSVersionTable.PSVersion.Major -lt 5) {
        Write-Warning "PowerShell 5.0 or higher is recommended"
    }
    
    if (-not $allGood) {
        Write-Error "Some prerequisites are missing. Please install them before continuing."
        Write-Host ""
        Write-Host "Required installations:" -ForegroundColor Yellow
        Write-Host "  • Python 3.9+: https://www.python.org/downloads/"
        Write-Host "  • Docker Desktop: https://www.docker.com/products/docker-desktop"
        Write-Host "  • Git: https://git-scm.com/download/win"
        exit 1
    }
    
    Write-Success "All prerequisites are satisfied"
}

# =============================================================================
# PYTHON ENVIRONMENT SETUP
# =============================================================================

function Set-PythonEnvironment {
    Write-Header "Setting up Python Environment"
    
    # Determine Python command
    $pythonCmd = "python"
    if (-not (Get-Command "python" -ErrorAction SilentlyContinue)) {
        $pythonCmd = "python3"
    }
    
    # Create virtual environment if it doesn't exist
    if (-not (Test-Path $VenvName)) {
        Write-Info "Creating Python virtual environment..."
        & $pythonCmd -m venv $VenvName
        Write-Success "Virtual environment created"
    }
    else {
        Write-Info "Virtual environment already exists"
    }
    
    # Activate virtual environment
    Write-Info "Activating virtual environment..."
    $activateScript = Join-Path $VenvName "Scripts\Activate.ps1"
    if (Test-Path $activateScript) {
        & $activateScript
    }
    else {
        Write-Warning "Could not find activation script at $activateScript"
    }
    
    # Upgrade pip
    Write-Info "Upgrading pip..."
    & python -m pip install --upgrade pip
    
    # Install requirements
    if (Test-Path "requirements.txt") {
        Write-Info "Installing Python dependencies..."
        & python -m pip install -r requirements.txt
        Write-Success "Python dependencies installed"
    }
    else {
        Write-Warning "requirements.txt not found"
    }
    
    # Install development dependencies
    if (Test-Path "requirements-dev.txt") {
        Write-Info "Installing development dependencies..."
        & python -m pip install -r requirements-dev.txt
        Write-Success "Development dependencies installed"
    }
}

# =============================================================================
# ENVIRONMENT CONFIGURATION
# =============================================================================

function Set-EnvironmentFiles {
    Write-Header "Setting up Environment Configuration"
    
    # Copy .env.example to .env if it doesn't exist
    if (-not (Test-Path ".env")) {
        if (Test-Path ".env.example") {
            Write-Info "Creating .env file from .env.example..."
            Copy-Item ".env.example" ".env"
            Write-Success ".env file created"
            Write-Warning "Please review and update .env file with your specific configuration"
        }
        else {
            Write-Warning ".env.example not found"
        }
    }
    else {
        Write-Info ".env file already exists"
    }
    
    # Copy terraform.tfvars.example if needed
    if ((Test-Path "terraform\terraform.tfvars.example") -and (-not (Test-Path "terraform\terraform.tfvars"))) {
        Write-Info "Creating terraform.tfvars from example..."
        Copy-Item "terraform\terraform.tfvars.example" "terraform\terraform.tfvars"
        Write-Success "terraform.tfvars created"
        Write-Warning "Please review and update terraform\terraform.tfvars for your Azure deployment"
    }
}

# =============================================================================
# DOCKER ENVIRONMENT SETUP
# =============================================================================

function Set-DockerEnvironment {
    Write-Header "Setting up Docker Environment"
    
    # Check if Docker is running
    try {
        docker info | Out-Null
        Write-Success "Docker is running"
    }
    catch {
        Write-Error "Docker is not running. Please start Docker Desktop and try again."
        exit 1
    }
    
    # Create necessary directories
    Write-Info "Creating necessary directories..."
    $directories = @(
        "data\raw", "data\silver", "data\gold", "data\quarantine",
        "logs", "notebooks",
        "docker\sql\init", "docker\grafana\provisioning", "docker\prometheus"
    )
    
    foreach ($dir in $directories) {
        if (-not (Test-Path $dir)) {
            New-Item -ItemType Directory -Path $dir -Force | Out-Null
        }
    }
    
    # Create basic Prometheus configuration if it doesn't exist
    if (-not (Test-Path "docker\prometheus\prometheus.yml")) {
        Write-Info "Creating basic Prometheus configuration..."
        $prometheusConfig = @"
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']
  
  - job_name: 'spark-master'
    static_configs:
      - targets: ['spark-master:4040']
    
  - job_name: 'spark-worker'
    static_configs:
      - targets: ['spark-worker-1:8081']
"@
        $prometheusConfig | Out-File -FilePath "docker\prometheus\prometheus.yml" -Encoding UTF8
        Write-Success "Prometheus configuration created"
    }
    
    Write-Success "Docker environment setup completed"
}

# =============================================================================
# DOCKER SERVICES STARTUP
# =============================================================================

function Start-DockerServices {
    Write-Header "Starting Docker Services"
    
    Write-Info "Starting infrastructure services..."
    docker-compose up -d minio sqlserver zookeeper kafka redis
    
    Write-Info "Waiting for services to be ready..."
    Start-Sleep -Seconds 30
    
    Write-Info "Starting processing services..."
    docker-compose up -d spark-master spark-worker-1
    
    Write-Info "Starting monitoring services..."
    docker-compose up -d prometheus grafana
    
    Write-Info "Starting development tools..."
    docker-compose up -d kafka-ui jupyter
    
    Write-Success "All Docker services started"
    
    # Show service status
    Write-Info "Service status:"
    docker-compose ps
}

# =============================================================================
# INITIAL DATA SETUP
# =============================================================================

function Set-InitialData {
    Write-Header "Setting up Initial Data"
    
    # Wait for MinIO to be ready
    Write-Info "Waiting for MinIO to be ready..."
    Start-Sleep -Seconds 10
    
    # Create sample data if it doesn't exist
    if (-not (Test-Path "data\raw\sample_data.csv")) {
        Write-Info "Creating sample data..."
        $sampleData = @"
id,name,age,city,salary,department
1,John Doe,30,New York,50000,Engineering
2,Jane Smith,25,Los Angeles,45000,Marketing
3,Bob Johnson,35,Chicago,55000,Engineering
4,Alice Brown,28,Houston,48000,Sales
5,Charlie Wilson,32,Phoenix,52000,Engineering
"@
        $sampleData | Out-File -FilePath "data\raw\sample_data.csv" -Encoding UTF8
        Write-Success "Sample data created"
    }
    
    # Create sample configuration if it doesn't exist
    if (-not (Test-Path "config\sample_pipeline.yml")) {
        if (-not (Test-Path "config")) {
            New-Item -ItemType Directory -Path "config" -Force | Out-Null
        }
        Write-Info "Creating sample pipeline configuration..."
        $sampleConfig = @"
pipeline:
  name: "sample_data_processing"
  description: "Sample data processing pipeline"
  
source:
  type: "csv"
  path: "data/raw/sample_data.csv"
  options:
    header: true
    inferSchema: true

transformations:
  - type: "rename_column"
    from: "name"
    to: "full_name"
  
  - type: "cast_column"
    column: "salary"
    datatype: "double"
  
  - type: "add_column"
    name: "processed_date"
    value: "current_timestamp()"

destination:
  type: "parquet"
  path: "data/silver/sample_data_processed"
  mode: "overwrite"

quality_rules:
  - column: "age"
    rule: "not_null"
    action: "quarantine"
  
  - column: "salary"
    rule: "greater_than"
    value: 0
    action: "reject"
"@
        $sampleConfig | Out-File -FilePath "config\sample_pipeline.yml" -Encoding UTF8
        Write-Success "Sample pipeline configuration created"
    }
}

# =============================================================================
# VALIDATION AND TESTING
# =============================================================================

function Invoke-ValidationTests {
    Write-Header "Running Validation Tests"
    
    # Run configuration validation
    if (Test-Path "ci\check_config.sh") {
        Write-Info "Running configuration validation..."
        try {
            bash ci/check_config.sh
            Write-Success "Configuration validation completed"
        }
        catch {
            Write-Warning "Configuration validation failed (bash required)"
        }
    }
    
    # Run unit tests
    if (Test-Path "tests") {
        Write-Info "Running unit tests..."
        try {
            python -m pytest tests/ -v --tb=short
            Write-Success "Unit tests completed"
        }
        catch {
            Write-Warning "Some unit tests failed"
        }
    }
    
    # Run linting
    Write-Info "Running code linting..."
    try {
        flake8 src/ --max-line-length=88 --extend-ignore=E203,W503
        Write-Success "Linting completed"
    }
    catch {
        Write-Warning "Linting failed or flake8 not installed"
    }
    
    # Run type checking
    try {
        mypy src/ --ignore-missing-imports
        Write-Success "Type checking completed"
    }
    catch {
        Write-Warning "Type checking failed or mypy not installed"
    }
}

# =============================================================================
# INFORMATION DISPLAY
# =============================================================================

function Show-AccessInformation {
    Write-Header "Development Environment Access Information"
    
    Write-Host "🎉 Development environment setup completed successfully!" -ForegroundColor Green
    Write-Host ""
    
    Write-Host "Service Access URLs:" -ForegroundColor Blue
    Write-Host "  • MinIO Console:     " -NoNewline; Write-Host "http://localhost:9001" -ForegroundColor Yellow -NoNewline; Write-Host " (minio/minio12345)"
    Write-Host "  • Kafka UI:          " -NoNewline; Write-Host "http://localhost:8080" -ForegroundColor Yellow
    Write-Host "  • Spark Master UI:   " -NoNewline; Write-Host "http://localhost:8081" -ForegroundColor Yellow
    Write-Host "  • Spark Worker UI:   " -NoNewline; Write-Host "http://localhost:8082" -ForegroundColor Yellow
    Write-Host "  • Jupyter Notebook:  " -NoNewline; Write-Host "http://localhost:8888" -ForegroundColor Yellow -NoNewline; Write-Host " (token: jupyter123)"
    Write-Host "  • Grafana:           " -NoNewline; Write-Host "http://localhost:3000" -ForegroundColor Yellow -NoNewline; Write-Host " (admin/admin123)"
    Write-Host "  • Prometheus:        " -NoNewline; Write-Host "http://localhost:9090" -ForegroundColor Yellow
    
    Write-Host ""
    Write-Host "Database Connections:" -ForegroundColor Blue
    Write-Host "  • SQL Server:        " -NoNewline; Write-Host "localhost:1433" -ForegroundColor Yellow -NoNewline; Write-Host " (sa/YourStrong!Passw0rd)"
    Write-Host "  • Redis:             " -NoNewline; Write-Host "localhost:6379" -ForegroundColor Yellow -NoNewline; Write-Host " (password: redispassword)"
    
    Write-Host ""
    Write-Host "Next Steps:" -ForegroundColor Blue
    Write-Host "  1. Review and update " -NoNewline; Write-Host ".env" -ForegroundColor Yellow -NoNewline; Write-Host " file with your configuration"
    Write-Host "  2. Run a sample pipeline: " -NoNewline; Write-Host "make run-sample" -ForegroundColor Yellow
    Write-Host "  3. Check the documentation in " -NoNewline; Write-Host "docs\" -ForegroundColor Yellow -NoNewline; Write-Host " directory"
    Write-Host "  4. For Azure deployment, update " -NoNewline; Write-Host "terraform\terraform.tfvars" -ForegroundColor Yellow
    
    Write-Host ""
    Write-Host "Useful Commands:" -ForegroundColor Blue
    Write-Host "  • Start services:    " -NoNewline; Write-Host "docker-compose up -d" -ForegroundColor Yellow
    Write-Host "  • Stop services:     " -NoNewline; Write-Host "docker-compose down" -ForegroundColor Yellow
    Write-Host "  • View logs:         " -NoNewline; Write-Host "docker-compose logs -f [service]" -ForegroundColor Yellow
    Write-Host "  • Run tests:         " -NoNewline; Write-Host "python -m pytest tests\" -ForegroundColor Yellow
    Write-Host "  • Activate venv:     " -NoNewline; Write-Host ".\venv\Scripts\Activate.ps1" -ForegroundColor Yellow
}

# =============================================================================
# MAIN EXECUTION
# =============================================================================

function Main {
    if ($Help) {
        Show-Help
        return
    }
    
    Write-Header "MVP Config-Driven Data Pipeline - Development Setup"
    
    # Change to script directory
    $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
    $projectDir = Split-Path -Parent $scriptDir
    Set-Location $projectDir
    
    try {
        # Run setup steps
        Test-Prerequisites
        Set-PythonEnvironment
        Set-EnvironmentFiles
        Set-DockerEnvironment
        Start-DockerServices
        Set-InitialData
        
        # Optional validation (can be skipped if there are issues)
        if (-not $SkipValidation) {
            try {
                Invoke-ValidationTests
            }
            catch {
                Write-Warning "Some validation tests failed, but setup continues"
            }
        }
        
        Show-AccessInformation
        Write-Success "Development environment setup completed!"
    }
    catch {
        Write-Error "Setup failed: $($_.Exception.Message)"
        exit 1
    }
}

# =============================================================================
# SCRIPT EXECUTION
# =============================================================================

# Execute main function
Main