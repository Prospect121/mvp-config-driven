#!/bin/bash

# =============================================================================
# MVP Config-Driven Data Pipeline - Development Environment Setup
# =============================================================================
# This script sets up the complete development environment for the project

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_NAME="mvp-config-driven"
PYTHON_VERSION="3.9"
VENV_NAME="venv"

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

print_header() {
    echo -e "\n${BLUE}==============================================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}==============================================================================${NC}\n"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ $1${NC}"
}

check_command() {
    if command -v "$1" &> /dev/null; then
        print_success "$1 is installed"
        return 0
    else
        print_error "$1 is not installed"
        return 1
    fi
}

# =============================================================================
# PREREQUISITE CHECKS
# =============================================================================

check_prerequisites() {
    print_header "Checking Prerequisites"
    
    local all_good=true
    
    # Check Python
    if command -v python3 &> /dev/null; then
        local python_version=$(python3 --version | cut -d' ' -f2)
        print_success "Python $python_version is installed"
    else
        print_error "Python 3 is not installed"
        all_good=false
    fi
    
    # Check Docker
    if ! check_command "docker"; then
        all_good=false
    fi
    
    # Check Docker Compose
    if ! check_command "docker-compose"; then
        all_good=false
    fi
    
    # Check Git
    if ! check_command "git"; then
        all_good=false
    fi
    
    # Check Make (optional)
    if ! check_command "make"; then
        print_warning "Make is not installed (optional)"
    fi
    
    if [ "$all_good" = false ]; then
        print_error "Some prerequisites are missing. Please install them before continuing."
        exit 1
    fi
    
    print_success "All prerequisites are satisfied"
}

# =============================================================================
# PYTHON ENVIRONMENT SETUP
# =============================================================================

setup_python_environment() {
    print_header "Setting up Python Environment"
    
    # Create virtual environment if it doesn't exist
    if [ ! -d "$VENV_NAME" ]; then
        print_info "Creating Python virtual environment..."
        python3 -m venv $VENV_NAME
        print_success "Virtual environment created"
    else
        print_info "Virtual environment already exists"
    fi
    
    # Activate virtual environment
    print_info "Activating virtual environment..."
    source $VENV_NAME/bin/activate
    
    # Upgrade pip
    print_info "Upgrading pip..."
    pip install --upgrade pip
    
    # Install requirements
    if [ -f "requirements.txt" ]; then
        print_info "Installing Python dependencies..."
        pip install -r requirements.txt
        print_success "Python dependencies installed"
    else
        print_warning "requirements.txt not found"
    fi
    
    # Install development dependencies
    if [ -f "requirements-dev.txt" ]; then
        print_info "Installing development dependencies..."
        pip install -r requirements-dev.txt
        print_success "Development dependencies installed"
    fi
}

# =============================================================================
# ENVIRONMENT CONFIGURATION
# =============================================================================

setup_environment_files() {
    print_header "Setting up Environment Configuration"
    
    # Copy .env.example to .env if it doesn't exist
    if [ ! -f ".env" ]; then
        if [ -f ".env.example" ]; then
            print_info "Creating .env file from .env.example..."
            cp .env.example .env
            print_success ".env file created"
            print_warning "Please review and update .env file with your specific configuration"
        else
            print_warning ".env.example not found"
        fi
    else
        print_info ".env file already exists"
    fi
    
    # Copy terraform.tfvars.example if needed
    if [ -f "terraform/terraform.tfvars.example" ] && [ ! -f "terraform/terraform.tfvars" ]; then
        print_info "Creating terraform.tfvars from example..."
        cp terraform/terraform.tfvars.example terraform/terraform.tfvars
        print_success "terraform.tfvars created"
        print_warning "Please review and update terraform/terraform.tfvars for your Azure deployment"
    fi
}

# =============================================================================
# DOCKER ENVIRONMENT SETUP
# =============================================================================

setup_docker_environment() {
    print_header "Setting up Docker Environment"
    
    # Check if Docker is running
    if ! docker info &> /dev/null; then
        print_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi
    
    print_success "Docker is running"
    
    # Create necessary directories
    print_info "Creating necessary directories..."
    mkdir -p data/raw data/silver data/gold data/quarantine
    mkdir -p logs
    mkdir -p notebooks
    mkdir -p docker/sql/init
    mkdir -p docker/grafana/provisioning
    mkdir -p docker/prometheus
    
    # Create basic Prometheus configuration if it doesn't exist
    if [ ! -f "docker/prometheus/prometheus.yml" ]; then
        print_info "Creating basic Prometheus configuration..."
        cat > docker/prometheus/prometheus.yml << EOF
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
EOF
        print_success "Prometheus configuration created"
    fi
    
    print_success "Docker environment setup completed"
}

# =============================================================================
# DOCKER SERVICES STARTUP
# =============================================================================

start_docker_services() {
    print_header "Starting Docker Services"
    
    print_info "Starting infrastructure services..."
    docker-compose up -d minio sqlserver zookeeper kafka redis
    
    print_info "Waiting for services to be ready..."
    sleep 30
    
    print_info "Starting processing services..."
    docker-compose up -d spark-master spark-worker-1
    
    print_info "Starting monitoring services..."
    docker-compose up -d prometheus grafana
    
    print_info "Starting development tools..."
    docker-compose up -d kafka-ui jupyter
    
    print_success "All Docker services started"
    
    # Show service status
    print_info "Service status:"
    docker-compose ps
}

# =============================================================================
# INITIAL DATA SETUP
# =============================================================================

setup_initial_data() {
    print_header "Setting up Initial Data"
    
    # Wait for MinIO to be ready
    print_info "Waiting for MinIO to be ready..."
    sleep 10
    
    # Create sample data if it doesn't exist
    if [ ! -f "data/raw/sample_data.csv" ]; then
        print_info "Creating sample data..."
        cat > data/raw/sample_data.csv << EOF
id,name,age,city,salary,department
1,John Doe,30,New York,50000,Engineering
2,Jane Smith,25,Los Angeles,45000,Marketing
3,Bob Johnson,35,Chicago,55000,Engineering
4,Alice Brown,28,Houston,48000,Sales
5,Charlie Wilson,32,Phoenix,52000,Engineering
EOF
        print_success "Sample data created"
    fi
    
    # Create sample configuration if it doesn't exist
    if [ ! -f "config/sample_pipeline.yml" ]; then
        mkdir -p config
        print_info "Creating sample pipeline configuration..."
        cat > config/sample_pipeline.yml << EOF
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
EOF
        print_success "Sample pipeline configuration created"
    fi
}

# =============================================================================
# VALIDATION AND TESTING
# =============================================================================

run_validation_tests() {
    print_header "Running Validation Tests"
    
    # Activate virtual environment
    source $VENV_NAME/bin/activate
    
    # Run configuration validation
    if [ -f "ci/check_config.sh" ]; then
        print_info "Running configuration validation..."
        bash ci/check_config.sh
        print_success "Configuration validation completed"
    fi
    
    # Run unit tests
    if [ -d "tests" ]; then
        print_info "Running unit tests..."
        python -m pytest tests/ -v --tb=short
        print_success "Unit tests completed"
    fi
    
    # Run linting
    print_info "Running code linting..."
    if command -v flake8 &> /dev/null; then
        flake8 src/ --max-line-length=88 --extend-ignore=E203,W503
        print_success "Linting completed"
    fi
    
    # Run type checking
    if command -v mypy &> /dev/null; then
        print_info "Running type checking..."
        mypy src/ --ignore-missing-imports
        print_success "Type checking completed"
    fi
}

# =============================================================================
# INFORMATION DISPLAY
# =============================================================================

display_access_information() {
    print_header "Development Environment Access Information"
    
    echo -e "${GREEN}🎉 Development environment setup completed successfully!${NC}\n"
    
    echo -e "${BLUE}Service Access URLs:${NC}"
    echo -e "  • MinIO Console:     ${YELLOW}http://localhost:9001${NC} (minio/minio12345)"
    echo -e "  • Kafka UI:          ${YELLOW}http://localhost:8080${NC}"
    echo -e "  • Spark Master UI:   ${YELLOW}http://localhost:8081${NC}"
    echo -e "  • Spark Worker UI:   ${YELLOW}http://localhost:8082${NC}"
    echo -e "  • Jupyter Notebook:  ${YELLOW}http://localhost:8888${NC} (token: jupyter123)"
    echo -e "  • Grafana:           ${YELLOW}http://localhost:3000${NC} (admin/admin123)"
    echo -e "  • Prometheus:        ${YELLOW}http://localhost:9090${NC}"
    
    echo -e "\n${BLUE}Database Connections:${NC}"
    echo -e "  • SQL Server:        ${YELLOW}localhost:1433${NC} (sa/YourStrong!Passw0rd)"
    echo -e "  • Redis:             ${YELLOW}localhost:6379${NC} (password: redispassword)"
    
    echo -e "\n${BLUE}Next Steps:${NC}"
    echo -e "  1. Review and update ${YELLOW}.env${NC} file with your configuration"
    echo -e "  2. Run a sample pipeline: ${YELLOW}make run-sample${NC}"
    echo -e "  3. Check the documentation in ${YELLOW}docs/${NC} directory"
    echo -e "  4. For Azure deployment, update ${YELLOW}terraform/terraform.tfvars${NC}"
    
    echo -e "\n${BLUE}Useful Commands:${NC}"
    echo -e "  • Start services:    ${YELLOW}docker-compose up -d${NC}"
    echo -e "  • Stop services:     ${YELLOW}docker-compose down${NC}"
    echo -e "  • View logs:         ${YELLOW}docker-compose logs -f [service]${NC}"
    echo -e "  • Run tests:         ${YELLOW}python -m pytest tests/${NC}"
    echo -e "  • Activate venv:     ${YELLOW}source venv/bin/activate${NC}"
}

# =============================================================================
# MAIN EXECUTION
# =============================================================================

main() {
    print_header "MVP Config-Driven Data Pipeline - Development Setup"
    
    # Change to script directory
    cd "$(dirname "$0")/.."
    
    # Run setup steps
    check_prerequisites
    setup_python_environment
    setup_environment_files
    setup_docker_environment
    start_docker_services
    setup_initial_data
    
    # Optional validation (can be skipped if there are issues)
    if [ "${SKIP_VALIDATION:-false}" != "true" ]; then
        run_validation_tests || print_warning "Some validation tests failed, but setup continues"
    fi
    
    display_access_information
    
    print_success "Development environment setup completed!"
}

# =============================================================================
# SCRIPT EXECUTION
# =============================================================================

# Check if script is being sourced or executed
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi