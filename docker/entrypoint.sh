#!/bin/bash
# Script de entrada optimizado para Azure Container Instances
# Archivo: docker/entrypoint.sh

set -e

# Función para logging
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

# Función para validar variables de entorno requeridas
validate_env() {
    local required_vars=("AZURE_CLIENT_ID" "AZURE_CLIENT_SECRET" "AZURE_TENANT_ID" "INPUT_PATH" "OUTPUT_PATH")
    
    for var in "${required_vars[@]}"; do
        if [ -z "${!var}" ]; then
            log "ERROR: Variable de entorno requerida no configurada: $var"
            exit 1
        fi
    done
}

# Función para configurar autenticación de Azure
setup_azure_auth() {
    log "Configurando autenticación de Azure..."
    
    # Verificar que las credenciales estén disponibles
    if [ -n "$AZURE_CLIENT_ID" ] && [ -n "$AZURE_CLIENT_SECRET" ] && [ -n "$AZURE_TENANT_ID" ]; then
        export AZURE_CLIENT_ID="$AZURE_CLIENT_ID"
        export AZURE_CLIENT_SECRET="$AZURE_CLIENT_SECRET"
        export AZURE_TENANT_ID="$AZURE_TENANT_ID"
        log "Credenciales de Azure configuradas correctamente"
    else
        log "ERROR: Credenciales de Azure no configuradas"
        exit 1
    fi
}

# Función principal
main() {
    log "Iniciando pipeline integrado en Azure Container Instance"
    log "Modo de procesamiento: ${PROCESSING_MODE:-batch}"
    log "Ruta de entrada: ${INPUT_PATH}"
    log "Ruta de salida: ${OUTPUT_PATH}"
    
    # Validar variables de entorno
    validate_env
    
    # Configurar autenticación
    setup_azure_auth
    
    # Verificar que los archivos de configuración existan
    if [ ! -f "$CONFIG_PATH" ]; then
        log "ERROR: Archivo de configuración no encontrado: $CONFIG_PATH"
        exit 1
    fi
    
    if [ ! -f "$EXPECTATIONS_PATH" ]; then
        log "ERROR: Archivo de expectativas no encontrado: $EXPECTATIONS_PATH"
        exit 1
    fi
    
    # Crear directorios de trabajo si no existen
    mkdir -p /app/logs /app/output
    
    # Ejecutar el pipeline con manejo de errores
    log "Ejecutando pipeline integrado..."
    
    if python /app/src/integrated_pipeline.py \
        --input-path "$INPUT_PATH" \
        --output-path "$OUTPUT_PATH" \
        --config-path "$CONFIG_PATH" \
        --expectations-path "$EXPECTATIONS_PATH" \
        --mode "$PROCESSING_MODE"; then
        
        log "Pipeline ejecutado exitosamente"
        exit 0
    else
        log "ERROR: Pipeline falló durante la ejecución"
        exit 1
    fi
}

# Ejecutar función principal
main "$@"