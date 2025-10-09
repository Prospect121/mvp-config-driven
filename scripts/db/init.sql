-- Inicialización de la base de datos PostgreSQL para MVP Config-Driven
-- Este script se ejecuta automáticamente cuando se crea el contenedor

-- Crear esquemas para organizar las tablas
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS metadata;

-- Crear tabla de metadatos para tracking de datasets
CREATE TABLE IF NOT EXISTS metadata.dataset_versions (
    id SERIAL PRIMARY KEY,
    dataset_name VARCHAR(255) NOT NULL,
    version VARCHAR(50) NOT NULL,
    schema_path VARCHAR(500),
    record_count INTEGER,
    file_size_bytes BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(dataset_name, version)
);

-- Crear tabla de metadatos para tracking de ejecuciones
CREATE TABLE IF NOT EXISTS metadata.pipeline_executions (
    id SERIAL PRIMARY KEY,
    dataset_name VARCHAR(255) NOT NULL,
    execution_id VARCHAR(255) NOT NULL,
    pipeline_type VARCHAR(50) NOT NULL DEFAULT 'etl',
    status VARCHAR(50) NOT NULL,
    records_processed INTEGER,
    records_inserted INTEGER,
    records_updated INTEGER,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    ended_at TIMESTAMP,
    error_message TEXT
);

-- Crear índices para mejorar performance
CREATE INDEX IF NOT EXISTS idx_dataset_versions_name ON metadata.dataset_versions(dataset_name);
CREATE INDEX IF NOT EXISTS idx_pipeline_executions_dataset ON metadata.pipeline_executions(dataset_name);
CREATE INDEX IF NOT EXISTS idx_pipeline_executions_status ON metadata.pipeline_executions(status);

-- Configurar timezone
SET timezone = 'America/Bogota';

-- Mensaje de confirmación
SELECT 'Database initialized successfully' AS status;