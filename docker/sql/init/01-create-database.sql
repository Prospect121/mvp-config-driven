-- Crear base de datos para el MVP
USE master;
GO

-- Crear la base de datos si no existe
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'mvp_config_driven')
BEGIN
    CREATE DATABASE mvp_config_driven;
END
GO

USE mvp_config_driven;
GO

-- Crear esquemas
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'raw')
BEGIN
    EXEC('CREATE SCHEMA raw');
END
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze');
END
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
END
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
END
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'metadata')
BEGIN
    EXEC('CREATE SCHEMA metadata');
END
GO

-- Tabla de metadatos para tracking de pipelines
CREATE TABLE metadata.pipeline_runs (
    run_id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    pipeline_name NVARCHAR(255) NOT NULL,
    dataset_name NVARCHAR(255) NOT NULL,
    start_time DATETIME2 DEFAULT GETUTCDATE(),
    end_time DATETIME2 NULL,
    status NVARCHAR(50) NOT NULL DEFAULT 'RUNNING',
    records_processed BIGINT NULL,
    records_failed BIGINT NULL,
    error_message NVARCHAR(MAX) NULL,
    config_version NVARCHAR(50) NULL,
    created_at DATETIME2 DEFAULT GETUTCDATE(),
    updated_at DATETIME2 DEFAULT GETUTCDATE()
);
GO

-- Tabla de calidad de datos
CREATE TABLE metadata.data_quality_results (
    id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    run_id UNIQUEIDENTIFIER NOT NULL,
    rule_name NVARCHAR(255) NOT NULL,
    column_name NVARCHAR(255) NULL,
    expectation_type NVARCHAR(100) NOT NULL,
    success BIT NOT NULL,
    result_value DECIMAL(18,6) NULL,
    threshold_value DECIMAL(18,6) NULL,
    failed_records BIGINT NULL,
    total_records BIGINT NULL,
    created_at DATETIME2 DEFAULT GETUTCDATE(),
    FOREIGN KEY (run_id) REFERENCES metadata.pipeline_runs(run_id)
);
GO

-- Tabla de ejemplo para datos de pagos (gold layer)
CREATE TABLE gold.payments_summary (
    id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    payment_date DATE NOT NULL,
    payment_method NVARCHAR(50) NOT NULL,
    currency NVARCHAR(10) NOT NULL,
    total_amount DECIMAL(18,2) NOT NULL,
    transaction_count BIGINT NOT NULL,
    avg_amount DECIMAL(18,2) NOT NULL,
    min_amount DECIMAL(18,2) NOT NULL,
    max_amount DECIMAL(18,2) NOT NULL,
    created_at DATETIME2 DEFAULT GETUTCDATE(),
    updated_at DATETIME2 DEFAULT GETUTCDATE()
);
GO

-- Índices para optimización
CREATE INDEX IX_pipeline_runs_pipeline_dataset ON metadata.pipeline_runs(pipeline_name, dataset_name);
CREATE INDEX IX_pipeline_runs_start_time ON metadata.pipeline_runs(start_time);
CREATE INDEX IX_data_quality_results_run_id ON metadata.data_quality_results(run_id);
CREATE INDEX IX_payments_summary_date_method ON gold.payments_summary(payment_date, payment_method);
GO

-- Insertar datos de ejemplo
INSERT INTO metadata.pipeline_runs (pipeline_name, dataset_name, status, config_version)
VALUES 
    ('spark_pipeline', 'finanzas', 'COMPLETED', '1.0.0'),
    ('spark_pipeline', 'payments', 'COMPLETED', '1.0.0');
GO

PRINT 'Base de datos MVP Config Driven creada exitosamente';
GO