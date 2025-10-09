"""
Extended Spark Pipeline with Database Integration
Supports dynamic table creation from schema.json and Silver -> Gold flow
"""

import sys, os, yaml, json
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# Import database modules
from database.db_manager import DatabaseManager, create_database_manager_from_file
from database.schema_mapper import DatabaseEngine

# Importar funciones comunes
from common import norm_type, parse_order, safe_cast, maybe_config_s3a

def load_json_schema(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def spark_type_from_json(t):
    return {
        "string": "string", "number": "double", "integer": "int",
        "boolean": "boolean", "array": "array<string>", "object": "struct<>"
    }.get(t, "string")

def enforce_schema(df, jsch, mode="strict"):
    props = jsch.get("properties", {})
    req = set(jsch.get("required", []))
    
    for col, spec in props.items():
        json_type = spec.get("type")
        if isinstance(json_type, list):
            json_type = next((t for t in json_type if t != "null"), "string")
        
        spark_type = spark_type_from_json(json_type)
        
        if col in df.columns:
            if json_type == "string" and spec.get("format") == "date-time":
                df = df.withColumn(col, F.to_timestamp(F.col(col)))
            else:
                df = df.withColumn(col, F.col(col).cast(spark_type))
        elif col in req and mode == "strict":
            raise ValueError(f"Columna requerida '{col}' no encontrada")
        elif col in req:
            df = df.withColumn(col, F.lit(None).cast(spark_type))
    
    return df

def load_expectations(path):
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def apply_quality(df, rules, quarantine_path, run_id):
    stats = {"total": df.count(), "passed": 0, "quarantined": 0, "dropped": 0}
    quarantined_rows = []
    
    for rule in rules:
        rule_name = rule.get("name", "unnamed")
        condition = rule.get("condition")
        on_fail = rule.get("on_fail", "warn")
        
        if not condition:
            continue
        
        try:
            valid_df = df.filter(F.expr(condition))
            invalid_df = df.filter(~F.expr(condition))
            invalid_count = invalid_df.count()
            
            if invalid_count > 0:
                print(f"[quality] Rule '{rule_name}' failed for {invalid_count} rows")
                
                if on_fail == "quarantine" and quarantine_path:
                    invalid_with_meta = invalid_df.withColumn("_rule_failed", F.lit(rule_name)) \
                                                   .withColumn("_quarantine_ts", F.current_timestamp()) \
                                                   .withColumn("_run_id", F.lit(run_id))
                    quarantined_rows.append(invalid_with_meta)
                    stats["quarantined"] += invalid_count
                    df = valid_df
                elif on_fail == "drop":
                    df = valid_df
                    stats["dropped"] += invalid_count
                elif on_fail == "warn":
                    print(f"[quality] WARNING: {invalid_count} rows failed rule '{rule_name}'")
                else:
                    raise ValueError(f"Rule '{rule_name}' failed for {invalid_count} rows")
            
        except Exception as e:
            print(f"[quality] Error applying rule '{rule_name}': {e}")
            if on_fail not in ["warn", "ignore"]:
                raise
    
    # Save quarantined data
    if quarantined_rows and quarantine_path:
        quarantine_df = quarantined_rows[0]
        for qdf in quarantined_rows[1:]:
            quarantine_df = quarantine_df.union(qdf)
        
        quarantine_df.write.mode("append").parquet(quarantine_path)
        print(f"[quality] Quarantined {stats['quarantined']} rows to {quarantine_path}")
    
    stats["passed"] = df.count()
    return df, quarantined_rows, stats

def load_database_config(db_config_path: str, environment: str = "default"):
    """Load database configuration and return DatabaseManager instance"""
    return create_database_manager_from_file(db_config_path, environment)

def create_gold_table_name(dataset_id: str, table_settings: dict) -> str:
    """Generate table name based on dataset ID and settings"""
    prefix = table_settings.get("table_prefix", "")
    suffix = table_settings.get("table_suffix", "")
    return f"{prefix}{dataset_id}{suffix}".strip("_")

def apply_business_rules(df, rules):
    """
    Aplica reglas de negocio al DataFrame
    
    Args:
        df: DataFrame de PySpark
        rules: Lista de reglas a aplicar
        
    Returns:
        DataFrame filtrado según reglas
    """
    if not rules:
        return df
        
    for rule in rules:
        if rule.get('action') == 'filter':
            condition = rule.get('condition')
            if condition:
                initial_count = df.count()
                df = df.filter(F.expr(condition))
                final_count = df.count()
                print(f"[gold] Applied business rule '{condition}': {initial_count} -> {final_count} rows")
            else:
                print(f"[gold] Warning: Business rule missing condition: {rule}")
                
    return df

def apply_gold_transformations(df, gold_config: dict, table_settings: dict):
    """
    Aplica transformaciones dinámicas al DataFrame según configuración
    
    Args:
        df: DataFrame de PySpark a transformar
        gold_config: Dict con configuraciones de transformación del gold layer
        table_settings: Dict con configuraciones globales de tabla
        
    Returns:
        DataFrame transformado
    """
    
    # Get exclude columns from dataset config or default from table_settings
    exclude_columns = gold_config.get('exclude_columns', 
                                    table_settings.get('default_transformations', {}).get('exclude_columns', []))
    
    # Exclusión de columnas mejorada
    if exclude_columns:
        existing_exclude_cols = [col for col in exclude_columns if col in df.columns]
        if existing_exclude_cols:
            df = df.drop(*existing_exclude_cols)
            print(f"[gold] Excluded columns: {existing_exclude_cols}")
    
    # Adición de columnas
    add_cols = gold_config.get('add_columns', [])
    for col_def in add_cols:
        if isinstance(col_def, dict) and 'name' in col_def and 'value' in col_def:
            col_name = col_def['name']
            col_value = col_def['value']
            col_type = col_def.get('type', 'string')
            
            df = df.withColumn(col_name, 
                             F.lit(col_value).cast(col_type))
            print(f"[gold] Added column '{col_name}' with value '{col_value}' as {col_type}")
    
    # Apply business rules using separate function
    business_rules = gold_config.get('business_rules', [])
    df = apply_business_rules(df, business_rules)
    
    return df

def write_to_gold_database(df, dataset_id: str, schema_path: str, db_manager, table_settings: dict):
    """Write DataFrame to Gold database with dynamic table creation"""
    try:
        # Create table name
        table_name = create_gold_table_name(dataset_id, table_settings)
        
        # Generate schema version
        schema_version = datetime.now().strftime(table_settings.get("version_format", "%Y%m%d_%H%M%S"))
        
        print(f"[gold] Creating/updating table '{table_name}' from schema '{schema_path}'")
        
        # Load schema dictionary from path
        schema_dict = load_json_schema(schema_path)
        
        # Create or update table schema
        success = db_manager.create_table_from_schema(
            table_name=table_name,
            schema_dict=schema_dict,
            schema_version=schema_version
        )
        
        if not success:
            raise Exception(f"Failed to create/update table schema for {table_name}")
        
        # Load JSON schema to get expected columns
        json_schema = load_json_schema(schema_path)
        expected_columns = []
        if 'properties' in json_schema:
            expected_columns = list(json_schema['properties'].keys())
        
        # Filter DataFrame to only include columns defined in the schema
        # This removes partition columns (year, month) and metadata columns (_run_id, _ingestion_ts)
        available_columns = [col for col in expected_columns if col in df.columns]
        if available_columns:
            df_filtered = df.select(*available_columns)
            print(f"[gold] Filtered DataFrame to schema columns: {available_columns}")
        else:
            df_filtered = df
            print(f"[gold] Warning: No schema columns found, using full DataFrame")
        
        # Write data to table
        write_mode = table_settings.get("default_write_mode", "append")
        success = db_manager.write_dataframe(df_filtered, table_name, write_mode)
        
        if success:
            print(f"[gold] Successfully wrote data to table '{table_name}' in {write_mode} mode")
        else:
            raise Exception(f"Failed to write data to table {table_name}")
        
        return True
        
    except Exception as e:
        print(f"[gold] Error writing to database: {e}")
        return False

def main():
    """Main pipeline execution"""
    if len(sys.argv) < 3:
        print("Usage: python spark_job_with_db.py <dataset_config> <env_config> [db_config] [environment]")
        sys.exit(1)
    
    cfg_path = sys.argv[1]
    env_path = sys.argv[2]
    db_config_path = sys.argv[3] if len(sys.argv) > 3 else "config/database.yml"
    environment = sys.argv[4] if len(sys.argv) > 4 else "default"
    
    # Load configurations
    cfg = yaml.safe_load(open(cfg_path, 'r', encoding='utf-8'))
    env = yaml.safe_load(open(env_path, 'r', encoding='utf-8'))
    
    # Override environment from gold config if specified
    gold_config = cfg.get('output', {}).get('gold', {})
    if gold_config.get('environment'):
        environment = gold_config['environment']
        print(f"[gold] Using environment from dataset config: {environment}")
    
    # Load database configuration if file exists
    db_manager = None
    table_settings = {}
    execution_id = None
    
    if os.path.exists(db_config_path):
        db_full_config = yaml.safe_load(open(db_config_path, 'r', encoding='utf-8'))
        db_manager = load_database_config(db_config_path, environment)
        table_settings = db_full_config.get("table_settings", {})
        print(f"[gold] Database configuration loaded for environment: {environment}")
        
        # Log pipeline execution start
        try:
            execution_id = db_manager.log_pipeline_execution(
                dataset_name=cfg['id'],
                pipeline_type="etl",
                status="started"
            )
            print(f"[metadata] Pipeline execution started: {execution_id}")
        except Exception as e:
            print(f"[metadata] Warning: Failed to log pipeline start: {e}")
    else:
        print(f"[gold] Database config file not found: {db_config_path}. Skipping Gold layer.")
    
    try:
        # Initialize Spark
        spark = (
            SparkSession.builder
            .appName(f"cfg-pipeline::{cfg['id']}")
            .config("spark.sql.session.timeZone", env.get('timezone','UTC'))
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .getOrCreate()
        )
        
        # S3A autoconfig se maneja desde el módulo común
        
        # Configure source and output paths
        src = cfg['source']
        out = cfg['output']['silver']
        maybe_config_s3a(spark, src['path'], env)
        maybe_config_s3a(spark, out['path'], env)
        
        # Read data
        reader = spark.read.options(**src.get('options', {}))
        fmt = src['input_format']
        if fmt == 'csv':
            df = reader.csv(src['path'])
        elif fmt in ('json', 'jsonl'):
            df = reader.json(src['path'])
        elif fmt == 'parquet':
            df = reader.parquet(src['path'])
        else:
            raise ValueError(f"Formato no soportado: {fmt}")
        
        print(f"[source] Loaded {df.count()} rows from {src['path']}")
        
        # Apply standardization
        std = cfg.get('standardization', {})
        
        # Rename columns
        for r in std.get('rename', []) or []:
            if r['from'] in df.columns:
                df = df.withColumnRenamed(r['from'], r['to'])
        
        # Enforce schema if specified
        if 'schema' in cfg and cfg['schema'].get('ref'):
            jsch = load_json_schema(cfg['schema']['ref'])
            mode = cfg['schema'].get('mode','strict').lower()
            df = enforce_schema(df, jsch, mode=mode)
        
        # Apply type casts
        for c in std.get('casts', []) or []:
            df = safe_cast(df, c['column'], c['to'], format_hint=c.get('format_hint'), on_error=c.get('on_error','fail'))
        
        # Apply defaults
        for d in std.get('defaults', []) or []:
            col, val = d['column'], d['value']
            df = df.withColumn(col, F.when(F.col(col).isNull(), F.lit(val)).otherwise(F.col(col)))
        
        # Deduplicate
        if 'deduplicate' in std:
            key = std['deduplicate']['key']
            order = parse_order(std['deduplicate'].get('order_by', []))
            w = Window.partitionBy(*key).orderBy(*order) if order else Window.partitionBy(*key)
            df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn")==1).drop("_rn")
        
        # Add partitioning columns
        parts = out.get('partition_by', [])
        if parts:
            base_col_name = out.get('partition_from')
            base_col = None
            if base_col_name and base_col_name in df.columns:
                base_col = F.col(base_col_name)
            
            if base_col is not None:
                for p in parts:
                    if p not in df.columns:
                        lp = p.lower()
                        if lp == 'year':
                            df = df.withColumn('year', F.year(base_col))
                        elif lp == 'month':
                            df = df.withColumn('month', F.month(base_col))
                        elif lp == 'day':
                            df = df.withColumn('day', F.dayofmonth(base_col))
                        elif lp == 'date':
                            df = df.withColumn('date', F.to_date(base_col))
        
        # Add metadata columns
        run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%S%fZ")
        df = df.withColumn('_run_id', F.lit(run_id)).withColumn('_ingestion_ts', F.current_timestamp())
        
        # Apply quality checks
        if 'quality' in cfg and cfg['quality'].get('expectations_ref'):
            q = load_expectations(cfg['quality']['expectations_ref'])
            rules = q.get('rules', [])
            quarantine_path = cfg['quality'].get('quarantine')
            maybe_config_s3a(spark, quarantine_path or "", env)
            df, _, stats = apply_quality(df, rules, quarantine_path, run_id)
            print("[quality] stats:", stats)
        
        # Show final schema and count
        df.printSchema()
        final_count = df.count()
        print(f"[silver] Final row count: {final_count}")
        
        # Write to Silver layer (S3/MinIO)
        writer = (
            df.write
            .format(out.get('format','parquet'))
            .option('mergeSchema', str(out.get('merge_schema', True)).lower())
        )
        
        if parts:
            writer = writer.partitionBy(*parts)
        
        mode_cfg = (out.get('mode','append') or 'append').lower()
        if mode_cfg == 'overwrite_dynamic':
            writer.mode('overwrite').save(out['path'])
        else:
            writer.mode('append').save(out['path'])
        
        print(f"[silver] Successfully wrote to {out['path']}")
        
        # Log dataset version for Silver layer
        if db_manager:
            try:
                silver_version = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                db_manager.log_dataset_version(
                    dataset_name=f"{cfg['id']}_silver",
                    version=silver_version,
                    schema_path=cfg.get('schema', {}).get('ref'),
                    record_count=final_count
                )
                print(f"[metadata] Silver dataset version logged: {silver_version}")
            except Exception as e:
                print(f"[metadata] Warning: Failed to log silver dataset version: {e}")
        
        # Write to Gold layer (Database) if configured
        gold_config = cfg.get('output', {}).get('gold', {})
        
        # Verificación mejorada de 'enabled'
        if not gold_config.get('enabled', False):
            print("[gold] Capa Gold deshabilitada, omitiendo...")
            print("[pipeline] Pipeline completado exitosamente (solo Silver layer)")
            sys.exit(0)  # Salida limpia del proceso
        
        if (db_manager and 
            'schema' in cfg and cfg['schema'].get('ref')):
            
            schema_path = cfg['schema']['ref']
            dataset_id = cfg['id']
            
            print(f"[gold] Starting Gold layer processing for dataset: {dataset_id}")
            
            # Apply transformations (exclude columns)
            gold_df = apply_gold_transformations(df, gold_config, table_settings)
            
            success = write_to_gold_database(
                df=gold_df,
                dataset_id=dataset_id,
                schema_path=schema_path,
                db_manager=db_manager,
                table_settings=table_settings
            )
            
            if success:
                # Log dataset version for Gold layer
                try:
                    gold_version = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                    db_manager.log_dataset_version(
                        dataset_name=f"{dataset_id}_gold",
                        version=gold_version,
                        schema_path=schema_path,
                        record_count=gold_df.count()
                    )
                    print(f"[metadata] Gold dataset version logged: {gold_version}")
                except Exception as e:
                    print(f"[metadata] Warning: Failed to log gold dataset version: {e}")
                
                # Log successful pipeline completion
                if execution_id:
                    try:
                        db_manager.log_pipeline_execution(
                            dataset_name=cfg['id'],
                            status="completed",
                            execution_id=execution_id
                        )
                        print(f"[metadata] Pipeline execution completed: {execution_id}")
                    except Exception as e:
                        print(f"[metadata] Warning: Failed to log pipeline completion: {e}")
                
                print(f"[gold] Pipeline completed successfully!")
            else:
                # Log failed pipeline execution
                if execution_id:
                    try:
                        db_manager.log_pipeline_execution(
                            dataset_name=cfg['id'],
                            status="failed",
                            error_message="Gold layer processing failed",
                            execution_id=execution_id
                        )
                        print(f"[metadata] Pipeline execution failed: {execution_id}")
                    except Exception as e:
                        print(f"[metadata] Warning: Failed to log pipeline failure: {e}")
                
                print(f"[gold] Pipeline completed with Gold layer errors")
        else:
            # Log successful pipeline completion (Silver only)
            if execution_id:
                try:
                    db_manager.log_pipeline_execution(
                        dataset_name=cfg['id'],
                        status="completed",
                        execution_id=execution_id
                    )
                    print(f"[metadata] Pipeline execution completed: {execution_id}")
                except Exception as e:
                    print(f"[metadata] Warning: Failed to log pipeline completion: {e}")
            
            print(f"[pipeline] Silver layer processing completed (Gold layer not configured)")
        
        spark.stop()
        
    except Exception as e:
        # Log failed pipeline execution
        if execution_id and db_manager:
            try:
                db_manager.log_pipeline_execution(
                    dataset_name=cfg['id'],
                    status="failed",
                    error_message=str(e),
                    execution_id=execution_id
                )
                print(f"[metadata] Pipeline execution failed: {execution_id}")
            except Exception as log_error:
                print(f"[metadata] Warning: Failed to log pipeline failure: {log_error}")
        
        print(f"[pipeline] Pipeline failed with error: {e}")
        raise

if __name__ == "__main__":
    main()