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
from sources import load_source, flatten_json, sanitize_nulls, project_columns
from udf_catalog import get_udf

def load_json_schema(path):
    """Carga el schema desde archivo JSON o YAML y retorna un dict.

    Compatibilidad hacia atrás: mantiene el nombre de la función pero
    detecta automáticamente la extensión del archivo para usar el
    parser apropiado.
    """
    _, ext = os.path.splitext(path.lower())
    with open(path, 'r', encoding='utf-8') as f:
        if ext in ('.yml', '.yaml'):
            return yaml.safe_load(f)
        else:
            # Por defecto intentar JSON; si falla, intentar YAML
            try:
                return json.load(f)
            except Exception:
                f.seek(0)
                return yaml.safe_load(f)

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

def load_transforms(path):
    """Cargar archivo de transformaciones declarativas (YAML)."""
    try:
        with open(path, 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f) or {}
            return cfg
    except Exception as e:
        print(f"[transforms] Warning: No se pudo cargar '{path}': {e}")
        return {}

def apply_sql_transforms(df, transforms_cfg: dict):
    """
    Aplica transformaciones SQL declarativas definidas en transforms.yml

    Formato soportado:
    - transforms: [
        { target_column, expr, mode=create|replace, type?, on_error? }
      ]
    - sql: alias de 'transforms' (para compatibilidad)
    """
    if not transforms_cfg:
        return df

    transforms_list = transforms_cfg.get('transforms') or transforms_cfg.get('sql') or []
    if not transforms_list:
        return df

    default_on_error = (transforms_cfg.get('on_error') or '').lower() or None

    for t in transforms_list:
        if not isinstance(t, dict):
            print(f"[transforms] Ignorando entrada no soportada: {t}")
            continue

        target = t.get('target_column') or t.get('name')
        expr = t.get('expr')
        mode = (t.get('mode') or 'create').lower()
        cast_type = t.get('type')
        on_error = (t.get('on_error') or default_on_error)

        if not target or not expr:
            print(f"[transforms] Entrada inválida, falta target/expr: {t}")
            continue

        try:
            # Respetar el modo: create solo si no existe; replace siempre
            target_exists = target in df.columns
            if mode == 'create' and target_exists:
                print(f"[transforms] Skipped create for existing column '{target}'")
                continue

            df = df.withColumn(target, F.expr(expr))
            if cast_type:
                df = safe_cast(df, target, cast_type, on_error=on_error)
            print(f"[transforms] Applied expr to '{target}': {expr} type={cast_type or 'auto'}")
        except Exception as e:
            msg = f"[transforms] Error applying expr to '{target}': {e}"
            if (on_error or '').lower() == 'null':
                ttype = cast_type or 'string'
                df = df.withColumn(target, F.lit(None).cast(ttype))
                print(msg + f" -> set NULL ({ttype})")
            elif (on_error or '').lower() == 'skip':
                print(msg + " -> skipped")
            else:
                print(msg)
                raise

    return df

def apply_udf_transforms(df, transforms_cfg: dict):
    """Aplica transformaciones definidas con UDFs del catálogo."""
    if not transforms_cfg:
        return df

    udf_list = transforms_cfg.get('udf') or []
    if not udf_list:
        return df

    default_on_error = (transforms_cfg.get('on_error') or '').lower() or None

    for t in udf_list:
        if not isinstance(t, dict):
            print(f"[udf] Ignorando entrada no soportada: {t}")
            continue

        target = t.get('target_column') or t.get('name')
        func_name = t.get('function')
        args = t.get('args', [])
        mode = (t.get('mode') or 'create').lower()
        cast_type = t.get('type')
        on_error = (t.get('on_error') or default_on_error)

        if not target or not func_name:
            print(f"[udf] Entrada inválida, falta target/function: {t}")
            continue

        udf_func = get_udf(func_name)
        if not udf_func:
            print(f"[udf] UDF no encontrada en catálogo: {func_name}")
            continue

        try:
            # Respetar el modo: create solo si no existe; replace siempre
            target_exists = target in df.columns
            if mode == 'create' and target_exists:
                print(f"[udf] Skipped create for existing column '{target}'")
                continue

            cols = []
            for a in args:
                if isinstance(a, str) and a in df.columns:
                    cols.append(F.col(a))
                else:
                    # Permitir literales simples
                    cols.append(F.lit(a))

            df = df.withColumn(target, udf_func(*cols))
            if cast_type:
                df = safe_cast(df, target, cast_type, on_error=on_error)
            print(f"[udf] Applied UDF '{func_name}' to '{target}' args={args} type={cast_type or 'auto'}")
        except Exception as e:
            msg = f"[udf] Error applying UDF '{func_name}' to '{target}': {e}"
            if (on_error or '').lower() == 'null':
                ttype = cast_type or 'string'
                df = df.withColumn(target, F.lit(None).cast(ttype))
                print(msg + f" -> set NULL ({ttype})")
            elif (on_error or '').lower() == 'skip':
                print(msg + " -> skipped")
            else:
                print(msg)
                raise

    return df

def apply_quality(df, rules, quarantine_path, run_id):
    if not rules: return df, None, {}

    quarantine_rules, hard_fail, drops, warns = [], [], [], []
    for r in rules:
        expr = r["expr"]
        on_fail = r.get("on_fail","quarantine").lower()
        name = r.get("name","rule")
        cond_bad = f"NOT ({expr})"
        if on_fail == "quarantine": quarantine_rules.append((name, cond_bad))
        elif on_fail == "fail":     hard_fail.append((name, cond_bad))
        elif on_fail == "drop":     drops.append(cond_bad)
        elif on_fail == "warn":     warns.append(cond_bad)
        else:                       quarantine_rules.append((name, cond_bad))

    stats = {}
    for name, cond in hard_fail:
        if df.filter(cond).limit(1).count() > 0:
            raise ValueError(f"[quality][FAIL] Regla '{name}' violada.")

    # Procesar reglas de cuarentena acumulando todas las fallas por registro
    bad_df = None
    if quarantine_rules:
        # Crear una columna para cada regla que indique si falla (True/False)
        temp_df = df
        for rule_name, cond in quarantine_rules:
            temp_df = temp_df.withColumn(f"_fails_{rule_name}", F.expr(cond))
        
        # Crear una condición que identifique registros que fallan al menos una regla
        any_fail_conditions = [f"_fails_{rule_name}" for rule_name, _ in quarantine_rules]
        any_fail_expr = " OR ".join(any_fail_conditions)
        
        # Filtrar registros que fallan al menos una regla
        bad_df = temp_df.filter(any_fail_expr)
        
        if bad_df.count() > 0:
            # Crear una lista de reglas fallidas por registro
            failed_rules_expr = "CASE "
            for i, (rule_name, _) in enumerate(quarantine_rules):
                if i == 0:
                    failed_rules_expr += f"WHEN _fails_{rule_name} THEN '{rule_name}'"
                else:
                    failed_rules_expr += f" WHEN _fails_{rule_name} THEN CONCAT_WS(', ', CASE WHEN _fails_{rule_name} THEN '{rule_name}' END"
                    # Agregar reglas anteriores si también fallan
                    for j in range(i):
                        prev_rule_name, _ = quarantine_rules[j]
                        failed_rules_expr += f", CASE WHEN _fails_{prev_rule_name} THEN '{prev_rule_name}' END"
                    failed_rules_expr += ")"
            failed_rules_expr += " END"
            
            # Simplificar: usar array y array_join para concatenar todas las reglas fallidas
            failed_rules_cases = []
            for rule_name, _ in quarantine_rules:
                failed_rules_cases.append(f"CASE WHEN _fails_{rule_name} THEN '{rule_name}' END")
            
            failed_rules_expr = f"array_join(filter(array({', '.join(failed_rules_cases)}), x -> x IS NOT NULL), ', ')"
            
            bad_df = bad_df.withColumn('_failed_rules', F.expr(failed_rules_expr))
            
            # Remover las columnas temporales de fallas
            for rule_name, _ in quarantine_rules:
                bad_df = bad_df.drop(f"_fails_{rule_name}")
            
            stats["quarantine_count"] = bad_df.count()
            
            # Filtrar registros buenos (que no fallan ninguna regla de cuarentena)
            all_bad_conditions = " OR ".join([f"({cond})" for _, cond in quarantine_rules])
            df = df.filter(f"NOT ({all_bad_conditions})")

    if drops:
        to_drop = " OR ".join([f"({c})" for c in drops])
        stats["dropped_count"] = df.filter(to_drop).count()
        df = df.filter(f"NOT ({to_drop})")

    if warns:
        any_warn = " OR ".join([f"({c})" for c in warns])
        stats["warn_count"] = df.filter(any_warn).count()

    if bad_df is not None and quarantine_path and stats.get('quarantine_count', 0) > 0:
        (bad_df
          .withColumn('_quarantine_reason', F.concat(F.lit('rules_failed: '), F.col('_failed_rules')))
          .withColumn('_run_id', F.lit(run_id))
          .withColumn('_ingestion_ts', F.current_timestamp())
          .drop('_failed_rules')  # Remover columna temporal
          .write.mode('append').parquet(quarantine_path))
        print(f"[quality] Quarantine -> {quarantine_path} rows={stats.get('quarantine_count',0)}")
    elif quarantine_rules and quarantine_path:
        print(f"[quality] No quarantine needed - all records passed validation")

    return df, bad_df, stats

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
    
    # Defaults from global table settings
    default_transforms = table_settings.get('default_transformations', {})

    # Exclude columns: dataset-specific overrides, else global defaults
    exclude_columns = gold_config.get('exclude_columns', default_transforms.get('exclude_columns', []))
    
    # Exclusión de columnas mejorada
    if exclude_columns:
        existing_exclude_cols = [col for col in exclude_columns if col in df.columns]
        if existing_exclude_cols:
            df = df.drop(*existing_exclude_cols)
            print(f"[gold] Excluded columns: {existing_exclude_cols}")
    
    # Adición de columnas: usar del dataset, o por defecto desde database.yml
    add_cols = gold_config.get('add_columns')
    if not add_cols:
        add_cols = default_transforms.get('add_columns', [])
    for col_def in add_cols:
        if isinstance(col_def, dict) and 'name' in col_def and 'value' in col_def:
            col_name = col_def['name']
            col_value = col_def['value']
            col_type = col_def.get('type', 'string')
            
            # Handle special functions like current_timestamp()
            if col_value == "current_timestamp()":
                df = df.withColumn(col_name, F.current_timestamp())
            elif col_value == "current_date()":
                df = df.withColumn(col_name, F.current_date())
            elif isinstance(col_value, str) and col_value.startswith("uuid()"):
                # Generate UUID using monotonically_increasing_id as a simple alternative
                df = df.withColumn(col_name, F.monotonically_increasing_id().cast("string"))
            else:
                # Regular literal values
                df = df.withColumn(col_name, F.lit(col_value).cast(col_type))
            
            print(f"[gold] Added column '{col_name}' with value '{col_value}' as {col_type}")
    
    # Reglas de negocio: usar del dataset, o por defecto desde database.yml
    business_rules = gold_config.get('business_rules')
    if not business_rules:
        business_rules = default_transforms.get('business_rules', [])
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
        upsert_keys = table_settings.get("upsert_keys", None)
        success = db_manager.write_dataframe(df_filtered, table_name, write_mode, upsert_keys)
        
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
        maybe_config_s3a(spark, out['path'], env)

        # Read data via unified source loader
        df = load_source(cfg, spark, env)
        print(f"[source] Loaded {df.count()} rows from {src.get('path', src.get('jdbc',{}).get('table','<jdbc>'))}")

        # Optional Bronze step: convert Raw to Parquet before validations/transforms
        # Prefer new location under output.bronze; fallback to top-level for backward compatibility
        bronze_cfg = (cfg.get('output', {}).get('bronze') or cfg.get('bronze') or {})
        if bronze_cfg.get('enabled', False):
            bronze_path = bronze_cfg.get('path')
            bronze_format = (bronze_cfg.get('format') or 'parquet').lower()
            bronze_mode = (bronze_cfg.get('mode') or 'overwrite').lower()
            compression = bronze_cfg.get('compression', 'snappy')
            part_cols = bronze_cfg.get('partition_by', []) or []
            base_col_name = bronze_cfg.get('partition_from')
            base_col = F.col(base_col_name) if base_col_name and base_col_name in df.columns else None

            if bronze_path:
                maybe_config_s3a(spark, bronze_path, env)
                writer = df.write.mode(bronze_mode).option('compression', compression)
                if part_cols:
                    # Add partitioning columns if needed
                    if base_col is not None:
                        # Convert base column to timestamp to derive partitions robustly
                        ts_col = F.to_timestamp(base_col)
                        for p in part_cols:
                            if p not in df.columns:
                                lp = p.lower()
                                if lp == 'year':
                                    df = df.withColumn('year', F.year(ts_col))
                                elif lp == 'month':
                                    df = df.withColumn('month', F.month(ts_col))
                                elif lp == 'day':
                                    df = df.withColumn('day', F.dayofmonth(ts_col))
                                elif lp == 'date':
                                    df = df.withColumn('date', F.to_date(ts_col))
                        writer = writer.partitionBy(*part_cols)
                    else:
                        # If base column missing, only partition if requested columns already exist
                        existing = [p for p in part_cols if p in df.columns]
                        if len(existing) == len(part_cols):
                            writer = writer.partitionBy(*part_cols)
                        else:
                            print(f"[bronze] Base column '{base_col_name}' not found; skipping partitioning")

                if bronze_format == 'parquet':
                    writer.parquet(bronze_path)
                else:
                    # Fallback a parquet si formato no soportado
                    print(f"[bronze] Formato '{bronze_format}' no soportado, usando parquet")
                    writer.parquet(bronze_path)

                # Releer desde Bronze para continuar con Silver
                df = spark.read.parquet(bronze_path)
                print(f"[bronze] Wrote and reloaded Bronze dataset at {bronze_path}")
        
        # Apply standardization
        std = cfg.get('standardization', {})

        # Optional: flatten nested JSON structures
        json_norm = cfg.get('json_normalization', {})
        if json_norm.get('flatten', True) and src.get('input_format') in ('json','jsonl'):
            paths = json_norm.get('paths')
            df = flatten_json(df, paths)

        # Optional: handle nulls
        nulls_cfg = cfg.get('null_handling', {})
        if nulls_cfg:
            df = sanitize_nulls(df, fills=nulls_cfg.get('fills'), drop_if_null=nulls_cfg.get('drop_if_null'))

        # Rename columns FIRST - before any validations and projections
        for r in std.get('rename', []) or []:
            from_col = r['from']
            to_col = r['to']
            
            # Handle nested fields (e.g., metadata.session_id)
            if '.' in from_col:
                # Extract nested field using col() function
                df = df.withColumn(to_col, F.col(from_col))
            elif from_col in df.columns:
                # Regular column rename
                df = df.withColumnRenamed(from_col, to_col)

        # Optional: project columns (after rename to preserve renamed columns)
        keep_cols = cfg.get('select_columns')
        if keep_cols:
            df = project_columns(df, keep_cols)
        
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
            if col in df.columns:
                # Column exists, apply default for null values
                df = df.withColumn(col, F.when(F.col(col).isNull(), F.lit(val)).otherwise(F.col(col)))
            else:
                # Column doesn't exist, create it with the default value
                df = df.withColumn(col, F.lit(val))
        
        # Deduplicate
        if 'deduplicate' in std:
            key = std['deduplicate']['key']
            order = parse_order(std['deduplicate'].get('order_by', []))
            w = Window.partitionBy(*key).orderBy(*order) if order else Window.partitionBy(*key)
            df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn")==1).drop("_rn")

        # Apply declarative transforms (before partitioning & quality)
        if cfg.get('transforms_ref'):
            tpath = cfg['transforms_ref']
            try:
                transforms_cfg = load_transforms(tpath)
                if transforms_cfg:
                    df = apply_sql_transforms(df, transforms_cfg)
                    df = apply_udf_transforms(df, transforms_cfg)
                else:
                    print(f"[transforms] No transforms found in {tpath}")
            except Exception as e:
                print(f"[transforms] Error applying transforms: {e}")
        
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
            
            # Construir configuración efectiva (dataset.yml tiene prioridad sobre database.yml)
            effective_table_settings = dict(table_settings) if table_settings else {}
            # Overrides de dataset para comportamiento de escritura
            if gold_config.get('write_mode'):
                effective_table_settings['default_write_mode'] = gold_config['write_mode']
            if gold_config.get('upsert_keys'):
                effective_table_settings['upsert_keys'] = gold_config['upsert_keys']
            # Overrides opcionales de nombre de tabla
            if gold_config.get('table_prefix'):
                effective_table_settings['table_prefix'] = gold_config['table_prefix']
            if gold_config.get('table_suffix'):
                effective_table_settings['table_suffix'] = gold_config['table_suffix']

            # Apply transformations (dataset overrides + global defaults)
            gold_df = apply_gold_transformations(df, gold_config, effective_table_settings)
            
            success = write_to_gold_database(
                df=gold_df,
                dataset_id=dataset_id,
                schema_path=schema_path,
                db_manager=db_manager,
                table_settings=effective_table_settings
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