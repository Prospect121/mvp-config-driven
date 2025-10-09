#!/usr/bin/env python3
"""
Validation script for Gold layer database integration
Validates that data was correctly written to the database and schema matches expectations
"""

import sys
import yaml
import json
import argparse
from pathlib import Path
from typing import Dict, Any, List, Optional

# Add pipelines to path
sys.path.append(str(Path(__file__).parent.parent / "pipelines"))

from database.db_manager import DatabaseManager
from database.schema_mapper import SchemaMapper, DatabaseEngine


def load_config(config_path: str) -> Dict[str, Any]:
    """Load YAML configuration file"""
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def load_schema(schema_path: str) -> Dict[str, Any]:
    """Load JSON schema file"""
    with open(schema_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def validate_table_exists(db_manager: DatabaseManager, table_name: str) -> bool:
    """Check if table exists in database"""
    try:
        with db_manager.get_connection() as conn:
            # PostgreSQL query to check if table exists
            query = """
            SELECT COUNT(*) > 0 as table_exists FROM (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            );
            """
            
            cursor = conn.cursor()
            cursor.execute(query, (table_name,))
            result = cursor.fetchone()
            return bool(result[0])
    except Exception as e:
        print(f"Error checking table existence: {e}")
        return False


def validate_table_schema(db_manager: DatabaseManager, table_name: str, 
                         expected_schema: Dict[str, Any]) -> List[str]:
    """Validate that table schema matches expected schema"""
    errors = []
    
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # PostgreSQL query to get column information
            query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position;
            """
            
            cursor.execute(query, (table_name,))
            columns = cursor.fetchall()
            actual_columns = {
                col[0]: {  # column_name
                    'type': col[1],  # data_type
                    'nullable': col[2].upper() == 'YES'  # is_nullable
                }
                for col in columns
            }
            
            # Create schema mapper to get expected columns
            mapper = SchemaMapper(db_manager.engine)
            expected_columns = mapper.schema_to_columns(expected_schema)
            
            # Check for missing columns
            for col_def in expected_columns:
                if col_def.name not in actual_columns:
                    errors.append(f"Missing column: {col_def.name}")
                else:
                    actual_col = actual_columns[col_def.name]
                    # Note: Type checking is complex due to database-specific type names
                    # For now, just check nullability
                    if col_def.nullable != actual_col['nullable']:
                        errors.append(
                            f"Column {col_def.name}: nullable mismatch. "
                            f"Expected: {col_def.nullable}, Actual: {actual_col['nullable']}"
                        )
            
            # Check for extra columns
            expected_col_names = {col.name for col in expected_columns}
            for col_name in actual_columns:
                if col_name not in expected_col_names:
                    errors.append(f"Unexpected column: {col_name}")
                    
    except Exception as e:
        errors.append(f"Error validating table schema: {e}")
    
    return errors


def validate_data_count(db_manager: DatabaseManager, table_name: str) -> Optional[int]:
    """Get row count from table"""
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            result = cursor.fetchone()
            return result[0] if result else 0
    except Exception as e:
        print(f"Error getting row count: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description='Validate Gold layer database integration')
    parser.add_argument('dataset_config', help='Path to dataset configuration file')
    parser.add_argument('db_config', help='Path to database configuration file')
    parser.add_argument('environment', help='Environment (development, production, etc.)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    print("=== Gold Layer Validation ===")
    print(f"Dataset Config: {args.dataset_config}")
    print(f"Database Config: {args.db_config}")
    print(f"Environment: {args.environment}")
    print("=" * 30)
    
    try:
        # Load configurations
        dataset_config = load_config(args.dataset_config)
        db_config = load_config(args.db_config)
        
        # Check if Gold layer is enabled
        gold_config = dataset_config.get('output', {}).get('gold', {})
        if not gold_config.get('enabled', False):
            print("❌ Gold layer is not enabled in dataset configuration")
            return 1
        
        # Load schema
        schema_ref = dataset_config.get('schema', {}).get('ref')
        if not schema_ref:
            print("❌ No schema reference found in dataset configuration")
            return 1
        
        schema_path = Path(args.dataset_config).parent / Path(schema_ref).name
        if not schema_path.exists():
            print(f"❌ Schema file not found: {schema_path}")
            return 1
        
        schema = load_schema(str(schema_path))
        
        # Initialize database manager
        db_manager = DatabaseManager(str(args.db_config), args.environment)
        
        # Determine table name
        table_name = gold_config.get('table_name', dataset_config['id'])
        table_prefix = gold_config.get('table_settings', {}).get('table_prefix', 
                                     db_config.get('environments', {}).get(args.environment, {})
                                     .get('table_settings', {}).get('table_prefix', ''))
        table_suffix = gold_config.get('table_settings', {}).get('table_suffix',
                                     db_config.get('environments', {}).get(args.environment, {})
                                     .get('table_settings', {}).get('table_suffix', ''))
        
        full_table_name = f"{table_prefix}{table_name}{table_suffix}"
        
        print(f"Validating table: {full_table_name}")
        
        # Validation checks
        validation_errors = []
        
        # 1. Check if table exists
        if validate_table_exists(db_manager, full_table_name):
            print("✓ Table exists")
        else:
            print("❌ Table does not exist")
            validation_errors.append("Table does not exist")
            return 1
        
        # 2. Validate schema
        schema_errors = validate_table_schema(db_manager, full_table_name, schema)
        if schema_errors:
            print("❌ Schema validation failed:")
            for error in schema_errors:
                print(f"  - {error}")
            validation_errors.extend(schema_errors)
        else:
            print("✓ Schema validation passed")
        
        # 3. Check data count
        row_count = validate_data_count(db_manager, full_table_name)
        if row_count is not None:
            print(f"✓ Table contains {row_count:,} rows")
        else:
            print("❌ Could not retrieve row count")
            validation_errors.append("Could not retrieve row count")
        
        # 4. Check schema versioning table if enabled
        if gold_config.get('schema_versioning', False):
            schema_table = f"{full_table_name}_schema_versions"
            if validate_table_exists(db_manager, schema_table):
                print("✓ Schema versioning table exists")
            else:
                print("⚠️  Schema versioning enabled but table does not exist")
        
        # Summary
        print("\n=== Validation Summary ===")
        if validation_errors:
            print(f"❌ Validation failed with {len(validation_errors)} error(s):")
            for error in validation_errors:
                print(f"  - {error}")
            return 1
        else:
            print("✅ All validations passed!")
            return 0
            
    except Exception as e:
        print(f"❌ Validation failed with exception: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())