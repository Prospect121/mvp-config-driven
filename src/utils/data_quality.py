"""
Validaciones de calidad de datos.
"""
import yaml
import logging
from typing import Dict, Any, List, Tuple, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

class DataQualityValidator:
    """Validador de calidad de datos"""
    
    def __init__(self):
        self.supported_rules = {
            'not_null': self._validate_not_null,
            'unique': self._validate_unique,
            'range': self._validate_range,
            'regex': self._validate_regex,
            'in_list': self._validate_in_list,
            'custom_sql': self._validate_custom_sql
        }
    
    def load_expectations(self, expectations_path: str) -> Dict[str, Any]:
        """
        Carga expectativas de calidad desde archivo YAML.
        
        Args:
            expectations_path: Ruta al archivo de expectativas
            
        Returns:
            Configuración de expectativas
        """
        try:
            with open(expectations_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Error loading expectations from {expectations_path}: {e}")
            raise
    
    def validate(self, df: DataFrame, expectations_path: str, 
                quarantine_path: Optional[str] = None) -> Tuple[DataFrame, Optional[DataFrame], Dict[str, Any]]:
        """
        Ejecuta validaciones de calidad de datos.
        
        Args:
            df: DataFrame a validar
            expectations_path: Ruta a las expectativas
            quarantine_path: Ruta para datos en cuarentena
            
        Returns:
            Tupla con (df_limpio, df_cuarentena, estadísticas)
        """
        expectations = self.load_expectations(expectations_path)
        rules = expectations.get('rules', [])
        
        if not rules:
            logger.info("No quality rules defined")
            return df, None, {'total_rows': df.count(), 'clean_rows': df.count(), 'quarantine_rows': 0}
        
        total_rows = df.count()
        df_with_quality = df
        quality_columns = []
        
        # Aplicar cada regla de calidad
        for i, rule in enumerate(rules):
            rule_name = f"quality_rule_{i}"
            quality_col = f"_quality_{rule_name}"
            
            df_with_quality = self._apply_rule(df_with_quality, rule, quality_col)
            quality_columns.append(quality_col)
        
        # Crear columna de calidad general
        if quality_columns:
            quality_expr = F.lit(True)
            for col in quality_columns:
                quality_expr = quality_expr & F.col(col)
            
            df_with_quality = df_with_quality.withColumn('_is_valid', quality_expr)
        else:
            df_with_quality = df_with_quality.withColumn('_is_valid', F.lit(True))
        
        # Separar datos limpios y en cuarentena
        df_clean = df_with_quality.filter(F.col('_is_valid') == True)
        df_quarantine = df_with_quality.filter(F.col('_is_valid') == False)
        
        # Remover columnas de calidad del resultado limpio
        cols_to_drop = quality_columns + ['_is_valid']
        df_clean = df_clean.drop(*cols_to_drop)
        
        clean_rows = df_clean.count()
        quarantine_rows = df_quarantine.count()
        
        # Guardar datos en cuarentena si se especifica path
        if quarantine_path and quarantine_rows > 0:
            try:
                df_quarantine.write.mode('append').parquet(quarantine_path)
                logger.info(f"Saved {quarantine_rows} rows to quarantine: {quarantine_path}")
            except Exception as e:
                logger.error(f"Failed to save quarantine data: {e}")
        
        stats = {
            'total_rows': total_rows,
            'clean_rows': clean_rows,
            'quarantine_rows': quarantine_rows,
            'quality_rate': clean_rows / total_rows if total_rows > 0 else 0
        }
        
        logger.info(f"Quality validation completed: {stats}")
        
        return df_clean, df_quarantine if quarantine_rows > 0 else None, stats
    
    def _apply_rule(self, df: DataFrame, rule: Dict[str, Any], quality_col: str) -> DataFrame:
        """
        Aplica una regla de calidad específica.
        
        Args:
            df: DataFrame
            rule: Definición de la regla
            quality_col: Nombre de la columna de calidad a crear
            
        Returns:
            DataFrame con columna de calidad añadida
        """
        rule_type = rule.get('type')
        
        if rule_type not in self.supported_rules:
            logger.warning(f"Unsupported rule type: {rule_type}")
            return df.withColumn(quality_col, F.lit(True))
        
        try:
            return self.supported_rules[rule_type](df, rule, quality_col)
        except Exception as e:
            logger.error(f"Error applying rule {rule_type}: {e}")
            return df.withColumn(quality_col, F.lit(True))
    
    def _validate_not_null(self, df: DataFrame, rule: Dict[str, Any], quality_col: str) -> DataFrame:
        """Valida que las columnas no sean nulas"""
        columns = rule.get('columns', [])
        if not columns:
            return df.withColumn(quality_col, F.lit(True))
        
        condition = F.lit(True)
        for col in columns:
            if col in df.columns:
                condition = condition & F.col(col).isNotNull()
        
        return df.withColumn(quality_col, condition)
    
    def _validate_unique(self, df: DataFrame, rule: Dict[str, Any], quality_col: str) -> DataFrame:
        """Valida unicidad de columnas"""
        columns = rule.get('columns', [])
        if not columns:
            return df.withColumn(quality_col, F.lit(True))
        
        # Contar duplicados
        window_spec = F.window.partitionBy(*columns)
        df_with_count = df.withColumn('_dup_count', F.count('*').over(window_spec))
        
        condition = F.col('_dup_count') == 1
        return df_with_count.withColumn(quality_col, condition).drop('_dup_count')
    
    def _validate_range(self, df: DataFrame, rule: Dict[str, Any], quality_col: str) -> DataFrame:
        """Valida rangos de valores"""
        column = rule.get('column')
        min_val = rule.get('min')
        max_val = rule.get('max')
        
        if not column or column not in df.columns:
            return df.withColumn(quality_col, F.lit(True))
        
        condition = F.lit(True)
        
        if min_val is not None:
            condition = condition & (F.col(column) >= min_val)
        
        if max_val is not None:
            condition = condition & (F.col(column) <= max_val)
        
        return df.withColumn(quality_col, condition)
    
    def _validate_regex(self, df: DataFrame, rule: Dict[str, Any], quality_col: str) -> DataFrame:
        """Valida patrones regex"""
        column = rule.get('column')
        pattern = rule.get('pattern')
        
        if not column or not pattern or column not in df.columns:
            return df.withColumn(quality_col, F.lit(True))
        
        condition = F.col(column).rlike(pattern)
        return df.withColumn(quality_col, condition)
    
    def _validate_in_list(self, df: DataFrame, rule: Dict[str, Any], quality_col: str) -> DataFrame:
        """Valida valores en lista permitida"""
        column = rule.get('column')
        allowed_values = rule.get('values', [])
        
        if not column or not allowed_values or column not in df.columns:
            return df.withColumn(quality_col, F.lit(True))
        
        condition = F.col(column).isin(allowed_values)
        return df.withColumn(quality_col, condition)
    
    def _validate_custom_sql(self, df: DataFrame, rule: Dict[str, Any], quality_col: str) -> DataFrame:
        """Valida usando SQL personalizado"""
        sql_condition = rule.get('condition')
        
        if not sql_condition:
            return df.withColumn(quality_col, F.lit(True))
        
        try:
            condition = F.expr(sql_condition)
            return df.withColumn(quality_col, condition)
        except Exception as e:
            logger.error(f"Error in custom SQL condition: {e}")
            return df.withColumn(quality_col, F.lit(True))