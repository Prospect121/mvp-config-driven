"""
Implementación del patrón Strategy para diferentes estrategias de procesamiento
de datos. Permite cambiar algoritmos de procesamiento dinámicamente según
el tipo de datos, volumen, o requisitos específicos.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Type, Union
from enum import Enum
from dataclasses import dataclass
import pandas as pd
from datetime import datetime
import asyncio

from ..utils.logging import get_logger, log_execution_time

logger = get_logger(__name__)

class ProcessingMode(Enum):
    """Modos de procesamiento disponibles."""
    BATCH = "batch"
    STREAMING = "streaming"
    MICRO_BATCH = "micro_batch"
    REAL_TIME = "real_time"

class DataFormat(Enum):
    """Formatos de datos soportados."""
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    AVRO = "avro"
    DELTA = "delta"
    XML = "xml"

@dataclass
class ProcessingContext:
    """Contexto para estrategias de procesamiento."""
    data_format: DataFormat
    processing_mode: ProcessingMode
    volume_estimate: int
    quality_requirements: Dict[str, Any]
    performance_requirements: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = None

class DataProcessingStrategy(ABC):
    """Interfaz base para estrategias de procesamiento de datos."""
    
    @abstractmethod
    async def process(self, 
                     data: Any, 
                     context: ProcessingContext,
                     config: Dict[str, Any]) -> Any:
        """Procesa los datos según la estrategia específica."""
        pass
    
    @abstractmethod
    def supports_format(self, data_format: DataFormat) -> bool:
        """Indica si la estrategia soporta el formato de datos."""
        pass
    
    @abstractmethod
    def supports_mode(self, processing_mode: ProcessingMode) -> bool:
        """Indica si la estrategia soporta el modo de procesamiento."""
        pass
    
    @abstractmethod
    def get_performance_characteristics(self) -> Dict[str, Any]:
        """Retorna las características de performance de la estrategia."""
        pass
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Nombre de la estrategia."""
        pass

class PandasBatchStrategy(DataProcessingStrategy):
    """Estrategia de procesamiento batch usando Pandas."""
    
    def __init__(self):
        self.logger = get_logger(f"{__name__}.PandasBatchStrategy")
    
    @log_execution_time()
    async def process(self, 
                     data: Union[str, pd.DataFrame], 
                     context: ProcessingContext,
                     config: Dict[str, Any]) -> pd.DataFrame:
        """Procesa datos usando Pandas en modo batch."""
        
        self.logger.info(
            "Iniciando procesamiento Pandas batch",
            data_format=context.data_format.value,
            volume_estimate=context.volume_estimate
        )
        
        # Cargar datos si es necesario
        if isinstance(data, str):
            df = await self._load_data(data, context.data_format)
        else:
            df = data.copy()
        
        # Aplicar transformaciones
        df = await self._apply_transformations(df, config)
        
        # Validar calidad de datos
        await self._validate_quality(df, context.quality_requirements)
        
        self.logger.info(
            "Procesamiento Pandas batch completado",
            records_processed=len(df),
            columns=list(df.columns)
        )
        
        return df
    
    async def _load_data(self, data_path: str, data_format: DataFormat) -> pd.DataFrame:
        """Carga datos según el formato."""
        if data_format == DataFormat.CSV:
            return pd.read_csv(data_path)
        elif data_format == DataFormat.JSON:
            return pd.read_json(data_path)
        elif data_format == DataFormat.PARQUET:
            return pd.read_parquet(data_path)
        else:
            raise ValueError(f"Formato no soportado: {data_format}")
    
    async def _apply_transformations(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Aplica transformaciones configuradas."""
        transformations = config.get('transformations', [])
        
        for transform in transformations:
            transform_type = transform.get('type')
            
            if transform_type == 'filter':
                condition = transform.get('condition')
                df = df.query(condition)
            elif transform_type == 'aggregate':
                group_by = transform.get('group_by', [])
                agg_functions = transform.get('functions', {})
                df = df.groupby(group_by).agg(agg_functions).reset_index()
            elif transform_type == 'rename':
                columns = transform.get('columns', {})
                df = df.rename(columns=columns)
            elif transform_type == 'drop':
                columns = transform.get('columns', [])
                df = df.drop(columns=columns)
            elif transform_type == 'add_column':
                column_name = transform.get('name')
                expression = transform.get('expression')
                df[column_name] = df.eval(expression)
        
        return df
    
    async def _validate_quality(self, df: pd.DataFrame, requirements: Dict[str, Any]):
        """Valida la calidad de los datos."""
        for check_name, check_config in requirements.items():
            if check_name == 'null_percentage':
                max_null_pct = check_config.get('max_percentage', 5.0)
                for column in check_config.get('columns', df.columns):
                    null_pct = (df[column].isnull().sum() / len(df)) * 100
                    if null_pct > max_null_pct:
                        raise ValueError(f"Columna {column} excede el porcentaje máximo de nulos: {null_pct}%")
            
            elif check_name == 'duplicate_percentage':
                max_dup_pct = check_config.get('max_percentage', 1.0)
                dup_pct = (df.duplicated().sum() / len(df)) * 100
                if dup_pct > max_dup_pct:
                    raise ValueError(f"Datos exceden el porcentaje máximo de duplicados: {dup_pct}%")
    
    def supports_format(self, data_format: DataFormat) -> bool:
        """Soporta CSV, JSON y Parquet."""
        return data_format in [DataFormat.CSV, DataFormat.JSON, DataFormat.PARQUET]
    
    def supports_mode(self, processing_mode: ProcessingMode) -> bool:
        """Soporta modo batch."""
        return processing_mode == ProcessingMode.BATCH
    
    def get_performance_characteristics(self) -> Dict[str, Any]:
        """Características de performance."""
        return {
            "max_records": 10_000_000,
            "memory_efficient": False,
            "parallel_processing": False,
            "streaming_capable": False,
            "best_for": ["small_to_medium_datasets", "complex_transformations"]
        }
    
    @property
    def name(self) -> str:
        return "PandasBatchStrategy"

class SparkBatchStrategy(DataProcessingStrategy):
    """Estrategia de procesamiento batch usando Spark."""
    
    def __init__(self, spark_session=None):
        self.logger = get_logger(f"{__name__}.SparkBatchStrategy")
        self.spark = spark_session
    
    @log_execution_time()
    async def process(self, 
                     data: Union[str, Any], 
                     context: ProcessingContext,
                     config: Dict[str, Any]) -> Any:
        """Procesa datos usando Spark en modo batch."""
        
        if not self.spark:
            raise RuntimeError("Spark session no configurada")
        
        self.logger.info(
            "Iniciando procesamiento Spark batch",
            data_format=context.data_format.value,
            volume_estimate=context.volume_estimate
        )
        
        # Cargar datos
        df = await self._load_data(data, context.data_format)
        
        # Aplicar transformaciones
        df = await self._apply_transformations(df, config)
        
        # Validar calidad
        await self._validate_quality(df, context.quality_requirements)
        
        record_count = df.count()
        self.logger.info(
            "Procesamiento Spark batch completado",
            records_processed=record_count
        )
        
        return df
    
    async def _load_data(self, data_path: str, data_format: DataFormat):
        """Carga datos usando Spark."""
        if data_format == DataFormat.CSV:
            return self.spark.read.csv(data_path, header=True, inferSchema=True)
        elif data_format == DataFormat.JSON:
            return self.spark.read.json(data_path)
        elif data_format == DataFormat.PARQUET:
            return self.spark.read.parquet(data_path)
        elif data_format == DataFormat.DELTA:
            return self.spark.read.format("delta").load(data_path)
        else:
            raise ValueError(f"Formato no soportado: {data_format}")
    
    async def _apply_transformations(self, df, config: Dict[str, Any]):
        """Aplica transformaciones usando Spark SQL."""
        transformations = config.get('transformations', [])
        
        # Registrar DataFrame como vista temporal
        df.createOrReplaceTempView("temp_data")
        
        for transform in transformations:
            transform_type = transform.get('type')
            
            if transform_type == 'sql':
                sql_query = transform.get('query')
                df = self.spark.sql(sql_query)
                df.createOrReplaceTempView("temp_data")
            elif transform_type == 'filter':
                condition = transform.get('condition')
                df = df.filter(condition)
            elif transform_type == 'aggregate':
                group_by = transform.get('group_by', [])
                agg_expressions = transform.get('expressions', [])
                df = df.groupBy(*group_by).agg(*agg_expressions)
        
        return df
    
    async def _validate_quality(self, df, requirements: Dict[str, Any]):
        """Valida calidad usando Spark."""
        total_count = df.count()
        
        for check_name, check_config in requirements.items():
            if check_name == 'null_percentage':
                max_null_pct = check_config.get('max_percentage', 5.0)
                for column in check_config.get('columns', df.columns):
                    null_count = df.filter(df[column].isNull()).count()
                    null_pct = (null_count / total_count) * 100
                    if null_pct > max_null_pct:
                        raise ValueError(f"Columna {column} excede el porcentaje máximo de nulos: {null_pct}%")
    
    def supports_format(self, data_format: DataFormat) -> bool:
        """Soporta múltiples formatos."""
        return data_format in [
            DataFormat.CSV, DataFormat.JSON, DataFormat.PARQUET, 
            DataFormat.AVRO, DataFormat.DELTA
        ]
    
    def supports_mode(self, processing_mode: ProcessingMode) -> bool:
        """Soporta batch y micro-batch."""
        return processing_mode in [ProcessingMode.BATCH, ProcessingMode.MICRO_BATCH]
    
    def get_performance_characteristics(self) -> Dict[str, Any]:
        """Características de performance."""
        return {
            "max_records": 1_000_000_000,
            "memory_efficient": True,
            "parallel_processing": True,
            "streaming_capable": True,
            "best_for": ["large_datasets", "distributed_processing", "complex_analytics"]
        }
    
    @property
    def name(self) -> str:
        return "SparkBatchStrategy"

class StreamingStrategy(DataProcessingStrategy):
    """Estrategia de procesamiento en streaming."""
    
    def __init__(self):
        self.logger = get_logger(f"{__name__}.StreamingStrategy")
    
    @log_execution_time()
    async def process(self, 
                     data: Any, 
                     context: ProcessingContext,
                     config: Dict[str, Any]) -> Any:
        """Procesa datos en modo streaming."""
        
        self.logger.info(
            "Iniciando procesamiento streaming",
            data_format=context.data_format.value
        )
        
        # Configurar stream
        stream_config = config.get('streaming', {})
        batch_size = stream_config.get('batch_size', 1000)
        window_duration = stream_config.get('window_duration', '10 seconds')
        
        # Procesar en micro-batches
        processed_records = 0
        async for batch in self._create_batches(data, batch_size):
            processed_batch = await self._process_batch(batch, config)
            processed_records += len(processed_batch)
            
            # Aplicar ventana de tiempo si es necesario
            if 'window_operations' in config:
                await self._apply_window_operations(processed_batch, config['window_operations'])
        
        self.logger.info(
            "Procesamiento streaming completado",
            records_processed=processed_records
        )
        
        return {"status": "completed", "records_processed": processed_records}
    
    async def _create_batches(self, data, batch_size: int):
        """Crea micro-batches del stream de datos."""
        # Simulación de streaming - en implementación real sería un stream real
        if hasattr(data, '__iter__'):
            batch = []
            for item in data:
                batch.append(item)
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch
    
    async def _process_batch(self, batch: List[Any], config: Dict[str, Any]) -> List[Any]:
        """Procesa un micro-batch."""
        transformations = config.get('transformations', [])
        
        for transform in transformations:
            transform_type = transform.get('type')
            
            if transform_type == 'filter':
                condition_func = transform.get('function')
                batch = [item for item in batch if condition_func(item)]
            elif transform_type == 'map':
                map_func = transform.get('function')
                batch = [map_func(item) for item in batch]
        
        return batch
    
    async def _apply_window_operations(self, batch: List[Any], window_config: Dict[str, Any]):
        """Aplica operaciones de ventana temporal."""
        # Implementación de ventanas deslizantes, tumbling, etc.
        pass
    
    def supports_format(self, data_format: DataFormat) -> bool:
        """Soporta JSON principalmente."""
        return data_format in [DataFormat.JSON, DataFormat.AVRO]
    
    def supports_mode(self, processing_mode: ProcessingMode) -> bool:
        """Soporta streaming y real-time."""
        return processing_mode in [ProcessingMode.STREAMING, ProcessingMode.REAL_TIME]
    
    def get_performance_characteristics(self) -> Dict[str, Any]:
        """Características de performance."""
        return {
            "max_records": float('inf'),
            "memory_efficient": True,
            "parallel_processing": True,
            "streaming_capable": True,
            "latency": "low",
            "best_for": ["real_time_processing", "event_streams", "continuous_data"]
        }
    
    @property
    def name(self) -> str:
        return "StreamingStrategy"

class ProcessingStrategySelector:
    """Selector de estrategias de procesamiento basado en contexto."""
    
    def __init__(self):
        self.strategies: Dict[str, DataProcessingStrategy] = {}
        self.logger = get_logger(f"{__name__}.ProcessingStrategySelector")
    
    def register_strategy(self, strategy: DataProcessingStrategy):
        """Registra una nueva estrategia."""
        self.strategies[strategy.name] = strategy
        self.logger.info(f"Estrategia registrada: {strategy.name}")
    
    def select_strategy(self, context: ProcessingContext) -> DataProcessingStrategy:
        """Selecciona la mejor estrategia basada en el contexto."""
        
        # Filtrar estrategias compatibles
        compatible_strategies = []
        for strategy in self.strategies.values():
            if (strategy.supports_format(context.data_format) and 
                strategy.supports_mode(context.processing_mode)):
                compatible_strategies.append(strategy)
        
        if not compatible_strategies:
            raise ValueError(
                f"No hay estrategias compatibles para formato {context.data_format} "
                f"y modo {context.processing_mode}"
            )
        
        # Seleccionar la mejor estrategia basada en volumen y requisitos
        best_strategy = self._rank_strategies(compatible_strategies, context)
        
        self.logger.info(
            f"Estrategia seleccionada: {best_strategy.name}",
            data_format=context.data_format.value,
            processing_mode=context.processing_mode.value,
            volume_estimate=context.volume_estimate
        )
        
        return best_strategy
    
    def _rank_strategies(self, 
                        strategies: List[DataProcessingStrategy], 
                        context: ProcessingContext) -> DataProcessingStrategy:
        """Rankea estrategias basado en el contexto."""
        
        # Lógica de ranking simple
        volume = context.volume_estimate
        
        # Para volúmenes grandes, preferir Spark
        if volume > 1_000_000:
            spark_strategies = [s for s in strategies if 'Spark' in s.name]
            if spark_strategies:
                return spark_strategies[0]
        
        # Para streaming, preferir estrategias de streaming
        if context.processing_mode in [ProcessingMode.STREAMING, ProcessingMode.REAL_TIME]:
            streaming_strategies = [s for s in strategies if 'Streaming' in s.name]
            if streaming_strategies:
                return streaming_strategies[0]
        
        # Por defecto, usar la primera estrategia compatible
        return strategies[0]

class DataProcessor:
    """Procesador principal que usa el patrón Strategy."""
    
    def __init__(self):
        self.selector = ProcessingStrategySelector()
        self.logger = get_logger(f"{__name__}.DataProcessor")
        self._setup_default_strategies()
    
    def _setup_default_strategies(self):
        """Configura las estrategias por defecto."""
        self.selector.register_strategy(PandasBatchStrategy())
        self.selector.register_strategy(StreamingStrategy())
        # SparkBatchStrategy se registraría cuando Spark esté disponible
    
    async def process_data(self, 
                          data: Any,
                          context: ProcessingContext,
                          config: Dict[str, Any]) -> Any:
        """Procesa datos usando la estrategia más apropiada."""
        
        # Seleccionar estrategia
        strategy = self.selector.select_strategy(context)
        
        # Procesar datos
        result = await strategy.process(data, context, config)
        
        self.logger.info(
            "Procesamiento de datos completado",
            strategy=strategy.name,
            data_format=context.data_format.value,
            processing_mode=context.processing_mode.value
        )
        
        return result

# Ejemplo de uso
if __name__ == "__main__":
    import asyncio
    
    async def ejemplo_uso():
        # Crear procesador
        processor = DataProcessor()
        
        # Definir contexto
        context = ProcessingContext(
            data_format=DataFormat.CSV,
            processing_mode=ProcessingMode.BATCH,
            volume_estimate=50000,
            quality_requirements={
                'null_percentage': {'max_percentage': 5.0, 'columns': ['amount', 'customer_id']},
                'duplicate_percentage': {'max_percentage': 1.0}
            },
            performance_requirements={'max_duration_minutes': 10}
        )
        
        # Configuración de procesamiento
        config = {
            'transformations': [
                {
                    'type': 'filter',
                    'condition': 'amount > 0'
                },
                {
                    'type': 'add_column',
                    'name': 'processed_date',
                    'expression': f"'{datetime.now().isoformat()}'"
                }
            ]
        }
        
        # Datos de ejemplo
        import pandas as pd
        sample_data = pd.DataFrame({
            'customer_id': [1, 2, 3, 4, 5],
            'amount': [100, 200, -50, 300, 150],
            'date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05']
        })
        
        # Procesar datos
        result = await processor.process_data(sample_data, context, config)
        print(f"Resultado: {len(result)} registros procesados")
    
    asyncio.run(ejemplo_uso())