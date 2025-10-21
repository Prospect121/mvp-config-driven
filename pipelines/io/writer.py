from typing import Optional, Sequence
from pyspark.sql import DataFrame
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

from pipelines.utils.logger import get_logger


logger = get_logger("io.writer")


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8), before_sleep=before_sleep_log(logger, level=30))
def write_parquet(df: DataFrame, path: str, mode: str = "overwrite", partition_by: Optional[Sequence[str]] = None, coalesce: Optional[int] = None, repartition: Optional[int] = None) -> None:
    """Write a DataFrame to Parquet with optional partitioning and basic optimizations.

    Args:
        df: Spark DataFrame a escribir.
        path: Ruta destino (local o s3a://...).
        mode: Política de escritura (overwrite/append).
        partition_by: Columnas para particionar.
        coalesce: Número de particiones para coalesce antes de escribir.
        repartition: Número de particiones para repartition antes de escribir.
    """
    tmp = df
    if repartition and repartition > 0:
        tmp = tmp.repartition(repartition)
    if coalesce and coalesce > 0:
        tmp = tmp.coalesce(coalesce)

    writer = tmp.write.mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    logger.info("writing parquet", extra={"path": path, "mode": mode, "partition_by": partition_by, "coalesce": coalesce, "repartition": repartition})
    writer.parquet(path)