from typing import Any, Dict, Optional, Sequence

from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

from datacore.io.fs import storage_options_from_env, write_df
from pipelines.utils.logger import get_logger


logger = get_logger("io.writer")


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    before_sleep=before_sleep_log(logger, level=30),
)
def write_parquet(
    df: Any,
    path: str,
    *,
    mode: str = "overwrite",
    partition_by: Optional[Sequence[str]] = None,
    coalesce: Optional[int] = None,
    repartition: Optional[int] = None,
    env: Optional[Dict[str, str]] = None,
    writer_options: Optional[Dict[str, Any]] = None,
) -> None:
    """Write a DataFrame to Parquet using the shared filesystem adapters."""

    storage_opts = storage_options_from_env(path, env or {}) if path else {}
    logger.info(
        "writing parquet",
        extra={
            "path": path,
            "mode": mode,
            "partition_by": partition_by,
            "coalesce": coalesce,
            "repartition": repartition,
            "storage_options": bool(storage_opts),
        },
    )
    write_df(
        df,
        path,
        "parquet",
        mode=mode,
        partition_by=partition_by,
        coalesce=coalesce,
        repartition=repartition,
        storage_options=storage_opts,
        writer_options=writer_options or {},
    )
