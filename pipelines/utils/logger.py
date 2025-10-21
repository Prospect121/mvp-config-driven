import logging
from typing import Optional

_DEFAULT_FORMAT = "%(asctime)s %(levelname)s [%(name)s] run_id=%(run_id)s - %(message)s"

class RunIdFilter(logging.Filter):
    def __init__(self, run_id: str):
        super().__init__()
        self.run_id = run_id
    def filter(self, record: logging.LogRecord) -> bool:
        # Attach run_id to record
        setattr(record, "run_id", self.run_id)
        return True


def get_logger(module_name: str, run_id: Optional[str] = None, level: int = logging.INFO) -> logging.Logger:
    """Create a structured logger with module name and optional run_id.

    Args:
        module_name: Logical module name, e.g. 'validation.quality'
        run_id: Optional run identifier attached to all records
        level: Logging level
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(module_name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(_DEFAULT_FORMAT)
        handler.setFormatter(formatter)
        if run_id:
            handler.addFilter(RunIdFilter(run_id))
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger