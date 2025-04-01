import logging
from rich.logging import RichHandler

def _setup_logging():
    """
    Setup logging configuration.
    """
    handler = RichHandler(
    )
    logging.basicConfig(
        format='%(message)s',
        datefmt='[%X]',
        handlers=[handler],
    )
    logger = logging.getLogger('tabpro')
    logger.setLevel(logging.INFO)
    return logger

logger = _setup_logging()

__all__ = [
    'logger',
]
