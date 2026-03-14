import logging
import os


LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def configure_logging(name: str) -> logging.Logger:
    level_name = os.environ.get("BATCH_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(level=level, format=LOG_FORMAT)
    else:
        root_logger.setLevel(level)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    return logger
