import logging
import os
from logging.handlers import RotatingFileHandler


# get logger function to create or return a logger with a default name and log levels
def get_logger(name="bronze_ingest", level=logging.INFO):
    # logs will be stored in this dir
    log_dir = "store/logs"
    # this will create if dir not exists
    os.makedirs(log_dir, exist_ok=True)
    # get logegr with given name
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    # output to see in this format
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    fh = RotatingFileHandler(
        os.path.join(log_dir, f"{name}.log"),
        # create a log with 5MB and keep last 3 files
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
    )
    fh.setFormatter(formatter)
    fh.setLevel(level)
    # logs will print on console as well
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    ch.setLevel(level)

    logger.setLevel(level)
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.propagate=False
    # this will return logger that can be used anywhere in project
    return logger
