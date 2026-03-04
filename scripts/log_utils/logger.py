"""
Central Logging System for Mini Data Platform

Provides structured, rotating logs for all components.
"""

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_DIR = Path(os.getenv("LOG_DIR", "/opt/airflow/logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)


class ColoredFormatter(logging.Formatter):
    """Colored formatter for console output"""

    COLORS = {
        "DEBUG": "\033[36m",
        "INFO": "\033[32m",
        "WARNING": "\033[33m",
        "ERROR": "\033[31m",
        "CRITICAL": "\033[35m",
        "RESET": "\033[0m",
    }

    def format(self, record):
        log_color = self.COLORS.get(record.levelname, self.COLORS["RESET"])
        record.levelname = f"{log_color}{record.levelname}{self.COLORS['RESET']}"
        return super().format(record)


class LogClassifier:
    """Classifies logs by component/source"""

    COMPONENTS = {
        "GENERATOR": "data_generator",
        "ETL": "etl",
        "BRONZE": "bronze",
        "SILVER": "silver",
        "GOLD": "gold",
        "MINIO": "minio",
        "POSTGRES": "postgres",
        "METABASE": "metabase",
        "AIRFLOW": "airflow",
        "TEST": "test",
        "SYSTEM": "system",
    }

    @classmethod
    def get_component_logger(cls, component: str, level: str = "INFO") -> logging.Logger:
        """Get a logger for a specific component"""

        logger = logging.getLogger(component)
        logger.setLevel(getattr(logging, level.upper()))
        logger.propagate = False

        if logger.handlers:
            return logger

        log_file = LOG_DIR / f"{component}.log"

        file_handler = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)-15s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(file_format)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_format = ColoredFormatter("%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%H:%M:%S")
        console_handler.setFormatter(console_format)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger


def get_logger(name: str = "SYSTEM", level: str = "INFO") -> logging.Logger:
    """Get a logger with the specified name and level"""
    return LogClassifier.get_component_logger(name.upper(), level)


class StructuredLogger:
    """Logger with structured logging support"""

    def __init__(self, component: str):
        self.logger = get_logger(component)
        self.component = component

    def log(self, level: str, message: str, **kwargs):
        """Log with additional structured data"""
        extra = " | ".join(f"{k}={v}" for k, v in kwargs.items()) if kwargs else ""
        msg = f"{message} | {extra}" if extra else message
        getattr(self.logger, level.lower())(msg)

    def info(self, message: str, **kwargs):
        self.log("INFO", message, **kwargs)

    def error(self, message: str, **kwargs):
        self.log("ERROR", message, **kwargs)

    def warning(self, message: str, **kwargs):
        self.log("WARNING", message, **kwargs)

    def debug(self, message: str, **kwargs):
        self.log("DEBUG", message, **kwargs)


def setup_dag_logger(dag_id: str) -> logging.Logger:
    """Setup logger for Airflow DAG"""
    logger = logging.getLogger(f"dag.{dag_id}")
    logger.setLevel(logging.INFO)

    dag_log_file = LOG_DIR / "dags" / f"{dag_id}.log"
    dag_log_file.parent.mkdir(parents=True, exist_ok=True)

    handler = RotatingFileHandler(dag_log_file, maxBytes=5 * 1024 * 1024, backupCount=3)
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))

    logger.addHandler(handler)
    return logger


class LogContext:
    """Context manager for temporary log levels"""

    def __init__(self, logger: logging.Logger, level: str):
        self.logger = logger
        self.new_level = getattr(logging, level.upper())
        self.old_level = None

    def __enter__(self):
        self.old_level = self.logger.level
        self.logger.setLevel(self.new_level)
        return self.logger

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.old_level is not None:
            self.logger.setLevel(self.old_level)


if __name__ == "__main__":
    logger = get_logger("test")
    logger.info("Testing the logging system")

    structured = StructuredLogger("generator")
    structured.info("Processing file", rows=1000, file="sales.parquet")
