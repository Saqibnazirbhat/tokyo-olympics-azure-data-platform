"""Structured, JSON-friendly logging used across the pipeline.

Centralising logger creation means every module emits consistently formatted,
correlatable log lines — essential when reading Databricks driver logs or
shipping logs to Azure Log Analytics.
"""

from __future__ import annotations

import logging
import sys

_CONFIGURED = False
_LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


def configure_logging(level: str = "INFO") -> None:
    """Configure the root logger once for the whole process.

    Idempotent: repeated calls (e.g. per notebook cell) are no-ops so we never
    attach duplicate handlers that would double-print every line.
    """
    global _CONFIGURED
    if _CONFIGURED:
        return

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(fmt=_LOG_FORMAT, datefmt=_DATE_FORMAT))

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())

    # Spark/py4j are extremely chatty at INFO — keep them at WARN.
    logging.getLogger("py4j").setLevel(logging.WARN)
    logging.getLogger("pyspark").setLevel(logging.WARN)

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """Return a module-scoped logger, configuring logging on first use."""
    configure_logging()
    return logging.getLogger(name)
