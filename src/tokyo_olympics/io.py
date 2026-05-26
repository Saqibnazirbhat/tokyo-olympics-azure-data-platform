"""Reusable read/write helpers.

All curated data is persisted as **Delta** (ACID, schema enforcement, time
travel) instead of the original notebook's headered CSV. Writes are idempotent
(``overwrite``) so a re-run reproduces the same state — a core ETL property the
original lacked.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from .logging_utils import get_logger

logger = get_logger(__name__)


def read_csv(
    spark: SparkSession,
    path: str,
    schema: StructType,
    *,
    encoding: str = "UTF-8",
) -> DataFrame:
    """Read a raw CSV with an explicit schema (no inference).

    ``encoding`` matters: several source files are ISO-8859, which is why the
    raw data shows mojibake like ``C�te d'Ivoire`` when read as UTF-8.
    """
    logger.info("Reading CSV %s (encoding=%s).", path, encoding)
    return (
        spark.read.option("header", "true")
        .option("encoding", encoding)
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(path)
    )


def write_delta(
    df: DataFrame,
    path: str,
    *,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
) -> None:
    """Write a DataFrame as a Delta table (idempotent overwrite by default)."""
    logger.info("Writing Delta table to %s (mode=%s, partition_by=%s).", path, mode, partition_by)
    writer = df.write.format("delta").mode(mode).option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    """Read a Delta table."""
    logger.info("Reading Delta table from %s.", path)
    return spark.read.format("delta").load(path)
