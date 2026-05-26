"""Bronze layer — raw, schema-validated ingestion with lineage metadata.

Bronze is a faithful copy of the source (no business logic) plus operational
columns so every downstream row is traceable to where and when it landed. This
auditability is something the original single-pass notebook had no concept of.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def to_bronze(df: DataFrame, source_file: str) -> DataFrame:
    """Add ingestion lineage columns to a freshly read source DataFrame."""
    return df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
        "_source_file", F.lit(source_file)
    )
