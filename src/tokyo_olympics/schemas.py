"""Explicit source schemas for every raw dataset.

The original notebook used ``inferSchema=true``, which (a) triggers a full extra
read pass, and (b) silently changes column types as data drifts. Declaring
schemas explicitly makes ingestion deterministic, faster, and self-documenting,
and lets a schema mismatch fail loudly instead of corrupting downstream types.
"""

from __future__ import annotations

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

ATHLETES_SCHEMA = StructType(
    [
        StructField("PersonName", StringType(), nullable=False),
        StructField("Country", StringType(), nullable=False),
        StructField("Discipline", StringType(), nullable=True),
    ]
)

COACHES_SCHEMA = StructType(
    [
        StructField("Name", StringType(), nullable=False),
        StructField("Country", StringType(), nullable=False),
        StructField("Discipline", StringType(), nullable=True),
        StructField("Event", StringType(), nullable=True),
    ]
)

ENTRIES_GENDER_SCHEMA = StructType(
    [
        StructField("Discipline", StringType(), nullable=False),
        StructField("Female", IntegerType(), nullable=True),
        StructField("Male", IntegerType(), nullable=True),
        StructField("Total", IntegerType(), nullable=True),
    ]
)

MEDALS_SCHEMA = StructType(
    [
        StructField("Rank", IntegerType(), nullable=True),
        StructField("TeamCountry", StringType(), nullable=False),
        StructField("Gold", IntegerType(), nullable=True),
        StructField("Silver", IntegerType(), nullable=True),
        StructField("Bronze", IntegerType(), nullable=True),
        StructField("Total", IntegerType(), nullable=True),
        StructField("Rank by Total", IntegerType(), nullable=True),
    ]
)

TEAMS_SCHEMA = StructType(
    [
        StructField("TeamName", StringType(), nullable=False),
        StructField("Discipline", StringType(), nullable=True),
        StructField("Country", StringType(), nullable=False),
        StructField("Event", StringType(), nullable=True),
    ]
)

# Lookup used by the bronze ingestion loop.
RAW_SCHEMAS: dict[str, StructType] = {
    "athletes": ATHLETES_SCHEMA,
    "coaches": COACHES_SCHEMA,
    "entriesgender": ENTRIES_GENDER_SCHEMA,
    "medals": MEDALS_SCHEMA,
    "teams": TEAMS_SCHEMA,
}
