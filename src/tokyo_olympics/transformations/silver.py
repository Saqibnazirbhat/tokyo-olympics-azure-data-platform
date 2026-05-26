"""Silver layer — cleaned, conformed, de-duplicated business entities.

Responsibilities:
* snake_case, business-friendly column names;
* trim whitespace and convert empty strings to NULL;
* drop duplicate rows;
* enforce the not-null invariants the warehouse depends on.

Each transformer takes a bronze DataFrame and returns the silver DataFrame.
"""

from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

_LINEAGE_COLS = ["_ingested_at", "_source_file"]


def _clean_string(col: Column) -> Column:
    """Trim, collapse internal whitespace, and NULL-out blanks."""
    trimmed = F.trim(F.regexp_replace(col, r"\s+", " "))
    return F.when(trimmed == "", None).otherwise(trimmed)


def _clean_all_strings(df: DataFrame) -> DataFrame:
    """Apply :func:`_clean_string` to every string column (except lineage)."""
    for f in df.schema.fields:
        if isinstance(f.dataType, StringType) and f.name not in _LINEAGE_COLS:
            df = df.withColumn(f.name, _clean_string(F.col(f.name)))
    return df


def _rename(df: DataFrame, mapping: dict[str, str]) -> DataFrame:
    for src, dst in mapping.items():
        df = df.withColumnRenamed(src, dst)
    return df


def silver_athletes(df: DataFrame) -> DataFrame:
    renamed = _rename(
        df,
        {"PersonName": "person_name", "Country": "country", "Discipline": "discipline"},
    )
    cleaned = _clean_all_strings(renamed)
    return cleaned.dropna(subset=["person_name", "country"]).dropDuplicates(
        ["person_name", "country", "discipline"]
    )


def silver_coaches(df: DataFrame) -> DataFrame:
    renamed = _rename(
        df,
        {
            "Name": "coach_name",
            "Country": "country",
            "Discipline": "discipline",
            "Event": "event",
        },
    )
    cleaned = _clean_all_strings(renamed)
    return cleaned.dropna(subset=["coach_name", "country"]).dropDuplicates(
        ["coach_name", "country", "discipline", "event"]
    )


def silver_entriesgender(df: DataFrame) -> DataFrame:
    renamed = _rename(
        df,
        {"Discipline": "discipline", "Female": "female", "Male": "male", "Total": "total"},
    )
    cleaned = _clean_all_strings(renamed)
    # Replace missing counts with 0 so downstream arithmetic never yields NULL.
    for c in ("female", "male", "total"):
        cleaned = cleaned.withColumn(c, F.coalesce(F.col(c), F.lit(0)))
    return cleaned.dropna(subset=["discipline"]).dropDuplicates(["discipline"])


def silver_medals(df: DataFrame) -> DataFrame:
    renamed = _rename(
        df,
        {
            "Rank": "rank",
            "TeamCountry": "country",
            "Gold": "gold",
            "Silver": "silver",
            "Bronze": "bronze",
            "Total": "total",
            "Rank by Total": "rank_by_total",
        },
    )
    cleaned = _clean_all_strings(renamed)
    for c in ("gold", "silver", "bronze", "total"):
        cleaned = cleaned.withColumn(c, F.coalesce(F.col(c), F.lit(0)))
    return cleaned.dropna(subset=["country"]).dropDuplicates(["country"])


def silver_teams(df: DataFrame) -> DataFrame:
    renamed = _rename(
        df,
        {
            "TeamName": "team_name",
            "Discipline": "discipline",
            "Country": "country",
            "Event": "event",
        },
    )
    cleaned = _clean_all_strings(renamed)
    return cleaned.dropna(subset=["team_name", "country"]).dropDuplicates(
        ["team_name", "discipline", "country", "event"]
    )


# Registry consumed by the pipeline's silver stage.
SILVER_TRANSFORMERS: dict[str, Callable[[DataFrame], DataFrame]] = {
    "athletes": silver_athletes,
    "coaches": silver_coaches,
    "entriesgender": silver_entriesgender,
    "medals": silver_medals,
    "teams": silver_teams,
}
