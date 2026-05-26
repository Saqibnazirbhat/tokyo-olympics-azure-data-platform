"""Gold layer — curated, analytics-ready business marts.

Each builder takes the dict of silver DataFrames and returns one gold table.
These formalise (and extend) the two throwaway queries in the original notebook
— "top gold medals" and "average entries by gender" — into governed, reusable
tables a BI tool (Power BI / Synapse) can sit on directly.
"""

from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Weighted scoring: gold worth more than silver worth more than bronze.
_GOLD_POINTS, _SILVER_POINTS, _BRONZE_POINTS = 3, 2, 1


def gold_medal_standings(silver: dict[str, DataFrame]) -> DataFrame:
    """Medal table enriched with a weighted points score and derived ranks."""
    medals = silver["medals"]
    scored = medals.withColumn(
        "medal_points",
        F.col("gold") * _GOLD_POINTS
        + F.col("silver") * _SILVER_POINTS
        + F.col("bronze") * _BRONZE_POINTS,
    ).withColumn("total_medals", F.col("gold") + F.col("silver") + F.col("bronze"))

    by_points = Window.orderBy(F.col("medal_points").desc())
    by_total = Window.orderBy(F.col("total_medals").desc())
    return (
        scored.withColumn("rank_by_points", F.rank().over(by_points))
        .withColumn("rank_by_total_medals", F.rank().over(by_total))
        .select(
            "country",
            "gold",
            "silver",
            "bronze",
            "total_medals",
            "medal_points",
            "rank_by_points",
            "rank_by_total_medals",
        )
        .orderBy("rank_by_points")
    )


def gold_gender_participation(silver: dict[str, DataFrame]) -> DataFrame:
    """Per-discipline gender split with participation percentages."""
    entries = silver["entriesgender"]
    safe_total = F.when(F.col("total") == 0, None).otherwise(F.col("total"))
    return (
        entries.withColumn("female_pct", F.round(F.col("female") / safe_total * 100, 2))
        .withColumn("male_pct", F.round(F.col("male") / safe_total * 100, 2))
        .select("discipline", "female", "male", "total", "female_pct", "male_pct")
        .orderBy(F.col("total").desc())
    )


def gold_athletes_by_country(silver: dict[str, DataFrame]) -> DataFrame:
    """Athlete headcount and discipline breadth per country."""
    athletes = silver["athletes"]
    return (
        athletes.groupBy("country")
        .agg(
            F.count("*").alias("athlete_count"),
            F.countDistinct("discipline").alias("discipline_count"),
        )
        .orderBy(F.col("athlete_count").desc())
    )


def gold_country_performance(silver: dict[str, DataFrame]) -> DataFrame:
    """Medals vs. squad size: efficiency (medals per 100 athletes).

    A genuinely new insight the original notebook never produced — it joins two
    datasets the notebook only ever read independently.
    """
    medals = silver["medals"].select(
        "country",
        (F.col("gold") + F.col("silver") + F.col("bronze")).alias("total_medals"),
    )
    athletes = silver["athletes"].groupBy("country").agg(F.count("*").alias("athlete_count"))
    joined = athletes.join(medals, on="country", how="left").fillna({"total_medals": 0})
    return joined.withColumn(
        "medals_per_100_athletes",
        F.round(F.col("total_medals") / F.col("athlete_count") * 100, 2),
    ).orderBy(F.col("medals_per_100_athletes").desc())


# Registry consumed by the pipeline's gold stage.
GOLD_BUILDERS: dict[str, Callable[[dict[str, DataFrame]], DataFrame]] = {
    "medal_standings": gold_medal_standings,
    "gender_participation": gold_gender_participation,
    "athletes_by_country": gold_athletes_by_country,
    "country_performance": gold_country_performance,
}
