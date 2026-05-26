"""Tests for gold-layer business marts."""

from __future__ import annotations

import pytest

from tokyo_olympics.transformations.gold import (
    gold_country_performance,
    gold_gender_participation,
    gold_medal_standings,
)

pytestmark = pytest.mark.spark


def _medals(spark):
    return spark.createDataFrame(
        [("USA", 39, 41, 33), ("China", 38, 32, 18)],
        ["country", "gold", "silver", "bronze"],
    )


def test_medal_standings_points_and_rank(spark):
    out = gold_medal_standings({"medals": _medals(spark)})
    usa = out.filter(out.country == "USA").first()
    # 39*3 + 41*2 + 33 = 117 + 82 + 33 = 232
    assert usa["medal_points"] == 232
    assert usa["total_medals"] == 113
    assert usa["rank_by_points"] == 1


def test_gender_participation_percentages(spark):
    entries = spark.createDataFrame(
        [("Athletics", 969, 1072, 2041), ("Empty", 0, 0, 0)],
        ["discipline", "female", "male", "total"],
    )
    out = gold_gender_participation({"entriesgender": entries})
    athletics = out.filter(out.discipline == "Athletics").first()
    assert athletics["female_pct"] == pytest.approx(47.48, abs=0.01)
    # Zero total must not divide-by-zero; it yields NULL, not an error.
    empty = out.filter(out.discipline == "Empty").first()
    assert empty["female_pct"] is None


def test_country_performance_efficiency(spark):
    athletes = spark.createDataFrame(
        [("USA", "A", "Swim"), ("USA", "B", "Judo"), ("Tonga", "C", "Judo")],
        ["country", "person_name", "discipline"],
    )
    out = gold_country_performance({"medals": _medals(spark), "athletes": athletes})
    usa = out.filter(out.country == "USA").first()
    # 113 medals / 2 athletes * 100 = 5650.0
    assert usa["medals_per_100_athletes"] == pytest.approx(5650.0)
    # Country with athletes but no medal row -> 0, not NULL.
    tonga = out.filter(out.country == "Tonga").first()
    assert tonga["total_medals"] == 0
