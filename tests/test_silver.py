"""Tests for silver-layer cleaning/conforming."""

from __future__ import annotations

import pytest

from tokyo_olympics.schemas import ENTRIES_GENDER_SCHEMA, MEDALS_SCHEMA
from tokyo_olympics.transformations.silver import (
    silver_athletes,
    silver_entriesgender,
    silver_medals,
)

pytestmark = pytest.mark.spark


def test_silver_athletes_renames_trims_and_dedupes(spark):
    raw = spark.createDataFrame(
        [
            ("  AALERUD Katrine ", "Norway", "Cycling Road"),
            ("AALERUD Katrine", "Norway", "Cycling Road"),  # dup after trim
            ("Empty Country", "", "Judo"),  # blank country -> dropped
        ],
        ["PersonName", "Country", "Discipline"],
    )
    out = silver_athletes(raw)
    assert set(out.columns) == {"person_name", "country", "discipline"}
    rows = out.collect()
    assert out.count() == 1
    assert rows[0]["person_name"] == "AALERUD Katrine"  # whitespace collapsed/trimmed


def test_silver_medals_renames_and_fills_nulls(spark):
    # Explicit schema: Spark can't infer a column whose only value is NULL.
    raw = spark.createDataFrame([(1, "USA", 39, 41, None, 113, 1)], schema=MEDALS_SCHEMA)
    out = silver_medals(raw)
    assert "country" in out.columns
    assert "rank_by_total" in out.columns
    assert out.first()["bronze"] == 0  # null coalesced to 0


def test_silver_entriesgender_coalesces_counts(spark):
    raw = spark.createDataFrame([("Archery", None, 64, 128)], schema=ENTRIES_GENDER_SCHEMA)
    out = silver_entriesgender(raw)
    assert out.first()["female"] == 0
