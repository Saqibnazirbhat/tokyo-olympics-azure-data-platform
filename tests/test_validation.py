"""Tests for the data-quality framework."""

from __future__ import annotations

import pytest

from tokyo_olympics.validation import (
    ColumnsExist,
    DataQualityError,
    MinRowCount,
    NonNegative,
    NotNull,
    Unique,
    ValidationSuite,
)

pytestmark = pytest.mark.spark


@pytest.fixture
def df(spark):
    return spark.createDataFrame(
        [("USA", 39, 41), ("China", 38, 32), ("Japan", None, 14)],
        ["country", "gold", "silver"],
    )


def test_columns_exist_pass_and_fail(df):
    assert ColumnsExist(["country", "gold"]).evaluate(df, "medals").passed
    result = ColumnsExist(["country", "missing"]).evaluate(df, "medals")
    assert not result.passed
    assert "missing" in result.observed


def test_not_null_detects_nulls(df):
    assert NotNull(["country"]).evaluate(df, "medals").passed
    result = NotNull(["gold"]).evaluate(df, "medals")
    assert not result.passed
    assert result.observed == {"gold": 1}


def test_unique(df):
    assert Unique(["country"]).evaluate(df, "medals").passed
    dupes = df.union(df)
    assert not Unique(["country"]).evaluate(dupes, "medals").passed


def test_min_row_count(df):
    assert MinRowCount(3).evaluate(df, "medals").passed
    assert not MinRowCount(10).evaluate(df, "medals").passed


def test_non_negative(spark):
    bad = spark.createDataFrame([(1,), (-2,)], ["gold"])
    result = NonNegative(["gold"]).evaluate(bad, "medals")
    assert not result.passed
    assert result.observed == 1


def test_suite_fail_fast_raises(df):
    suite = ValidationSuite("medals", [NotNull(["gold"], critical=True)])
    with pytest.raises(DataQualityError):
        suite.run(df, fail_fast=True)


def test_suite_warn_mode_does_not_raise(df):
    suite = ValidationSuite("medals", [NotNull(["gold"], critical=True)])
    results = suite.run(df, fail_fast=False)
    assert any(not r.passed for r in results)


def test_non_critical_failure_never_raises(df):
    suite = ValidationSuite("medals", [NotNull(["gold"], critical=False)])
    results = suite.run(df, fail_fast=True)  # would raise if treated as critical
    assert any(not r.passed for r in results)
