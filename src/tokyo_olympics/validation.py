"""A lightweight, dependency-free data-quality framework.

Each :class:`Rule` evaluates a DataFrame and returns a :class:`ValidationResult`.
A :class:`ValidationSuite` runs many rules, logs every outcome, and (optionally)
raises :class:`DataQualityError` when a *critical* rule fails — giving the
pipeline a real quality gate between layers.

This is intentionally small and self-contained; for larger estates it maps
cleanly onto Great Expectations or Databricks DLT ``expectations``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .logging_utils import get_logger

logger = get_logger(__name__)


class DataQualityError(Exception):
    """Raised when one or more critical data-quality rules fail."""


@dataclass(frozen=True)
class ValidationResult:
    rule: str
    dataset: str
    passed: bool
    critical: bool
    observed: Any
    detail: str


class Rule(ABC):
    """Base class for a single data-quality assertion."""

    def __init__(self, *, critical: bool = True) -> None:
        self.critical = critical

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def evaluate(self, df: DataFrame, dataset: str) -> ValidationResult: ...

    def _result(self, dataset: str, passed: bool, observed: Any, detail: str) -> ValidationResult:
        return ValidationResult(self.name, dataset, passed, self.critical, observed, detail)


class ColumnsExist(Rule):
    """Assert that the expected columns are present."""

    def __init__(self, columns: list[str], *, critical: bool = True) -> None:
        super().__init__(critical=critical)
        self.columns = columns

    def evaluate(self, df: DataFrame, dataset: str) -> ValidationResult:
        missing = [c for c in self.columns if c not in df.columns]
        return self._result(dataset, not missing, missing, f"missing columns: {missing or 'none'}")


class NotNull(Rule):
    """Assert that the given columns contain no nulls."""

    def __init__(self, columns: list[str], *, critical: bool = True) -> None:
        super().__init__(critical=critical)
        self.columns = columns

    def evaluate(self, df: DataFrame, dataset: str) -> ValidationResult:
        counts = df.select(
            [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in self.columns]
        ).first()
        # An un-grouped aggregation always yields exactly one row.
        assert counts is not None
        offenders = {c: counts[c] for c in self.columns if (counts[c] or 0) > 0}
        return self._result(
            dataset, not offenders, offenders, f"null counts: {offenders or 'none'}"
        )


class Unique(Rule):
    """Assert that the combination of columns is unique (no duplicate keys)."""

    def __init__(self, columns: list[str], *, critical: bool = True) -> None:
        super().__init__(critical=critical)
        self.columns = columns

    def evaluate(self, df: DataFrame, dataset: str) -> ValidationResult:
        total = df.count()
        distinct = df.select(*self.columns).distinct().count()
        dupes = total - distinct
        return self._result(dataset, dupes == 0, dupes, f"{dupes} duplicate rows on {self.columns}")


class MinRowCount(Rule):
    """Assert the table has at least ``min_rows`` rows (catches empty loads)."""

    def __init__(self, min_rows: int, *, critical: bool = True) -> None:
        super().__init__(critical=critical)
        self.min_rows = min_rows

    def evaluate(self, df: DataFrame, dataset: str) -> ValidationResult:
        count = df.count()
        return self._result(
            dataset, count >= self.min_rows, count, f"{count} rows (min {self.min_rows})"
        )


class NonNegative(Rule):
    """Assert numeric columns have no negative values."""

    def __init__(self, columns: list[str], *, critical: bool = True) -> None:
        super().__init__(critical=critical)
        self.columns = columns

    def evaluate(self, df: DataFrame, dataset: str) -> ValidationResult:
        condition = None
        for c in self.columns:
            clause = F.col(c) < 0
            condition = clause if condition is None else (condition | clause)
        bad = df.filter(condition).count() if condition is not None else 0
        return self._result(dataset, bad == 0, bad, f"{bad} rows with negative {self.columns}")


@dataclass
class ValidationSuite:
    """Run a set of rules against a dataset and aggregate the outcome."""

    dataset: str
    rules: list[Rule] = field(default_factory=list)

    def run(self, df: DataFrame, *, fail_fast: bool = True) -> list[ValidationResult]:
        results: list[ValidationResult] = []
        # Cache: most rules trigger an action, so we materialise once.
        df.cache()
        try:
            for rule in self.rules:
                result = rule.evaluate(df, self.dataset)
                results.append(result)
                level = logger.info if result.passed else logger.error
                level(
                    "DQ [%s] %s -> %s (%s)",
                    self.dataset,
                    result.rule,
                    "PASS" if result.passed else "FAIL",
                    result.detail,
                )
        finally:
            df.unpersist()

        critical_failures = [r for r in results if not r.passed and r.critical]
        if critical_failures and fail_fast:
            names = ", ".join(r.rule for r in critical_failures)
            raise DataQualityError(f"Critical data-quality failures on '{self.dataset}': {names}")
        return results
