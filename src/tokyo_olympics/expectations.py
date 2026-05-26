"""Per-dataset data-quality expectations applied at the silver boundary.

Keeping the expectations declarative and in one place makes the contract for
each table obvious and reviewable, and lets the pipeline gate on them before
anything reaches the gold layer.
"""

from __future__ import annotations

from .validation import (
    ColumnsExist,
    MinRowCount,
    NonNegative,
    NotNull,
    Unique,
    ValidationSuite,
)

# Builders so each suite is constructed fresh (rules are cheap, stateless).
_SUITE_FACTORIES = {
    "athletes": lambda: ValidationSuite(
        "athletes",
        [
            ColumnsExist(["person_name", "country", "discipline"]),
            NotNull(["person_name", "country"]),
            MinRowCount(1000),
            Unique(["person_name", "country", "discipline"]),
        ],
    ),
    "coaches": lambda: ValidationSuite(
        "coaches",
        [
            ColumnsExist(["coach_name", "country", "discipline", "event"]),
            NotNull(["coach_name", "country"]),
            MinRowCount(100),
        ],
    ),
    "entriesgender": lambda: ValidationSuite(
        "entriesgender",
        [
            ColumnsExist(["discipline", "female", "male", "total"]),
            NotNull(["discipline"]),
            NonNegative(["female", "male", "total"]),
            Unique(["discipline"]),
        ],
    ),
    "medals": lambda: ValidationSuite(
        "medals",
        [
            ColumnsExist(["country", "gold", "silver", "bronze", "total"]),
            NotNull(["country"]),
            NonNegative(["gold", "silver", "bronze", "total"]),
            Unique(["country"]),
        ],
    ),
    "teams": lambda: ValidationSuite(
        "teams",
        [
            ColumnsExist(["team_name", "discipline", "country", "event"]),
            NotNull(["team_name", "country"]),
            MinRowCount(100),
        ],
    ),
}


def suite_for(dataset: str) -> ValidationSuite:
    """Return a fresh :class:`ValidationSuite` for a silver dataset."""
    return _SUITE_FACTORIES[dataset]()
