# Legacy notebook (deprecated)

`Tokyo Olympic Transformation.ipynb` is the **original** single-notebook
implementation, kept here only for historical reference.

It is superseded by the `tokyo_olympics` package and the thin orchestration
notebook at `notebooks/01_run_pipeline.py`. Do not run or extend it.

Known issues that motivated the rewrite:
- Service-principal credentials pasted inline in source.
- `inferSchema=true` everywhere (slow, non-deterministic types).
- No data validation, logging, tests, or error handling.
- Output written as headered CSV instead of Delta.
- No separation between raw, cleaned, and curated data.
