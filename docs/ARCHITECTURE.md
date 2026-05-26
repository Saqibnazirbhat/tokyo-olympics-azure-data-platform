# Architecture

## End-to-end data flow

```text
   ┌──────────────┐     ┌───────────────────────────────────────────────┐
   │  Source CSVs │     │              Azure Data Lake Storage Gen2        │
   │ (5 datasets) │     │  container: tokyo-olympic-data                   │
   └──────┬───────┘     │                                                  │
          │  upload     │   raw-data/  ──►  bronze/  ──►  silver/  ──► gold/│
          └────────────►│   (CSV)          (Delta)       (Delta)      (Delta)
                        └───────────────────────────────────────────────┘
                                          ▲
                                          │ OAuth (service principal)
                                          │ secrets via Key Vault scope
                        ┌─────────────────┴──────────────────┐
                        │        Azure Databricks Job          │
                        │  installs tokyo_olympics wheel       │
                        │  entry point: tokyo-pipeline         │
                        └─────────────────┬──────────────────┘
                                          │ deployed by
                        ┌─────────────────┴──────────────────┐
                        │   Databricks Asset Bundle (CD)       │
                        │   GitHub Actions on release          │
                        └─────────────────────────────────────┘

   Downstream: Power BI / Synapse Serverless query the gold Delta tables.
```

## Medallion layers

| Layer  | Format | Purpose | Key operations |
|--------|--------|---------|----------------|
| **Raw**    | CSV   | Immutable landing zone | Files dropped by upstream/ADF |
| **Bronze** | Delta | Faithful, schema-validated copy + lineage | Explicit schema read, `_ingested_at`, `_source_file` |
| **Silver** | Delta | Cleaned, conformed entities | snake_case rename, trim, NULL-blanks, de-dupe, **DQ gate** |
| **Gold**   | Delta | Curated analytics marts | Joins & aggregations: standings, gender split, efficiency |

## Why these choices

- **Medallion architecture** gives clear separation of concerns and lets each
  stage be re-run independently and idempotently.
- **Delta Lake** over CSV: ACID writes, schema enforcement/evolution, time
  travel, and `MERGE` support — none of which raw CSV offers.
- **Installable wheel, not notebook logic**: the same tested code runs in CI and
  on the cluster. The notebook is a thin entry point only.
- **Config-driven environments** (`conf/*.yaml` + `TOKYO_*` env vars): one code
  path, many environments; `dev` runs entirely on local sample data.
- **Secrets via Key Vault-backed scope**: no credential is ever committed.

## Module map

| Module | Responsibility |
|--------|----------------|
| `config.py`        | Typed, layered configuration |
| `logging_utils.py` | Centralised structured logging |
| `secrets.py`       | Databricks/Key Vault vs. env-var secret resolution |
| `spark.py`         | Session factory + ADLS OAuth wiring |
| `schemas.py`       | Explicit source schemas |
| `io.py`            | CSV read / Delta read-write helpers |
| `validation.py`    | Rule-based data-quality engine |
| `expectations.py`  | Per-dataset DQ suites |
| `transformations/` | `bronze.py`, `silver.py`, `gold.py` |
| `pipeline.py`      | Orchestrator + `tokyo-pipeline` CLI |
```
