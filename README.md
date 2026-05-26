# Tokyo 2020 Olympics — Azure Data Engineering Platform

[![CI](https://img.shields.io/badge/CI-GitHub%20Actions-blue)](.github/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](pyproject.toml)
[![Spark](https://img.shields.io/badge/PySpark-3.4%2B-orange)](pyproject.toml)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-enabled-brightgreen)](https://delta.io)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A production-grade, **medallion-architecture** data pipeline that ingests, cleans,
validates, and curates the Tokyo 2020 Summer Olympics datasets on **Azure
Databricks + ADLS Gen2 + Delta Lake**.

The pipeline runs **identically on a laptop and in the cloud**: `dev` executes
end-to-end against local sample data with zero Azure dependencies, while `prod`
runs as a scheduled Databricks Job over ADLS Gen2.

---

## Table of contents
- [Highlights](#highlights)
- [Architecture](#architecture)
- [Project structure](#project-structure)
- [Datasets](#datasets)
- [Quick start (local)](#quick-start-local)
- [Running on Azure Databricks](#running-on-azure-databricks)
- [Configuration](#configuration)
- [Data quality](#data-quality)
- [Testing & CI/CD](#testing--cicd)
- [Gold-layer outputs](#gold-layer-outputs)

---

## Highlights

| Capability | What it delivers |
|------------|------------------|
| 🏗️ **Medallion architecture** | Raw → Bronze → Silver → Gold, each idempotent and independently re-runnable |
| 🧱 **Delta Lake** | ACID writes, schema enforcement, time travel — replaces fragile headered CSV |
| 📦 **Installable package** | Tested `tokyo_olympics` wheel; the notebook is a thin entry point only |
| 🔐 **Secret-safe** | Service-principal creds via Key Vault-backed scope or env vars — never committed |
| ✅ **Data-quality gate** | Declarative rule engine fails the run on critical issues before Gold |
| 📝 **Structured logging** | Consistent, parseable logs across every stage |
| ⚙️ **Config-driven** | `conf/*.yaml` + `TOKYO_*` env overrides; one code path, many environments |
| 🚀 **CI/CD** | GitHub Actions: lint, type-check, test, build wheel, deploy via Databricks Asset Bundles |

## Architecture

```text
 raw-data/ (CSV)  ─►  bronze/ (Delta)  ─►  silver/ (Delta)  ─►  gold/ (Delta)
 immutable land     schema + lineage     clean + DQ gate      analytics marts
```

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full diagram, layer
contract, and design rationale.

## Project structure

```text
.
├── conf/                       # Layered YAML config (base + per-env)
│   ├── base.yaml
│   ├── dev.yaml                # local sample-data run
│   └── prod.yaml               # Azure Databricks + ADLS Gen2
├── data/raw-data/              # Sample source CSVs (the raw landing zone)
├── docs/ARCHITECTURE.md
├── notebooks/
│   └── 01_run_pipeline.py      # thin Databricks orchestration notebook
├── src/tokyo_olympics/         # The installable, tested package
│   ├── config.py  logging_utils.py  secrets.py  spark.py
│   ├── schemas.py  io.py  validation.py  expectations.py  pipeline.py
│   └── transformations/        # bronze.py · silver.py · gold.py
├── tests/                      # pytest suite (config, validation, silver, gold)
├── .github/workflows/          # ci.yml · cd.yml
├── databricks.yml              # Databricks Asset Bundle (job + schedule)
├── pyproject.toml  Makefile  .pre-commit-config.yaml  .env.example
└── README.md
```

## Datasets

| Dataset | Grain | Notable issues handled |
|---------|-------|------------------------|
| `Athletes`      | one row per athlete | mixed encoding, whitespace, duplicates |
| `Coaches`       | one row per coach   | ISO-8859 encoding (`Côte d'Ivoire` mojibake), nullable event |
| `EntriesGender` | one row per discipline | string→int counts, null counts → 0 |
| `Medals`        | one row per country | column rename (`TeamCountry`, `Rank by Total`), null medals → 0 |
| `Teams`         | one row per team/event | encoding, duplicates |

## Quick start (local)

**Prerequisites:** Python 3.10–3.12 and a JDK (8/11/17) on `PATH` (PySpark needs a JVM).

```bash
# 1. Install the package + dev tooling
make install            # == pip install -e ".[dev]" && pre-commit install

# 2. Run the full Bronze→Silver→Gold pipeline against ./data
make pipeline-dev       # == TOKYO_ENV=dev python -m tokyo_olympics.pipeline --env dev

# 3. Run the test suite
make test
```

Outputs land in `data/bronze/`, `data/silver/`, and `data/gold/` as Delta tables
(git-ignored). Inspect a mart:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.read.format("delta").load("data/gold/medal_standings").show()
```

## Running on Azure Databricks

1. **Provision** an ADLS Gen2 account + container, a Databricks workspace, and a
   service principal with `Storage Blob Data Contributor` on the container.
2. **Store** the SP credentials in **Azure Key Vault** and back a Databricks
   **secret scope** named `tokyo-olympics` with keys `adls-tenant-id`,
   `adls-client-id`, `adls-client-secret`.
3. **Upload** the source CSVs to `raw-data/` in the container.
4. **Deploy** the job with the Databricks Asset Bundle:
   ```bash
   databricks bundle deploy -t prod
   databricks bundle run tokyo_medallion_job -t prod
   ```
   The bundle builds the wheel, creates an autoscaling job cluster, installs the
   wheel, and schedules a daily run (06:00 UTC). The thin notebook
   `notebooks/01_run_pipeline.py` is available for interactive runs.

> **Orchestration note:** the raw upload step is typically driven by **Azure Data
> Factory** (Copy activity) on a trigger, which then kicks off this Databricks
> job — a standard ADF → Databricks → ADLS pattern.

## Configuration

Resolution order (highest wins): **`TOKYO_*` env vars → `conf/<env>.yaml` → `conf/base.yaml`**.

```bash
export TOKYO_ENV=prod                       # select environment
export TOKYO_STORAGE__ACCOUNT_NAME=myacct    # nested override (storage.account_name)
export TOKYO_SPARK__SHUFFLE_PARTITIONS=64    # nested override (spark.shuffle_partitions)
```

Copy `.env.example` → `.env` for local credentials. **The populated `.env` is
git-ignored — never commit secrets.**

## Data quality

A declarative rule engine (`validation.py`) runs a per-dataset suite
(`expectations.py`) at the **silver boundary**, before anything reaches gold:

```python
ValidationSuite("medals", [
    ColumnsExist(["country", "gold", "silver", "bronze", "total"]),
    NotNull(["country"]),
    NonNegative(["gold", "silver", "bronze", "total"]),
    Unique(["country"]),
])
```

Critical failures abort the run when `data_quality.fail_fast` is `true` (default
in `prod`); otherwise they are logged as warnings (`dev`). Built-in rules:
`ColumnsExist`, `NotNull`, `Unique`, `MinRowCount`, `NonNegative` — and the
framework maps cleanly onto Great Expectations or Databricks DLT expectations.

## Testing & CI/CD

```bash
make lint        # ruff
make format      # black + ruff --fix
make typecheck   # mypy
make test        # pytest + coverage (incl. local Spark tests)
```

- **CI** (`.github/workflows/ci.yml`): on every push/PR — ruff, black, mypy,
  pytest (with a JVM for Spark tests), then builds the wheel.
- **CD** (`.github/workflows/cd.yml`): on GitHub Release — builds and deploys the
  Databricks Asset Bundle (`dev` for pre-releases, `prod` for full releases).

## Gold-layer outputs

| Table | Description |
|-------|-------------|
| `medal_standings`     | Medal table with a weighted points score (G=3, S=2, B=1) and derived ranks |
| `gender_participation`| Per-discipline female/male split with participation percentages |
| `athletes_by_country` | Athlete headcount and discipline breadth per country |
| `country_performance` | **Medals per 100 athletes** — joins medals × squad size for an efficiency metric the original analysis never produced |

