"""End-to-end medallion pipeline orchestrator + CLI entry point.

Stages:
    1. Bronze  — read each raw CSV with an explicit schema + encoding, stamp
                 lineage, persist as Delta.
    2. Silver  — clean/conform, run the data-quality gate, persist as Delta.
    3. Gold    — build curated analytics marts, persist as Delta.

Run locally:   python -m tokyo_olympics.pipeline --env dev
On Databricks: packaged as a wheel and invoked via the `tokyo-pipeline` entry
point by the Databricks Asset Bundle job.
"""

from __future__ import annotations

import argparse
import sys
import time

from pyspark.sql import DataFrame, SparkSession

from .config import AppConfig, load_config
from .expectations import suite_for
from .io import read_csv, read_delta, write_delta
from .logging_utils import get_logger
from .schemas import RAW_SCHEMAS
from .secrets import resolve_secret_provider
from .spark import configure_adls_oauth, get_spark
from .transformations.bronze import to_bronze
from .transformations.gold import GOLD_BUILDERS
from .transformations.silver import SILVER_TRANSFORMERS

logger = get_logger(__name__)


def run_bronze(spark: SparkSession, config: AppConfig) -> None:
    logger.info("=== BRONZE: ingesting %d raw datasets ===", len(config.dataset_names))
    for dataset in config.dataset_names:
        raw = read_csv(
            spark,
            config.raw_path(dataset),
            RAW_SCHEMAS[dataset],
            encoding=config.raw_encoding(dataset),
        )
        bronze = to_bronze(raw, source_file=config.datasets[dataset]["raw_file"])
        write_delta(bronze, config.layer_path("bronze", dataset))


def run_silver(spark: SparkSession, config: AppConfig) -> None:
    logger.info("=== SILVER: cleaning, validating, conforming ===")
    for dataset, transform in SILVER_TRANSFORMERS.items():
        bronze = read_delta(spark, config.layer_path("bronze", dataset))
        silver = transform(bronze)

        # Quality gate: fail the run on critical issues (per env config).
        suite_for(dataset).run(silver, fail_fast=config.data_quality.fail_fast)

        write_delta(silver, config.layer_path("silver", dataset))


def run_gold(spark: SparkSession, config: AppConfig) -> None:
    logger.info("=== GOLD: building %d analytics marts ===", len(GOLD_BUILDERS))
    silver: dict[str, DataFrame] = {
        name: read_delta(spark, config.layer_path("silver", name)) for name in SILVER_TRANSFORMERS
    }
    for table_name, build in GOLD_BUILDERS.items():
        gold = build(silver)
        write_delta(gold, config.layer_path("gold", table_name))


def run_pipeline(config: AppConfig, dbutils: object | None = None) -> None:
    """Execute the full bronze -> silver -> gold flow."""
    start = time.monotonic()
    logger.info("Starting Tokyo Olympics pipeline (env=%s).", config.env)

    spark = get_spark(config)
    secrets = resolve_secret_provider(config.secrets, dbutils=dbutils)
    configure_adls_oauth(spark, config, secrets)

    run_bronze(spark, config)
    run_silver(spark, config)
    run_gold(spark, config)

    elapsed = time.monotonic() - start
    logger.info("Pipeline completed successfully in %.1fs.", elapsed)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Tokyo Olympics medallion pipeline.")
    parser.add_argument(
        "--env",
        default=None,
        help="Environment to run (dev|prod). Defaults to $TOKYO_ENV or 'dev'.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Console-script entry point (`tokyo-pipeline`)."""
    args = _parse_args(argv)
    config = load_config(env=args.env)
    try:
        run_pipeline(config)
    except Exception:
        logger.exception("Pipeline failed.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
