"""Shared pytest fixtures — a local Delta-enabled Spark session."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Session-scoped local Spark with Delta enabled.

    Building a session is expensive, so we reuse one across the whole test run.
    """
    builder = (
        SparkSession.builder.appName("tokyo-olympics-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    try:
        from delta import configure_spark_with_delta_pip

        session = configure_spark_with_delta_pip(builder).getOrCreate()
    except ImportError:
        session = builder.getOrCreate()

    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
