"""Spark session lifecycle and ADLS Gen2 authentication.

On Databricks a session already exists and Delta is built in, so we reuse it.
Locally we build a Delta-enabled session via ``delta-spark`` so the exact same
pipeline code runs in tests and on a laptop.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from .config import AppConfig
from .logging_utils import get_logger
from .secrets import SecretProvider

logger = get_logger(__name__)


def get_spark(config: AppConfig) -> SparkSession:
    """Return a configured :class:`SparkSession`.

    Reuses the active session if one exists (Databricks); otherwise builds a
    local Delta-enabled session.
    """
    active = SparkSession.getActiveSession()
    if active is not None:
        logger.info("Reusing active SparkSession (Databricks or prior local run).")
        spark = active
    else:
        logger.info("Building local Delta-enabled SparkSession '%s'.", config.spark.app_name)
        builder = (
            SparkSession.builder.appName(config.spark.app_name)
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        try:
            # Pulls the matching Delta JARs when run outside Databricks.
            from delta import configure_spark_with_delta_pip

            spark = configure_spark_with_delta_pip(builder).getOrCreate()
        except ImportError:  # pragma: no cover - delta always present in our deps
            spark = builder.getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", str(config.spark.shuffle_partitions))
    spark.sparkContext.setLogLevel("WARN")
    return spark


def configure_adls_oauth(spark: SparkSession, config: AppConfig, secrets: SecretProvider) -> None:
    """Wire up OAuth (service principal) access to ADLS Gen2.

    Skipped entirely for local runs. Credentials are read through the
    :class:`SecretProvider`, never hardcoded.
    """
    if config.storage.use_local:
        logger.info("Local storage in use — skipping ADLS OAuth configuration.")
        return

    tenant_id = secrets.get(config.secrets.tenant_id_key)
    client_id = secrets.get(config.secrets.client_id_key)
    client_secret = secrets.get(config.secrets.client_secret_key)
    account = config.storage.account_name
    prefix = "fs.azure.account"

    conf = {
        f"{prefix}.auth.type.{account}.dfs.core.windows.net": "OAuth",
        f"{prefix}.oauth.provider.type.{account}.dfs.core.windows.net": (
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
        ),
        f"{prefix}.oauth2.client.id.{account}.dfs.core.windows.net": client_id,
        f"{prefix}.oauth2.client.secret.{account}.dfs.core.windows.net": client_secret,
        f"{prefix}.oauth2.client.endpoint.{account}.dfs.core.windows.net": (
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
        ),
    }
    for key, value in conf.items():
        spark.conf.set(key, value)
    logger.info("Configured ADLS Gen2 OAuth for account '%s'.", account)
