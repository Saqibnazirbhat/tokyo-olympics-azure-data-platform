# Databricks notebook source
# MAGIC %md
# MAGIC # Tokyo Olympics — Medallion Pipeline (orchestration)
# MAGIC
# MAGIC This notebook is intentionally **thin**. All logic lives in the installed
# MAGIC `tokyo_olympics` wheel so it is version-controlled, unit-tested, and reused
# MAGIC identically by the scheduled Databricks Job. The notebook only:
# MAGIC
# MAGIC 1. reads the target environment from a widget, and
# MAGIC 2. calls `run_pipeline`, passing the notebook's `dbutils` so secrets are
# MAGIC    resolved from the Key Vault-backed secret scope.
# MAGIC
# MAGIC > Install the wheel on the cluster first (Asset Bundle does this automatically),
# MAGIC > or `%pip install` it during development.

# COMMAND ----------

# MAGIC %pip install /Workspace/Shared/wheels/tokyo_olympics-1.0.0-py3-none-any.whl

# COMMAND ----------

dbutils.widgets.dropdown("env", "prod", ["dev", "prod"], "Environment")

# COMMAND ----------

from tokyo_olympics.config import load_config
from tokyo_olympics.pipeline import run_pipeline

config = load_config(env=dbutils.widgets.get("env"))

# `dbutils` routes secret resolution through the Key Vault-backed scope.
run_pipeline(config, dbutils=dbutils)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect the gold marts

# COMMAND ----------

display(spark.read.format("delta").load(config.layer_path("gold", "medal_standings")))
