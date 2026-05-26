"""Tokyo 2020 Olympics — enterprise medallion data pipeline.

A config-driven PySpark/Delta Lake pipeline that ingests raw Olympics CSVs into
Bronze, cleans and conforms them into Silver, and builds curated analytics tables
in Gold. Designed to run on Azure Databricks against ADLS Gen2, and locally
against ./data for development and CI.
"""

__version__ = "1.0.0"
