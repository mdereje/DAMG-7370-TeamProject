# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 01 — Extract From Source (Connectivity Validation)
# MAGIC Validates that the Chinook Azure SQL source is reachable via Connection Manager federation
# MAGIC and that all active tables in the parent metadata can be queried.
# MAGIC
# MAGIC This notebook does NOT pass data to downstream notebooks — each notebook reads the source
# MAGIC independently. Temp views do not persist across notebook boundaries in a Databricks Job.

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

catalog_name   = dbutils.widgets.get("destination_catalog")
schema_name    = dbutils.widgets.get("destination_schema")
source_catalog = dbutils.widgets.get("source_catalog")
source_schema  = dbutils.widgets.get("source_schema")

print(f"catalog_name   : {catalog_name}")
print(f"schema_name    : {schema_name}")
print(f"source_catalog : {source_catalog}")
print(f"source_schema  : {source_schema}")

# COMMAND ----------

# MAGIC %md ## Read Active Tables from Parent Metadata

# COMMAND ----------

parent_df = spark.table(f"{catalog_name}.{schema_name}.pipeline_metadata_parent")

active_tables = (
    parent_df
    .filter("active_flag = 'Y'")
    .select("table_name", "file_name")
    .collect()
)

print(f"Active tables found in metadata: {[r.table_name for r in active_tables]}")

if not active_tables:
    raise Exception("No active tables found in pipeline_metadata_parent. Aborting pipeline.")

# COMMAND ----------

# MAGIC %md ## Validate Connectivity — Query Each Source Table

# COMMAND ----------

failed_tables = []

for row in active_tables:
    table_name = row.table_name
    fq_source  = f"{source_catalog}.{source_schema}.{table_name}"
    try:
        count = spark.read.table(fq_source).limit(1).count()
        print(f"  OK  {fq_source}")
    except Exception as e:
        print(f"  FAIL {fq_source}: {e}")
        failed_tables.append(table_name)

# COMMAND ----------

# MAGIC %md ## Fail Fast if Any Source Table is Unreachable

# COMMAND ----------

if failed_tables:
    raise Exception(
        f"Source connectivity check failed for: {failed_tables}. "
        "Check the Connection Manager connection and firewall rules before proceeding."
    )

print(f"\nAll {len(active_tables)} source tables reachable. Pipeline may proceed.")
