# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 02 — Load Raw
# MAGIC Reads directly from the Chinook Azure SQL source via Connection Manager federation,
# MAGIC writes immutable Parquet snapshots to the Chinook volume,
# MAGIC and logs every run to the child execution metrics table.
# MAGIC
# MAGIC NOTE: This notebook reads from source directly — temp views do not persist across
# MAGIC notebook boundaries in a Databricks Job.

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name",   "workspace")
dbutils.widgets.text("schema_name",    "raw_zone")
dbutils.widgets.text("base_path",      "/Volumes/workspace/raw_zone/chinook")
dbutils.widgets.text("source_catalog", "chinook_azure_catalog")
dbutils.widgets.text("source_schema",  "chinook")

catalog_name   = dbutils.widgets.get("catalog_name")
schema_name    = dbutils.widgets.get("schema_name")
base_path      = dbutils.widgets.get("base_path")
source_catalog = dbutils.widgets.get("source_catalog")
source_schema  = dbutils.widgets.get("source_schema")

print(f"catalog_name   : {catalog_name}")
print(f"schema_name    : {schema_name}")
print(f"base_path      : {base_path}")
print(f"source_catalog : {source_catalog}")
print(f"source_schema  : {source_schema}")

# COMMAND ----------

# MAGIC %md ## Imports

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import Row

run_ts     = datetime.now(timezone.utc)
run_date   = run_ts.strftime("%Y/%m/%d")
run_ts_str = run_ts.strftime("%Y%m%d_%H%M%S")

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

print(f"Tables to load into Raw: {[r.table_name for r in active_tables]}")

# COMMAND ----------

# MAGIC %md ## Extract from Source + Write Parquet Snapshots + Log Metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN workspace.raw_zone;
# MAGIC

# COMMAND ----------

child_rows = []

for row in active_tables:
    table_name = row.table_name
    file_name  = row.file_name
    status     = "FAILED"
    src_count  = 0
    tgt_count  = 0
    file_loc   = ""

    try:
        # Read directly from federated Azure SQL source — no temp view dependency
        fq_source = f"{source_catalog}.{source_schema}.{table_name}"
        df        = spark.read.table(fq_source)
        src_count = df.count()

        # Dynamic path — never overwrites previous runs
        file_loc  = f"{base_path}/{table_name.lower()}/{run_date}/{table_name.lower()}_{run_ts_str}.parquet"

        df.write.mode("overwrite").parquet(file_loc)

        tgt_count = spark.read.parquet(file_loc).count()

        if src_count != tgt_count:
            raise ValueError(f"Row count mismatch — source: {src_count}, target: {tgt_count}")

        status = "SUCCESS"
        print(f"  {table_name}: {src_count} rows → {file_loc}")

    except Exception as e:
        status = "FAILED"
        print(f"  {table_name}: FAILED — {e}")

    child_rows.append(Row(
        table_name       = table_name,
        execution_time   = run_ts,
        status           = status,
        source_row_count = src_count,
        target_row_count = tgt_count,
        file_location    = file_loc,
        created_date     = run_ts.date()
    ))

# COMMAND ----------

# MAGIC %md ## Write to Child Execution Metrics Table

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    IntegerType, DateType
)

schema = StructType([
    StructField("table_name",       StringType(),    True),
    StructField("execution_time",   TimestampType(), True),
    StructField("status",           StringType(),    True),
    StructField("source_row_count", IntegerType(),   True),
    StructField("target_row_count", IntegerType(),   True),
    StructField("file_location",    StringType(),    True),
    StructField("created_date",     DateType(),      True),
])

metrics_df = spark.createDataFrame(child_rows, schema)
metrics_df.write.format("delta").mode("append").saveAsTable(
    f"{catalog_name}.{schema_name}.pipeline_metadata_child"
)

print("\nChild metrics written.")
display(metrics_df)

# COMMAND ----------

# MAGIC %md ## Fail the notebook if any table failed

# COMMAND ----------

failures = [r for r in child_rows if r.status == "FAILED"]
if failures:
    failed_names = [r.table_name for r in failures]
    raise Exception(f"Raw load failed for tables: {failed_names}")

print("All tables loaded to Raw successfully.")
