# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Raw to Bronze
# MAGIC Reads the latest file_location for each active table from the child metadata table,
# MAGIC loads the Parquet snapshot, and overwrites the corresponding Bronze Delta table.
# MAGIC No transformations — Bronze is an exact Delta copy of Raw.

# COMMAND ----------
# MAGIC %md ## Parameters

# COMMAND ----------
dbutils.widgets.text("catalog_name",        "workspace")
dbutils.widgets.text("raw_schema_name",     "raw_zone")
dbutils.widgets.text("bronze_schema_name",  "bronze")

catalog_name       = dbutils.widgets.get("catalog_name")
raw_schema_name    = dbutils.widgets.get("raw_schema_name")
bronze_schema_name = dbutils.widgets.get("bronze_schema_name")

print(f"catalog_name       : {catalog_name}")
print(f"raw_schema_name    : {raw_schema_name}")
print(f"bronze_schema_name : {bronze_schema_name}")

# COMMAND ----------
# MAGIC %md ## Get Latest File Location per Table from Child Metadata

# COMMAND ----------
from pyspark.sql import functions as F

child_df = spark.table(f"{catalog_name}.{raw_schema_name}.pipeline_metadata_child")

# Pick the most recent SUCCESS run per table
latest_df = (
    child_df
    .filter("status = 'SUCCESS'")
    .groupBy("table_name")
    .agg(F.max_by("file_location", "execution_time").alias("file_location"))
)

latest_rows = latest_df.collect()
print(f"Tables to promote to Bronze: {[r.table_name for r in latest_rows]}")

# COMMAND ----------
# MAGIC %md ## Load Raw Parquet → Write Bronze Delta (overwrite)

# COMMAND ----------
for row in latest_rows:
    table_name = row.table_name
    file_loc   = row.file_location

    print(f"  Processing: {table_name}")
    print(f"    Source: {file_loc}")

    df = spark.read.parquet(file_loc)

    bronze_table = f"{catalog_name}.{bronze_schema_name}.{table_name.lower()}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(bronze_table)
    )

    count = spark.table(bronze_table).count()
    print(f"    Bronze table {bronze_table}: {count} rows written.")

print("\nRaw → Bronze complete.")
