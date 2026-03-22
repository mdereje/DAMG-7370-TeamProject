# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Extract From Source
# MAGIC Reads the parent metadata table and extracts each active table from the Chinook Azure SQL source
# MAGIC via the Connection Manager federation. Results are staged for Raw zone loading.

# COMMAND ----------
# MAGIC %md ## Parameters

# COMMAND ----------
dbutils.widgets.text("catalog_name", "workspace")
dbutils.widgets.text("schema_name", "raw_zone")
dbutils.widgets.text("source_catalog", "chinook_azure_catalog")
dbutils.widgets.text("source_schema", "dbo")

catalog_name  = dbutils.widgets.get("catalog_name")
schema_name   = dbutils.widgets.get("schema_name")
source_catalog = dbutils.widgets.get("source_catalog")
source_schema  = dbutils.widgets.get("source_schema")

print(f"catalog_name   : {catalog_name}")
print(f"schema_name    : {schema_name}")
print(f"source_catalog : {source_catalog}")
print(f"source_schema  : {source_schema}")

# COMMAND ----------
# MAGIC %md ## Read Parent Metadata Table

# COMMAND ----------
parent_df = spark.table(f"{catalog_name}.{schema_name}.pipeline_metadata_parent")

active_tables = (
    parent_df
    .filter("active_flag = 'Y'")
    .select("table_name", "file_name")
    .collect()
)

print(f"Active tables to extract: {[r.table_name for r in active_tables]}")

# COMMAND ----------
# MAGIC %md ## Extract Each Table and Store as Temp View

# COMMAND ----------
for row in active_tables:
    table_name = row.table_name
    fq_source  = f"{source_catalog}.{source_schema}.{table_name}"

    print(f"  Extracting: {fq_source}")
    try:
        df = spark.read.table(fq_source)
        # Store as a temp view so notebook 02 can pick it up within the same job cluster session
        df.createOrReplaceTempView(f"extract_{table_name.lower()}")
        print(f"    Rows extracted: {df.count()} — temp view: extract_{table_name.lower()}")
    except Exception as e:
        print(f"    FAILED to extract {table_name}: {e}")
        raise

print("\nExtraction complete.")
