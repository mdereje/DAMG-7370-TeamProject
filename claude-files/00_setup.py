# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup
# MAGIC Run this notebook ONCE before executing the pipeline for the first time.
# MAGIC Creates all required schemas and metadata tables.

# COMMAND ----------
dbutils.widgets.text("catalog_name", "workspace")
catalog_name = dbutils.widgets.get("catalog_name")

# COMMAND ----------
# MAGIC %md ## Create Schemas

# COMMAND ----------
for schema in ["raw_zone", "bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema}")
    print(f"Schema ready: {catalog_name}.{schema}")

# COMMAND ----------
# MAGIC %md ## Create Raw Zone Volume (run manually or via SQL)
# MAGIC
# MAGIC If the volume does not yet exist, run the following in a SQL cell or Catalog UI:
# MAGIC ```sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.raw_zone.chinook;
# MAGIC ```

# COMMAND ----------
# MAGIC %md ## Create Parent Metadata Table

# COMMAND ----------
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.raw_zone.pipeline_metadata_parent (
        table_name    STRING        NOT NULL,
        file_name     STRING,
        active_flag   STRING        NOT NULL,
        created_date  DATE,
        modified_date DATE
    )
    USING DELTA
""")
print("pipeline_metadata_parent ready.")

# COMMAND ----------
# MAGIC %md ## Seed Parent Metadata — Chinook Tables

# COMMAND ----------
from datetime import date

today = date.today()

chinook_tables = [
    ("Album",         "album.csv"),
    ("Artist",        "artist.csv"),
    ("Customer",      "customer.csv"),
    ("Employee",      "employee.csv"),
    ("Genre",         "genre.csv"),
    ("Invoice",       "invoice.csv"),
    ("InvoiceLine",   "invoiceline.csv"),
    ("MediaType",     "mediatype.csv"),
    ("Playlist",      "playlist.csv"),
    ("PlaylistTrack", "playlisttrack.csv"),
    ("Track",         "track.csv"),
]

existing = spark.table(f"{catalog_name}.raw_zone.pipeline_metadata_parent")
existing_names = [r.table_name for r in existing.select("table_name").collect()]

rows_to_insert = [
    (t, f, "Y", today, today)
    for t, f in chinook_tables
    if t not in existing_names
]

if rows_to_insert:
    from pyspark.sql.types import StructType, StructField, StringType, DateType
    schema = StructType([
        StructField("table_name",    StringType(), True),
        StructField("file_name",     StringType(), True),
        StructField("active_flag",   StringType(), True),
        StructField("created_date",  DateType(),   True),
        StructField("modified_date", DateType(),   True),
    ])
    seed_df = spark.createDataFrame(rows_to_insert, schema)
    seed_df.write.format("delta").mode("append").saveAsTable(
        f"{catalog_name}.raw_zone.pipeline_metadata_parent"
    )
    print(f"Inserted {len(rows_to_insert)} rows into pipeline_metadata_parent.")
else:
    print("Parent metadata already seeded — nothing inserted.")

display(spark.table(f"{catalog_name}.raw_zone.pipeline_metadata_parent"))

# COMMAND ----------
# MAGIC %md ## Create Child Execution Metrics Table

# COMMAND ----------
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.raw_zone.pipeline_metadata_child (
        table_name          STRING,
        execution_time      TIMESTAMP,
        status              STRING,
        source_row_count    INT,
        target_row_count    INT,
        file_location       STRING,
        created_date        DATE
    )
    USING DELTA
""")
print("pipeline_metadata_child ready.")

# COMMAND ----------
print("\nSetup complete. You can now run the pipeline job.")
