# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Bronze to Silver
# MAGIC Runs DQX data profiling and quality validation on each Bronze table.
# MAGIC Failed records are written to a quarantine table.
# MAGIC Passing records are cleaned and written to Silver Delta tables.

# COMMAND ----------
# MAGIC %md ## Parameters

# COMMAND ----------
dbutils.widgets.text("catalog_name",        "workspace")
dbutils.widgets.text("bronze_schema_name",  "bronze")
dbutils.widgets.text("silver_schema_name",  "silver")
dbutils.widgets.text("raw_schema_name",     "raw_zone")

catalog_name       = dbutils.widgets.get("catalog_name")
bronze_schema_name = dbutils.widgets.get("bronze_schema_name")
silver_schema_name = dbutils.widgets.get("silver_schema_name")
raw_schema_name    = dbutils.widgets.get("raw_schema_name")

print(f"catalog_name       : {catalog_name}")
print(f"bronze_schema_name : {bronze_schema_name}")
print(f"silver_schema_name : {silver_schema_name}")

# COMMAND ----------
# MAGIC %md ## Install & Import DQX

# COMMAND ----------
# MAGIC %pip install databricks-labs-dqx --quiet

# COMMAND ----------
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.col_functions import (
    is_not_null, is_not_null_and_not_empty,
    value_is_in_list, is_valid_date,
    is_unique_value
)
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType, DateType, IntegerType
from datetime import datetime, timezone

run_ts = datetime.now(timezone.utc)

# COMMAND ----------
# MAGIC %md ## DQX Rule Definitions per Table

# COMMAND ----------
# Define quality rules for each table.
# Keys must match the Bronze table names exactly (lowercase).
DQX_RULES = {
    "invoice": [
        {"criticality": "error",   "check": {"function": "is_not_null",    "arguments": {"col_names": ["InvoiceId"]}}},
        {"criticality": "error",   "check": {"function": "is_not_null",    "arguments": {"col_names": ["CustomerId"]}}},
        {"criticality": "error",   "check": {"function": "is_not_null",    "arguments": {"col_names": ["InvoiceDate"]}}},
        {"criticality": "warn",    "check": {"function": "is_not_null",    "arguments": {"col_names": ["Total"]}}},
    ],
    "invoiceline": [
        {"criticality": "error",   "check": {"function": "is_not_null",    "arguments": {"col_names": ["InvoiceLineId"]}}},
        {"criticality": "error",   "check": {"function": "is_not_null",    "arguments": {"col_names": ["InvoiceId"]}}},
        {"criticality": "error",   "check": {"function": "is_not_null",    "arguments": {"col_names": ["TrackId"]}}},
        {"criticality": "error",   "check": {"function": "is_not_null",    "arguments": {"col_names": ["Quantity"]}}},
    ],
    "customer": [
        {"criticality": "error",   "check": {"function": "is_not_null",    "arguments": {"col_names": ["CustomerId"]}}},
        {"criticality": "warn",    "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_names": ["Email"]}}},
    ],
    "track": [
        {"criticality": "error",   "check": {"function": "is_not_null",    "arguments": {"col_names": ["TrackId"]}}},
        {"criticality": "error",   "check": {"function": "is_not_null",    "arguments": {"col_names": ["Name"]}}},
    ],
    "album": [
        {"criticality": "error",   "check": {"function": "is_not_null",    "arguments": {"col_names": ["AlbumId"]}}},
        {"criticality": "error",   "check": {"function": "is_not_null",    "arguments": {"col_names": ["Title"]}}},
    ],
    "artist": [
        {"criticality": "error",   "check": {"function": "is_not_null",    "arguments": {"col_names": ["ArtistId"]}}},
    ],
    "genre": [
        {"criticality": "error",   "check": {"function": "is_not_null",    "arguments": {"col_names": ["GenreId"]}}},
    ],
    "mediatype": [
        {"criticality": "error",   "check": {"function": "is_not_null",    "arguments": {"col_names": ["MediaTypeId"]}}},
    ],
    "employee": [
        {"criticality": "error",   "check": {"function": "is_not_null",    "arguments": {"col_names": ["EmployeeId"]}}},
    ],
    "playlist": [
        {"criticality": "error",   "check": {"function": "is_not_null",    "arguments": {"col_names": ["PlaylistId"]}}},
    ],
    "playlisttrack": [
        {"criticality": "error",   "check": {"function": "is_not_null",    "arguments": {"col_names": ["PlaylistId", "TrackId"]}}},
    ],
}

# COMMAND ----------
# MAGIC %md ## Profiling Helper

# COMMAND ----------
def profile_dataframe(df, table_name):
    """Print basic profiling stats for a Bronze DataFrame."""
    total = df.count()
    print(f"\n  --- Profile: {table_name} ({total} rows) ---")

    # Null counts
    null_counts = df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]).collect()[0].asDict()
    print("  Nulls per column:")
    for col, cnt in null_counts.items():
        if cnt > 0:
            print(f"    {col}: {cnt} nulls")

    # Duplicate count (all columns)
    dup_count = total - df.dropDuplicates().count()
    print(f"  Duplicate rows: {dup_count}")

    return total

# COMMAND ----------
# MAGIC %md ## DQX Execution Log Table (ensure exists)

# COMMAND ----------
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.{silver_schema_name}.dqx_execution_log (
        table_name          STRING,
        run_timestamp       TIMESTAMP,
        total_records       BIGINT,
        passed_records      BIGINT,
        failed_records      BIGINT,
        created_date        DATE
    )
    USING DELTA
""")

# COMMAND ----------
# MAGIC %md ## Quarantine Table (ensure exists)

# COMMAND ----------
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.{silver_schema_name}.quarantine (
        table_name          STRING,
        run_timestamp       TIMESTAMP,
        _error_details      STRING,
        _row_data           STRING
    )
    USING DELTA
""")

# COMMAND ----------
# MAGIC %md ## Main Loop — Bronze → DQX → Silver

# COMMAND ----------
# Get active table list from parent metadata
parent_df = spark.table(f"{catalog_name}.{raw_schema_name}.pipeline_metadata_parent")
active_tables = [
    r.table_name.lower()
    for r in parent_df.filter("active_flag = 'Y'").select("table_name").collect()
]

print(f"Tables to process: {active_tables}")

dqx_log_rows = []

for table_name in active_tables:
    print(f"\nProcessing: {table_name}")
    bronze_table = f"{catalog_name}.{bronze_schema_name}.{table_name}"
    silver_table = f"{catalog_name}.{silver_schema_name}.{table_name}"

    try:
        df = spark.table(bronze_table)
    except Exception as e:
        print(f"  Could not read Bronze table {bronze_table}: {e} — skipping.")
        continue

    total_records = profile_dataframe(df, table_name)

    # --- DQX Validation ---
    rules = DQX_RULES.get(table_name, [])

    if rules:
        engine = DQEngine(spark)
        valid_df, quarantine_df = engine.apply_checks_and_split(df, rules)
    else:
        print(f"  No DQX rules defined — all records pass for {table_name}.")
        valid_df      = df
        quarantine_df = spark.createDataFrame([], df.schema)

    passed_count = valid_df.count()
    failed_count = quarantine_df.count()

    print(f"  Passed: {passed_count} | Failed/Quarantined: {failed_count}")

    # Write quarantine records
    if failed_count > 0:
        quarantine_out = (
            quarantine_df
            .withColumn("table_name",     F.lit(table_name))
            .withColumn("run_timestamp",  F.lit(run_ts).cast(TimestampType()))
            .select(
                F.col("table_name"),
                F.col("run_timestamp"),
                F.col("_error").cast(StringType()).alias("_error_details"),
                F.to_json(F.struct(*[c for c in quarantine_df.columns if not c.startswith("_")])).alias("_row_data")
            )
        )
        quarantine_out.write.format("delta").mode("append").saveAsTable(
            f"{catalog_name}.{silver_schema_name}.quarantine"
        )

    # --- Silver Cleaning ---
    clean_df = valid_df

    # Trim all string columns
    for col_name, dtype in clean_df.dtypes:
        if dtype == "string":
            clean_df = clean_df.withColumn(col_name, F.trim(F.col(col_name)))

    # Table-specific cleaning
    if table_name == "customer":
        clean_df = clean_df.withColumn("Email", F.lower(F.col("Email")))
        clean_df = clean_df.fillna({"Company": "Individual", "Fax": "N/A"})

    if table_name == "invoice":
        clean_df = clean_df.withColumn("InvoiceDate", F.to_timestamp(F.col("InvoiceDate")))

    if table_name == "employee":
        clean_df = clean_df.withColumn("BirthDate",   F.to_timestamp(F.col("BirthDate")))
        clean_df = clean_df.withColumn("HireDate",    F.to_timestamp(F.col("HireDate")))

    # Write to Silver
    (
        clean_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(silver_table)
    )
    print(f"  Silver table {silver_table} written.")

    # Accumulate DQX log row
    dqx_log_rows.append((
        table_name,
        run_ts,
        total_records,
        passed_count,
        failed_count,
        run_ts.date()
    ))

# COMMAND ----------
# MAGIC %md ## Write DQX Execution Log

# COMMAND ----------
from pyspark.sql.types import StructType, StructField, LongType

log_schema = StructType([
    StructField("table_name",     StringType(),    True),
    StructField("run_timestamp",  TimestampType(), True),
    StructField("total_records",  LongType(),      True),
    StructField("passed_records", LongType(),      True),
    StructField("failed_records", LongType(),      True),
    StructField("created_date",   DateType(),      True),
])

log_df = spark.createDataFrame(dqx_log_rows, log_schema)
log_df.write.format("delta").mode("append").saveAsTable(
    f"{catalog_name}.{silver_schema_name}.dqx_execution_log"
)

print("\nDQX execution log written.")
display(log_df)

print("\nBronze → Silver complete.")
