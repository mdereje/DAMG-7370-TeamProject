# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 04 — Bronze to Silver
# MAGIC Runs DQX-style data profiling and quality validation on each Bronze table.
# MAGIC Validation is implemented with native PySpark to avoid DQX cluster compatibility
# MAGIC issues (pipelines.id CONFIG_NOT_AVAILABLE on standard clusters).
# MAGIC Failed records are written to a quarantine table.
# MAGIC Passing records are cleaned and written to Silver Delta tables.

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

catalog_name       = dbutils.widgets.get("catalog_name")
bronze_schema_name = dbutils.widgets.get("bronze_schema_name")
silver_schema_name = dbutils.widgets.get("silver_schema_name")
raw_schema_name    = dbutils.widgets.get("raw_schema_name")

print(f"catalog_name       : {catalog_name}")
print(f"raw_schema_name    : {raw_schema_name}")
print(f"bronze_schema_name : {bronze_schema_name}")
print(f"silver_schema_name : {silver_schema_name}")

# COMMAND ----------

# MAGIC %md ## Imports

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DateType, LongType, IntegerType
)
from datetime import datetime, timezone

run_ts = datetime.now(timezone.utc)

# COMMAND ----------

# MAGIC %md ## Quality Rule Definitions
# MAGIC
# MAGIC Rules are defined as dicts with:
# MAGIC - `col`: column name to check
# MAGIC - `check`: one of `not_null`, `not_null_and_not_empty`, `min_value`
# MAGIC - `criticality`: `error` (record fails) or `warn` (record logged but passes)
# MAGIC - Optional: `min_val` for min_value checks

# COMMAND ----------

# Each entry: {"col": ..., "check": ..., "criticality": "error"|"warn", "min_val": ...}
DQX_RULES = {
    "invoice": [
        {"col": "InvoiceId",   "check": "not_null",             "criticality": "error"},
        {"col": "CustomerId",  "check": "not_null",             "criticality": "error"},
        {"col": "InvoiceDate", "check": "not_null",             "criticality": "error"},
        {"col": "Total",       "check": "not_null",             "criticality": "warn"},
        {"col": "Total",       "check": "min_value", "min_val": 0, "criticality": "warn"},
    ],
    "invoiceline": [
        {"col": "InvoiceLineId", "check": "not_null",             "criticality": "error"},
        {"col": "InvoiceId",     "check": "not_null",             "criticality": "error"},
        {"col": "TrackId",       "check": "not_null",             "criticality": "error"},
        {"col": "Quantity",      "check": "not_null",             "criticality": "error"},
        {"col": "Quantity",      "check": "min_value", "min_val": 1, "criticality": "error"},
    ],
    "customer": [
        {"col": "CustomerId", "check": "not_null",             "criticality": "error"},
        {"col": "Email",      "check": "not_null_and_not_empty", "criticality": "warn"},
    ],
    "track": [
        {"col": "TrackId", "check": "not_null", "criticality": "error"},
        {"col": "Name",    "check": "not_null", "criticality": "error"},
    ],
    "album": [
        {"col": "AlbumId", "check": "not_null", "criticality": "error"},
        {"col": "Title",   "check": "not_null", "criticality": "error"},
    ],
    "artist":        [{"col": "ArtistId",     "check": "not_null", "criticality": "error"}],
    "genre":         [{"col": "GenreId",      "check": "not_null", "criticality": "error"}],
    "mediatype":     [{"col": "MediaTypeId",  "check": "not_null", "criticality": "error"}],
    "employee":      [{"col": "EmployeeId",   "check": "not_null", "criticality": "error"}],
    "playlist":      [{"col": "PlaylistId",   "check": "not_null", "criticality": "error"}],
    "playlisttrack": [
        {"col": "PlaylistId", "check": "not_null", "criticality": "error"},
        {"col": "TrackId",    "check": "not_null", "criticality": "error"},
    ],
}

# COMMAND ----------

# MAGIC %md ## Profiling Helper

# COMMAND ----------

def profile_dataframe(df, table_name):
    """Print DQX-style profiling stats for a Bronze DataFrame."""
    total = df.count()
    print(f"\n  --- Profile: {table_name} ({total} rows) ---")

    # Null counts per column
    null_counts = df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]).collect()[0].asDict()
    has_nulls = {c: v for c, v in null_counts.items() if v > 0}
    if has_nulls:
        print("  Nulls per column:")
        for col, cnt in has_nulls.items():
            print(f"    {col}: {cnt} nulls")
    else:
        print("  Nulls: none")

    # Duplicate rows
    dup_count = total - df.dropDuplicates().count()
    print(f"  Duplicate rows: {dup_count}")

    # Data type summary
    print(f"  Schema: {', '.join([f'{c}({t})' for c, t in df.dtypes])}")

    return total

# COMMAND ----------

# MAGIC %md ## Native DQX-style Validation Engine

# COMMAND ----------

def apply_quality_rules(df, rules):
    """
    Apply quality rules to a DataFrame.
    Returns (valid_df, quarantine_df) — same contract as DQEngine.apply_checks_and_split().

    Each failed record in quarantine_df gets an extra column:
      _dq_errors: JSON array of {col, check, criticality} for every failing rule
    """
    if not rules:
        return df, spark.createDataFrame([], df.schema)

    # Build a failure flag column per rule
    fail_exprs = []
    for i, rule in enumerate(rules):
        col   = rule["col"]
        check = rule["check"]
        crit  = rule["criticality"]

        if check == "not_null":
            fail_cond = F.col(col).isNull()
        elif check == "not_null_and_not_empty":
            fail_cond = F.col(col).isNull() | (F.trim(F.col(col)) == "")
        elif check == "min_value":
            fail_cond = F.col(col).isNull() | (F.col(col) < rule.get("min_val", 0))
        else:
            fail_cond = F.lit(False)

        # Only errors count toward quarantine; warns are logged but pass
        if crit == "error":
            fail_exprs.append(
                F.when(fail_cond, F.lit(f"{col}:{check}")).otherwise(F.lit(None))
            )

    if not fail_exprs:
        # All rules are warn-only — nothing quarantined
        return df, spark.createDataFrame([], df.schema)

    # Collect all error messages into an array column
    tagged_df = df.withColumn(
        "_dq_errors",
        F.to_json(F.array_compact(F.array(*fail_exprs)))
    )

    # Split: error rows have at least one non-null failure
    has_error = F.col("_dq_errors") != "[]"

    quarantine_df = tagged_df.filter(has_error)
    valid_df      = tagged_df.filter(~has_error).drop("_dq_errors")

    return valid_df, quarantine_df

# COMMAND ----------

# MAGIC %md ## Ensure Silver Infrastructure Tables Exist

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.{silver_schema_name}.dqx_execution_log (
        table_name      STRING,
        run_timestamp   TIMESTAMP,
        total_records   BIGINT,
        passed_records  BIGINT,
        failed_records  BIGINT,
        created_date    DATE
    ) USING DELTA
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.{silver_schema_name}.quarantine (
        table_name      STRING,
        run_timestamp   TIMESTAMP,
        _error_details  STRING,
        _row_data       STRING
    ) USING DELTA
""")

# COMMAND ----------

# MAGIC %md ## Main Loop — Bronze → Validate → Silver

# COMMAND ----------

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

    # --- Quality Validation ---
    rules     = DQX_RULES.get(table_name, [])
    valid_df, quarantine_df = apply_quality_rules(df, rules)

    passed_count = valid_df.count()
    failed_count = quarantine_df.count()
    print(f"  Passed: {passed_count} | Quarantined: {failed_count}")

    # Write quarantine records
    if failed_count > 0:
        data_cols = [c for c in quarantine_df.columns if not c.startswith("_")]
        quarantine_out = (
            quarantine_df
            .withColumn("table_name",     F.lit(table_name))
            .withColumn("run_timestamp",  F.lit(run_ts).cast(TimestampType()))
            .withColumn("_error_details", F.col("_dq_errors"))
            .withColumn("_row_data",      F.to_json(F.struct(*[F.col(c) for c in data_cols])))
            .select("table_name", "run_timestamp", "_error_details", "_row_data")
        )
        quarantine_out.write.format("delta").mode("append").saveAsTable(
            f"{catalog_name}.{silver_schema_name}.quarantine"
        )
        print(f"  {failed_count} records written to quarantine.")

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
        clean_df = clean_df.withColumn("BirthDate", F.to_timestamp(F.col("BirthDate")))
        clean_df = clean_df.withColumn("HireDate",  F.to_timestamp(F.col("HireDate")))

    # Write to Silver
    (
        clean_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(silver_table)
    )
    print(f"  Silver table {silver_table} written.")

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
