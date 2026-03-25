# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 05 — Silver to Gold
# MAGIC Builds the dimensional model from Silver tables.
# MAGIC - dim_date, dim_track, dim_artist, dim_album, dim_genre, dim_mediatype, dim_employee, dim_playlist
# MAGIC - dim_customer — SCD Type 2
# MAGIC - fact_sales
# MAGIC - fact_sales_customer_agg (aggregation of fact_sales)

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name",       "workspace")
dbutils.widgets.text("silver_schema_name", "silver")
dbutils.widgets.text("gold_schema_name",   "gold")

catalog_name       = dbutils.widgets.get("catalog_name")
silver_schema_name = dbutils.widgets.get("silver_schema_name")
gold_schema_name   = dbutils.widgets.get("gold_schema_name")

print(f"catalog_name       : {catalog_name}")
print(f"silver_schema_name : {silver_schema_name}")
print(f"gold_schema_name   : {gold_schema_name}")

# COMMAND ----------

# MAGIC %md ## Imports

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timezone, date

run_ts      = datetime.now(timezone.utc)
HIGH_DATE   = "9999-12-31"   # SCD2 open-ended sentinel

def silver(name):
    return f"{catalog_name}.{silver_schema_name}.{name}"

def gold(name):
    return f"{catalog_name}.{gold_schema_name}.{name}"

# COMMAND ----------

# MAGIC %md ## Helper — ensure Gold schema exists

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{gold_schema_name}")

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## dim_artist

# COMMAND ----------

artist_df = spark.table(silver("artist"))

dim_artist = (
    artist_df
    .select(
        F.col("ArtistId").alias("artist_id"),
        F.col("Name").alias("artist_name")
    )
    .withColumn("artist_key", F.monotonically_increasing_id())
)

dim_artist.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold("dim_artist"))
print(f"dim_artist: {dim_artist.count()} rows")

# COMMAND ----------

# MAGIC %md ## dim_genre

# COMMAND ----------

genre_df = spark.table(silver("genre"))

dim_genre = (
    genre_df
    .select(
        F.col("GenreId").alias("genre_id"),
        F.col("Name").alias("genre_name")
    )
    .withColumn("genre_key", F.monotonically_increasing_id())
)

dim_genre.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold("dim_genre"))
print(f"dim_genre: {dim_genre.count()} rows")

# COMMAND ----------

# MAGIC %md ## dim_mediatype

# COMMAND ----------

mt_df = spark.table(silver("mediatype"))

dim_mediatype = (
    mt_df
    .select(
        F.col("MediaTypeId").alias("media_type_id"),
        F.col("Name").alias("media_type_name")
    )
    .withColumn("media_type_key", F.monotonically_increasing_id())
)

dim_mediatype.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold("dim_mediatype"))
print(f"dim_mediatype: {dim_mediatype.count()} rows")

# COMMAND ----------

# MAGIC %md ## dim_album

# COMMAND ----------

album_df  = spark.table(silver("album"))

dim_album = (
    album_df
    .select(
        F.col("AlbumId").alias("album_id"),
        F.col("Title").alias("album_title"),
        F.col("ArtistId").alias("artist_id")
    )
    .withColumn("album_key", F.monotonically_increasing_id())
)

dim_album.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold("dim_album"))
print(f"dim_album: {dim_album.count()} rows")

# COMMAND ----------

# MAGIC %md ## dim_track

# COMMAND ----------

track_df = spark.table(silver("track"))

dim_track = (
    track_df
    .select(
        F.col("TrackId").alias("track_id"),
        F.col("Name").alias("track_name"),
        F.col("AlbumId").alias("album_id"),
        F.col("MediaTypeId").alias("media_type_id"),
        F.col("GenreId").alias("genre_id"),
        F.col("Composer").alias("composer"),
        F.col("Milliseconds").alias("duration_ms"),
        F.col("Bytes").alias("file_size_bytes"),
        F.col("UnitPrice").alias("unit_price")
    )
    .withColumn("track_key", F.monotonically_increasing_id())
)

dim_track.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold("dim_track"))
print(f"dim_track: {dim_track.count()} rows")

# COMMAND ----------

# MAGIC %md ## dim_employee

# COMMAND ----------

emp_df = spark.table(silver("employee"))

dim_employee = (
    emp_df
    .select(
        F.col("EmployeeId").alias("employee_id"),
        F.concat_ws(" ", F.col("FirstName"), F.col("LastName")).alias("employee_name"),
        F.col("Title").alias("title"),
        F.col("ReportsTo").alias("reports_to_employee_id"),
        F.col("BirthDate").alias("birth_date"),
        F.col("HireDate").alias("hire_date"),
        F.col("City").alias("city"),
        F.col("Country").alias("country"),
        F.col("Email").alias("email")
    )
    .withColumn("employee_key", F.monotonically_increasing_id())
)

dim_employee.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold("dim_employee"))
print(f"dim_employee: {dim_employee.count()} rows")

# COMMAND ----------

# MAGIC %md ## dim_playlist

# COMMAND ----------

pl_df = spark.table(silver("playlist"))

dim_playlist = (
    pl_df
    .select(
        F.col("PlaylistId").alias("playlist_id"),
        F.col("Name").alias("playlist_name")
    )
    .withColumn("playlist_key", F.monotonically_increasing_id())
)

dim_playlist.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold("dim_playlist"))
print(f"dim_playlist: {dim_playlist.count()} rows")

# COMMAND ----------

# MAGIC %md ## dim_date

# COMMAND ----------

invoice_df = spark.table(silver("invoice"))

date_df = (
    invoice_df
    .select(F.to_date(F.col("InvoiceDate")).alias("full_date"))
    .dropDuplicates()
    .withColumn("date_key",      F.date_format(F.col("full_date"), "yyyyMMdd").cast("int"))
    .withColumn("year",          F.year(F.col("full_date")))
    .withColumn("quarter",       F.quarter(F.col("full_date")))
    .withColumn("month",         F.month(F.col("full_date")))
    .withColumn("month_name",    F.date_format(F.col("full_date"), "MMMM"))
    .withColumn("day_of_month",  F.dayofmonth(F.col("full_date")))
    .withColumn("day_of_week",   F.dayofweek(F.col("full_date")))
    .withColumn("day_name",      F.date_format(F.col("full_date"), "EEEE"))
    .withColumn("is_weekend",    (F.dayofweek(F.col("full_date")).isin([1, 7])).cast("boolean"))
)

date_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold("dim_date"))
print(f"dim_date: {date_df.count()} rows")

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## dim_customer — SCD Type 2
# MAGIC
# MAGIC Tracks historical changes to customer attributes.
# MAGIC On first load: all rows inserted as current (is_current = true).
# MAGIC On subsequent loads: changed rows are expired and new versions inserted.

# COMMAND ----------

customer_silver = spark.table(silver("customer"))

# Columns that, if changed, trigger a new SCD2 version
SCD2_TRACKED_COLS = [
    "FirstName", "LastName", "Company", "Address",
    "City", "State", "Country", "PostalCode",
    "Phone", "Fax", "Email", "SupportRepId"
]

incoming = (
    customer_silver
    .select(
        F.col("CustomerId").alias("customer_id"),
        F.col("FirstName").alias("first_name"),
        F.col("LastName").alias("last_name"),
        F.col("Company").alias("company"),
        F.col("Address").alias("address"),
        F.col("City").alias("city"),
        F.col("State").alias("state"),
        F.col("Country").alias("country"),
        F.col("PostalCode").alias("postal_code"),
        F.col("Phone").alias("phone"),
        F.col("Fax").alias("fax"),
        F.col("Email").alias("email"),
        F.col("SupportRepId").alias("support_rep_id")
    )
)

gold_customer_table = gold("dim_customer")

# ---- Check table existence via Unity Catalog SQL — avoids Hive Metastore entirely ----
def table_exists_uc(full_table_name):
    """
    Queries information_schema.tables in Unity Catalog.
    Safe on Spark Connect clusters with Hive Metastore disabled.
    spark.catalog.tableExists(name, dbName) incorrectly routes to Hive; avoid it.
    """
    parts = full_table_name.split(".")
    catalog_n, schema_n, table_n = parts[0], parts[1], parts[2]
    result = spark.sql(f"""
        SELECT COUNT(*) AS cnt
        FROM {catalog_n}.information_schema.tables
        WHERE table_schema = '{schema_n}'
          AND table_name   = '{table_n}'
    """).collect()[0]["cnt"]
    return result > 0

table_exists = table_exists_uc(gold_customer_table)
if table_exists:
    existing = spark.table(gold_customer_table)

if not table_exists:
    print("  dim_customer: first load — inserting all rows as current.")
    first_load = (
        incoming
        .withColumn("customer_key",        F.monotonically_increasing_id())
        .withColumn("effective_start_date", F.lit(run_ts.date()).cast("date"))
        .withColumn("effective_end_date",   F.lit(None).cast("date"))
        .withColumn("is_current",           F.lit(True))
    )
    first_load.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_customer_table)
    print(f"  dim_customer: {first_load.count()} rows inserted.")

else:
    # ---- Subsequent loads — apply SCD Type 2 merge ----
    print("  dim_customer: applying SCD Type 2 merge.")

    dim_current = existing.filter("is_current = true")

    # Detect changed records
    tracked_incoming_cols = ["customer_id", "first_name", "last_name", "company", "address",
                              "city", "state", "country", "postal_code", "phone", "fax", "email",
                              "support_rep_id"]

    changed = (
        incoming.alias("new")
        .join(dim_current.alias("old"), "customer_id", "inner")
    )
    # Build change condition
    change_condition = F.lit(False)
    for col in tracked_incoming_cols[1:]:   # skip customer_id
        change_condition = change_condition | (F.col(f"new.{col}") != F.col(f"old.{col}"))

    changed_ids = (
        changed
        .filter(change_condition)
        .select(F.col("new.customer_id").alias("customer_id"))
        .distinct()
    )

    changed_id_list = [r.customer_id for r in changed_ids.collect()]
    print(f"  Changed customer_ids: {changed_id_list}")

    delta_table = DeltaTable.forName(spark, gold_customer_table)

    if changed_id_list:
        # Step 1 — expire old current records for changed customers
        delta_table.update(
            condition = F.col("customer_id").isin(changed_id_list) & F.col("is_current"),
            set = {
                "effective_end_date": F.lit(run_ts.date()).cast("date"),
                "is_current":         F.lit(False)
            }
        )

        # Step 2 — insert new current versions for changed customers
        new_versions = (
            incoming
            .filter(F.col("customer_id").isin(changed_id_list))
            .withColumn("customer_key",        F.monotonically_increasing_id())
            .withColumn("effective_start_date", F.lit(run_ts.date()).cast("date"))
            .withColumn("effective_end_date",   F.lit(None).cast("date"))
            .withColumn("is_current",           F.lit(True))
        )
        new_versions.write.format("delta").mode("append").saveAsTable(gold_customer_table)
        print(f"  {len(changed_id_list)} customer(s) versioned with SCD Type 2.")

    # Step 3 — insert brand-new customers (not in dim at all)
    existing_ids = [r.customer_id for r in existing.select("customer_id").distinct().collect()]
    truly_new = (
        incoming
        .filter(~F.col("customer_id").isin(existing_ids))
        .withColumn("customer_key",        F.monotonically_increasing_id())
        .withColumn("effective_start_date", F.lit(run_ts.date()).cast("date"))
        .withColumn("effective_end_date",   F.lit(None).cast("date"))
        .withColumn("is_current",           F.lit(True))
    )
    new_count = truly_new.count()
    if new_count > 0:
        truly_new.write.format("delta").mode("append").saveAsTable(gold_customer_table)
        print(f"  {new_count} new customer(s) inserted.")

    print(f"  dim_customer SCD2 merge complete. Total rows: {spark.table(gold_customer_table).count()}")

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## fact_sales

# COMMAND ----------

invoice_df     = spark.table(silver("invoice"))
invoiceline_df = spark.table(silver("invoiceline"))
dim_cust_df    = spark.table(gold("dim_customer")).filter("is_current = true")
dim_track_df   = spark.table(gold("dim_track"))
dim_date_df    = spark.table(gold("dim_date"))
dim_emp_df     = spark.table(gold("dim_employee"))

fact_sales = (
    invoiceline_df.alias("il")
    .join(invoice_df.alias("inv"),   F.col("il.InvoiceId")  == F.col("inv.InvoiceId"),    "inner")
    .join(dim_cust_df.alias("dc"),   F.col("inv.CustomerId") == F.col("dc.customer_id"),   "left")
    .join(dim_track_df.alias("dt"),  F.col("il.TrackId")    == F.col("dt.track_id"),       "left")
    .join(dim_date_df.alias("dd"),
          F.to_date(F.col("inv.InvoiceDate")) == F.col("dd.full_date"), "left")
    .join(dim_emp_df.alias("de"),
          F.col("dc.support_rep_id") == F.col("de.employee_id"), "left")
    .select(
        F.col("il.InvoiceLineId").alias("invoice_line_id"),
        F.col("inv.InvoiceId").alias("invoice_id"),
        F.col("dc.customer_key"),
        F.col("dt.track_key"),
        F.col("dd.date_key"),
        F.col("de.employee_key").alias("support_rep_key"),
        F.col("il.Quantity").alias("quantity"),
        F.col("il.UnitPrice").alias("unit_price"),
        (F.col("il.Quantity") * F.col("il.UnitPrice")).alias("line_total"),
        F.col("inv.BillingCountry").alias("billing_country"),
        F.col("inv.BillingCity").alias("billing_city")
    )
)

fact_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold("fact_sales"))
print(f"fact_sales: {fact_sales.count()} rows")

# COMMAND ----------

# MAGIC %md ## fact_sales_customer_agg
# MAGIC Aggregation of fact_sales at the customer level.
# MAGIC Must be built AFTER fact_sales is fully loaded.

# COMMAND ----------

fs = spark.table(gold("fact_sales"))
dc = spark.table(gold("dim_customer")).filter("is_current = true")

fact_sales_customer_agg = (
    fs
    .groupBy("customer_key")
    .agg(
        F.countDistinct("invoice_id").alias("total_invoices"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("line_total").alias("total_revenue"),
        F.avg("line_total").alias("avg_line_total"),
        F.max("date_key").alias("last_purchase_date_key")
    )
    .join(
        dc.select("customer_key", "customer_id", "first_name", "last_name", "country", "email"),
        "customer_key",
        "left"
    )
)

fact_sales_customer_agg.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold("fact_sales_customer_agg"))
print(f"fact_sales_customer_agg: {fact_sales_customer_agg.count()} rows")

# COMMAND ----------

print("\nSilver → Gold complete.")
print(f"  dim_artist, dim_genre, dim_mediatype, dim_album, dim_track,")
print(f"  dim_employee, dim_playlist, dim_date, dim_customer (SCD2),")
print(f"  fact_sales, fact_sales_customer_agg")
