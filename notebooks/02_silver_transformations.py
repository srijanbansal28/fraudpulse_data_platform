# Databricks notebook source

# MAGIC %md
# MAGIC # FraudPulse — Silver Layer Transformations
# MAGIC
# MAGIC Read from the Bronze table, deduplicate, parse timestamps, validate
# MAGIC data quality, and enrich with derived columns to create the
# MAGIC **Silver** (cleansed) Delta table.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

BRONZE_TABLE = "workspace.fraudpulse.bronze_transactions"
SILVER_TABLE = "workspace.fraudpulse.silver_transactions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Data

# COMMAND ----------

bronze_df = spark.table(BRONZE_TABLE)
print(f"Bronze records: {bronze_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Deduplicate on event_id
# MAGIC
# MAGIC Keep only the earliest-ingested copy of each event.

# COMMAND ----------

dedup_window = Window.partitionBy("event_id").orderBy("ingestion_timestamp")

deduped_df = (
    bronze_df
    .withColumn("row_num", F.row_number().over(dedup_window))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

duplicate_count = bronze_df.count() - deduped_df.count()
print(f"Duplicates removed: {duplicate_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Parse and Validate Timestamps

# COMMAND ----------

parsed_df = deduped_df.withColumn(
    "event_timestamp",
    F.to_timestamp(F.col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSX"),
)

# Fallback for slightly different ISO formats
parsed_df = parsed_df.withColumn(
    "event_timestamp",
    F.coalesce(
        F.col("event_timestamp"),
        F.to_timestamp(F.col("event_time")),
    ),
)

# Filter out records with unparseable timestamps
valid_df = parsed_df.filter(F.col("event_timestamp").isNotNull())
invalid_ts = parsed_df.count() - valid_df.count()
print(f"Records with invalid timestamps dropped: {invalid_ts}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Data Quality Filters

# COMMAND ----------

clean_df = (
    valid_df
    .filter(F.col("event_id").isNotNull() & (F.length(F.trim(F.col("event_id"))) > 0))
    .filter(F.col("transaction_id").isNotNull() & (F.length(F.trim(F.col("transaction_id"))) > 0))
    .filter(F.col("amount").isNotNull() & (F.col("amount") > 0))
    .filter(F.col("status").isin("SUCCESS", "FAILED", "PENDING"))
)

dropped_quality = valid_df.count() - clean_df.count()
print(f"Records dropped by quality filters: {dropped_quality}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Enrich with Derived Columns

# COMMAND ----------

enriched_df = (
    clean_df
    .withColumn("event_date", F.to_date("event_timestamp"))
    .withColumn("event_hour", F.hour("event_timestamp"))
    .withColumn("day_of_week", F.dayofweek("event_timestamp"))
    .withColumn(
        "amount_category",
        F.when(F.col("amount") > 25000, "HIGH")
        .when(F.col("amount") > 5000, "MEDIUM")
        .otherwise("LOW"),
    )
    .withColumn(
        "is_cross_border",
        F.when(F.col("country") != "IN", F.lit(True)).otherwise(F.lit(False)),
    )
    .withColumn("processing_timestamp", F.current_timestamp())
)

display(enriched_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Write to Silver Delta Table

# COMMAND ----------

(
    enriched_df
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("event_date")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

silver_count = spark.table(SILVER_TABLE).count()
print(f"Silver table '{SILVER_TABLE}' written with {silver_count} records.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Quality Summary

# COMMAND ----------

silver_df = spark.table(SILVER_TABLE)

summary = silver_df.groupBy("status", "amount_category").agg(
    F.count("*").alias("count"),
    F.round(F.avg("amount"), 2).alias("avg_amount"),
    F.round(F.min("amount"), 2).alias("min_amount"),
    F.round(F.max("amount"), 2).alias("max_amount"),
)

display(summary.orderBy("status", "amount_category"))
