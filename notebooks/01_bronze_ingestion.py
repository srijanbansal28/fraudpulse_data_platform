# Databricks notebook source

# MAGIC %md
# MAGIC # FraudPulse — Bronze Layer Ingestion
# MAGIC
# MAGIC Ingest raw JSONL transaction events into the **Bronze** Delta table.
# MAGIC This layer stores data in its original form with added metadata
# MAGIC columns for lineage tracking.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Unity Catalog paths
RAW_DATA_PATH = "/Volumes/workspace/fraudpulse/raw_data/"
BRONZE_TABLE = "workspace.fraudpulse.bronze_transactions"

spark.sql("USE CATALOG workspace")
spark.sql("CREATE SCHEMA IF NOT EXISTS fraudpulse")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Schema

# COMMAND ----------

transaction_schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("country", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("status", StringType(), True),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Raw Data

# COMMAND ----------

raw_df = (
    spark.read
    .schema(transaction_schema)
    .json(RAW_DATA_PATH)
)

print(f"Raw records read: {raw_df.count()}")
raw_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Ingestion Metadata

# COMMAND ----------

bronze_df = (
    raw_df
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_file", F.input_file_name())
    .withColumn("ingestion_date", F.current_date())
)

display(bronze_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Delta Table

# COMMAND ----------

(
    bronze_df
    .write
    .format("delta")
    .mode("append")
    .partitionBy("ingestion_date")
    .option("mergeSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)

print(f"Bronze table '{BRONZE_TABLE}' updated.")
print(f"Total Bronze records: {spark.table(BRONZE_TABLE).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Check

# COMMAND ----------

bronze_table = spark.table(BRONZE_TABLE)

quality_report = bronze_table.select(
    F.count("*").alias("total_records"),
    F.countDistinct("event_id").alias("unique_events"),
    F.sum(F.when(F.col("event_id").isNull(), 1).otherwise(0)).alias("null_event_ids"),
    F.sum(F.when(F.col("amount").isNull(), 1).otherwise(0)).alias("null_amounts"),
    F.sum(F.when(F.col("amount") <= 0, 1).otherwise(0)).alias("non_positive_amounts"),
    F.min("ingestion_timestamp").alias("earliest_ingestion"),
    F.max("ingestion_timestamp").alias("latest_ingestion"),
)

display(quality_report)
