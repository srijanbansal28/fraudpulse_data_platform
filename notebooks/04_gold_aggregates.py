# Databricks notebook source

# MAGIC %md
# MAGIC # FraudPulse — Gold Layer Aggregates
# MAGIC
# MAGIC Build pre-computed aggregate tables from the Silver and Anomaly
# MAGIC Scores layers for downstream dashboards and business analytics.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

SILVER_TABLE = "workspace.fraudpulse.silver_transactions"
ANOMALY_TABLE = "workspace.fraudpulse.anomaly_scores"

GOLD_DAILY_SUMMARY = "workspace.fraudpulse.gold_daily_summary"
GOLD_MERCHANT_RISK = "workspace.fraudpulse.gold_merchant_risk_profile"
GOLD_CUSTOMER_RISK = "workspace.fraudpulse.gold_customer_risk_profile"
GOLD_HOURLY_PATTERN = "workspace.fraudpulse.gold_hourly_pattern"
GOLD_PAYMENT_METHOD = "workspace.fraudpulse.gold_payment_method_summary"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Source Tables

# COMMAND ----------

silver_df = spark.table(SILVER_TABLE)
anomaly_df = spark.table(ANOMALY_TABLE)

print(f"Silver records:  {silver_df.count()}")
print(f"Anomaly records: {anomaly_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 1 — Daily Transaction Summary

# COMMAND ----------

daily_summary = (
    silver_df
    .groupBy("event_date")
    .agg(
        F.count("*").alias("total_transactions"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.countDistinct("merchant_id").alias("unique_merchants"),
        F.round(F.sum("amount"), 2).alias("total_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(F.max("amount"), 2).alias("max_amount"),
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_count"),
        F.sum(F.when(F.col("status") == "PENDING", 1).otherwise(0)).alias("pending_count"),
        F.sum(F.when(F.col("is_cross_border") == True, 1).otherwise(0)).alias("cross_border_count"),
    )
    .withColumn(
        "success_rate",
        F.round(F.col("success_count") / F.col("total_transactions") * 100, 2),
    )
    .withColumn("computed_at", F.current_timestamp())
    .orderBy("event_date")
)

(
    daily_summary
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_DAILY_SUMMARY)
)

print(f"✅ {GOLD_DAILY_SUMMARY} written")
display(daily_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 2 — Merchant Risk Profile

# COMMAND ----------

merchant_risk = (
    anomaly_df
    .groupBy("merchant_id")
    .agg(
        F.count("*").alias("total_transactions"),
        F.round(F.sum("amount"), 2).alias("total_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(F.avg("risk_score"), 2).alias("avg_risk_score"),
        F.max("risk_score").alias("max_risk_score"),
        F.sum(F.when(F.col("risk_level") == "CRITICAL", 1).otherwise(0)).alias("critical_count"),
        F.sum(F.when(F.col("risk_level") == "HIGH", 1).otherwise(0)).alias("high_risk_count"),
        F.round(F.avg("merchant_failure_rate") * 100, 2).alias("failure_rate_pct"),
    )
    .withColumn(
        "merchant_risk_tier",
        F.when(F.col("avg_risk_score") >= 50, "CRITICAL")
        .when(F.col("avg_risk_score") >= 25, "HIGH")
        .when(F.col("avg_risk_score") >= 10, "MEDIUM")
        .otherwise("LOW"),
    )
    .withColumn("computed_at", F.current_timestamp())
    .orderBy(F.desc("avg_risk_score"))
)

(
    merchant_risk
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_MERCHANT_RISK)
)

print(f"✅ {GOLD_MERCHANT_RISK} written")
display(merchant_risk.limit(15))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 3 — Customer Risk Profile

# COMMAND ----------

customer_risk = (
    anomaly_df
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("total_transactions"),
        F.round(F.sum("amount"), 2).alias("total_spent"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(F.avg("risk_score"), 2).alias("avg_risk_score"),
        F.max("risk_score").alias("max_risk_score"),
        F.sum(F.when(F.col("risk_level").isin("CRITICAL", "HIGH"), 1).otherwise(0)).alias("high_risk_txn_count"),
        F.sum(F.when(F.col("flag_high_amount") == True, 1).otherwise(0)).alias("high_amount_flags"),
        F.sum(F.when(F.col("flag_velocity") == True, 1).otherwise(0)).alias("velocity_flags"),
        F.countDistinct("merchant_id").alias("unique_merchants"),
        F.countDistinct("country").alias("unique_countries"),
    )
    .withColumn(
        "customer_risk_tier",
        F.when(F.col("avg_risk_score") >= 50, "CRITICAL")
        .when(F.col("avg_risk_score") >= 25, "HIGH")
        .when(F.col("avg_risk_score") >= 10, "MEDIUM")
        .otherwise("LOW"),
    )
    .withColumn("computed_at", F.current_timestamp())
    .orderBy(F.desc("avg_risk_score"))
)

(
    customer_risk
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_CUSTOMER_RISK)
)

print(f"✅ {GOLD_CUSTOMER_RISK} written")
display(customer_risk.limit(15))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 4 — Hourly Transaction Pattern

# COMMAND ----------

hourly_pattern = (
    silver_df
    .groupBy("event_hour")
    .agg(
        F.count("*").alias("total_transactions"),
        F.round(F.sum("amount"), 2).alias("total_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_count"),
        F.countDistinct("customer_id").alias("unique_customers"),
    )
    .withColumn(
        "failure_rate_pct",
        F.round(F.col("failed_count") / F.col("total_transactions") * 100, 2),
    )
    .withColumn("computed_at", F.current_timestamp())
    .orderBy("event_hour")
)

(
    hourly_pattern
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_HOURLY_PATTERN)
)

print(f"✅ {GOLD_HOURLY_PATTERN} written")
display(hourly_pattern)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 5 — Payment Method Summary

# COMMAND ----------

payment_summary = (
    silver_df
    .groupBy("payment_method")
    .agg(
        F.count("*").alias("total_transactions"),
        F.round(F.sum("amount"), 2).alias("total_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_count"),
    )
    .withColumn(
        "success_rate_pct",
        F.round(F.col("success_count") / F.col("total_transactions") * 100, 2),
    )
    .withColumn("computed_at", F.current_timestamp())
    .orderBy(F.desc("total_transactions"))
)

(
    payment_summary
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_PAYMENT_METHOD)
)

print(f"✅ {GOLD_PAYMENT_METHOD} written")
display(payment_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Summary

# COMMAND ----------

print("=" * 60)
print("  FraudPulse Gold Layer — Pipeline Complete")
print("=" * 60)
print()

gold_tables = [
    GOLD_DAILY_SUMMARY,
    GOLD_MERCHANT_RISK,
    GOLD_CUSTOMER_RISK,
    GOLD_HOURLY_PATTERN,
    GOLD_PAYMENT_METHOD,
]

for table_name in gold_tables:
    count = spark.table(table_name).count()
    print(f"  {table_name}: {count} rows")

print()
print("All Gold tables refreshed successfully.")
