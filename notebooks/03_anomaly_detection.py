# Databricks notebook source

# MAGIC %md
# MAGIC # FraudPulse — Anomaly Detection
# MAGIC
# MAGIC Apply statistical and rule-based anomaly detection to flag suspicious
# MAGIC transactions in the Silver layer, producing an **Anomaly Scores** table.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

SILVER_TABLE = "workspace.fraudpulse.silver_transactions"
ANOMALY_TABLE = "workspace.fraudpulse.anomaly_scores"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver Data

# COMMAND ----------

silver_df = spark.table(SILVER_TABLE)
print(f"Silver records: {silver_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 1 — High-Amount Transactions
# MAGIC
# MAGIC Flag transactions where the amount is significantly higher than the
# MAGIC customer's historical average (z-score > 2).

# COMMAND ----------

customer_stats = silver_df.groupBy("customer_id").agg(
    F.avg("amount").alias("cust_avg_amount"),
    F.stddev("amount").alias("cust_stddev_amount"),
    F.count("*").alias("cust_txn_count"),
)

scored_df = silver_df.join(customer_stats, on="customer_id", how="left")

scored_df = scored_df.withColumn(
    "amount_zscore",
    F.when(
        (F.col("cust_stddev_amount").isNotNull()) & (F.col("cust_stddev_amount") > 0),
        F.round((F.col("amount") - F.col("cust_avg_amount")) / F.col("cust_stddev_amount"), 4),
    ).otherwise(F.lit(0.0)),
)

scored_df = scored_df.withColumn(
    "flag_high_amount",
    F.when(F.col("amount_zscore") > 2.0, F.lit(True)).otherwise(F.lit(False)),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 2 — Velocity Check
# MAGIC
# MAGIC Flag customers with more than 5 transactions within a 1-hour window.

# COMMAND ----------

velocity_window = (
    Window
    .partitionBy("customer_id")
    .orderBy(F.col("event_timestamp").cast("long"))
    .rangeBetween(-3600, 0)
)

scored_df = scored_df.withColumn(
    "txn_count_1h", F.count("*").over(velocity_window)
)

scored_df = scored_df.withColumn(
    "flag_velocity",
    F.when(F.col("txn_count_1h") > 5, F.lit(True)).otherwise(F.lit(False)),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 3 — Cross-Border Transactions

# COMMAND ----------

scored_df = scored_df.withColumn(
    "flag_cross_border",
    F.when(F.col("is_cross_border") == True, F.lit(True)).otherwise(F.lit(False)),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 4 — Off-Hours Transactions
# MAGIC
# MAGIC Transactions between midnight and 5 AM (local time assumption).

# COMMAND ----------

scored_df = scored_df.withColumn(
    "flag_off_hours",
    F.when(F.col("event_hour").between(0, 4), F.lit(True)).otherwise(F.lit(False)),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 5 — Failed Transaction Spike
# MAGIC
# MAGIC Flag merchants with a failure rate above 30%.

# COMMAND ----------

merchant_failure = silver_df.groupBy("merchant_id").agg(
    F.count("*").alias("merchant_total"),
    F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("merchant_failed"),
)

merchant_failure = merchant_failure.withColumn(
    "merchant_failure_rate",
    F.round(F.col("merchant_failed") / F.col("merchant_total"), 4),
)

scored_df = scored_df.join(
    merchant_failure.select("merchant_id", "merchant_failure_rate"),
    on="merchant_id",
    how="left",
)

scored_df = scored_df.withColumn(
    "flag_merchant_risk",
    F.when(F.col("merchant_failure_rate") > 0.30, F.lit(True)).otherwise(F.lit(False)),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Composite Risk Score
# MAGIC
# MAGIC Weighted combination of all flags into a 0–100 risk score.

# COMMAND ----------

scored_df = scored_df.withColumn(
    "risk_score",
    (
        F.col("flag_high_amount").cast("int") * 35
        + F.col("flag_velocity").cast("int") * 25
        + F.col("flag_cross_border").cast("int") * 15
        + F.col("flag_off_hours").cast("int") * 10
        + F.col("flag_merchant_risk").cast("int") * 15
    ),
)

scored_df = scored_df.withColumn(
    "risk_level",
    F.when(F.col("risk_score") >= 60, "CRITICAL")
    .when(F.col("risk_score") >= 35, "HIGH")
    .when(F.col("risk_score") >= 15, "MEDIUM")
    .otherwise("LOW"),
)

scored_df = scored_df.withColumn("scored_at", F.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Anomaly Scores Table

# COMMAND ----------

output_columns = [
    "event_id",
    "transaction_id",
    "customer_id",
    "merchant_id",
    "event_timestamp",
    "amount",
    "currency",
    "payment_method",
    "country",
    "status",
    "amount_zscore",
    "txn_count_1h",
    "merchant_failure_rate",
    "flag_high_amount",
    "flag_velocity",
    "flag_cross_border",
    "flag_off_hours",
    "flag_merchant_risk",
    "risk_score",
    "risk_level",
    "scored_at",
]

(
    scored_df
    .select(output_columns)
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(ANOMALY_TABLE)
)

anomaly_count = spark.table(ANOMALY_TABLE).count()
print(f"Anomaly scores table '{ANOMALY_TABLE}' written with {anomaly_count} records.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Risk Distribution Summary

# COMMAND ----------

risk_summary = (
    spark.table(ANOMALY_TABLE)
    .groupBy("risk_level")
    .agg(
        F.count("*").alias("count"),
        F.round(F.avg("risk_score"), 2).alias("avg_risk_score"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .orderBy(F.desc("avg_risk_score"))
)

display(risk_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top 20 Highest-Risk Transactions

# COMMAND ----------

top_risks = (
    spark.table(ANOMALY_TABLE)
    .filter(F.col("risk_level").isin("CRITICAL", "HIGH"))
    .orderBy(F.desc("risk_score"), F.desc("amount"))
    .limit(20)
)

display(top_risks)
