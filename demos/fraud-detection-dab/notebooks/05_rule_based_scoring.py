# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Rule-Based Fraud Scoring

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "fraud_demo_dev")
dbutils.widgets.text("run_id", "manual_run")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
run_id = dbutils.widgets.get("run_id")

gold_table = f"{catalog}.{schema}.gold_transaction_features"
scored_table = f"{catalog}.{schema}.scored_transactions"
alerts_table = f"{catalog}.{schema}.fraud_alerts"
metrics_table = f"{catalog}.{schema}.fraud_pipeline_metrics"

features_df = spark.table(gold_table)

scored_df = (
    features_df
    .withColumn(
        "fraud_score",
        F.lit(0)
        + F.when(F.col("is_high_amount") == 1, 30).otherwise(0)
        + F.when(F.col("is_cross_border") == 1, 25).otherwise(0)
        + F.when(F.col("is_velocity_risk") == 1, 25).otherwise(0)
        + F.when(F.col("is_amount_spike") == 1, 15).otherwise(0)
        + F.when(F.col("is_late_night") == 1, 5).otherwise(0)
    )
    .withColumn("risk_band", F.when(F.col("fraud_score") >= 70, F.lit("HIGH")).when(F.col("fraud_score") >= 40, F.lit("MEDIUM")).otherwise(F.lit("LOW")))
    .withColumn("is_fraud_alert", (F.col("fraud_score") >= 70).cast("int"))
    .withColumn("scoring_run_id", F.lit(run_id))
    .withColumn("scored_at", F.current_timestamp())
)

alerts_df = (
    scored_df
    .where(F.col("is_fraud_alert") == 1)
    .select("transaction_id", "customer_id", "card_id", "event_ts", "amount", "merchant_category", "payment_channel", "transaction_country", "customer_home_country", "fraud_score", "risk_band", "customer_txn_count_10min", "card_txn_count_10min", "amount_to_customer_avg_ratio", "scoring_run_id", "scored_at")
)

scored_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(scored_table)
alerts_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(alerts_table)

summary_df = (
    scored_df
    .agg(
        F.count("*").alias("total_transactions"),
        F.sum("is_fraud_alert").alias("fraud_alerts"),
        F.round(F.avg("fraud_score"), 2).alias("avg_fraud_score"),
        F.round(F.max("fraud_score"), 2).alias("max_fraud_score"),
        F.round((F.sum("is_fraud_alert") / F.count("*")) * 100, 2).alias("fraud_alert_rate_pct")
    )
    .withColumn("scoring_run_id", F.lit(run_id))
    .withColumn("metric_created_at", F.current_timestamp())
)

summary_df.write.format("delta").mode("append").saveAsTable(metrics_table)

print(f"Scored table: {scored_table}")
print(f"Alerts table: {alerts_table}")
print(f"Metrics table: {metrics_table}")

display(summary_df)
display(spark.table(alerts_table).orderBy(F.desc("fraud_score"), F.desc("amount")).limit(20))
