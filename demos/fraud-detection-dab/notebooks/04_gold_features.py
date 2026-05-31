# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Gold Feature Engineering

from pyspark.sql import Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "fraud_demo_dev")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

silver_table = f"{catalog}.{schema}.silver_transactions"
gold_table = f"{catalog}.{schema}.gold_transaction_features"

silver_df = spark.table(silver_table)

customer_time_window = Window.partitionBy("customer_id").orderBy(F.col("event_ts").cast("long")).rangeBetween(-600, 0)
card_time_window = Window.partitionBy("card_id").orderBy(F.col("event_ts").cast("long")).rangeBetween(-600, 0)
customer_history_window = Window.partitionBy("customer_id")

features_df = (
    silver_df
    .withColumn("customer_txn_count_10min", F.count("*").over(customer_time_window))
    .withColumn("card_txn_count_10min", F.count("*").over(card_time_window))
    .withColumn("customer_avg_amount", F.avg("amount").over(customer_history_window))
    .withColumn("customer_max_amount", F.max("amount").over(customer_history_window))
    .withColumn("amount_to_customer_avg_ratio", F.round(F.col("amount") / F.greatest(F.col("customer_avg_amount"), F.lit(1.0)), 2))
    .withColumn("is_velocity_risk", (F.col("customer_txn_count_10min") >= 5).cast("int"))
    .withColumn("is_amount_spike", (F.col("amount_to_customer_avg_ratio") >= 3).cast("int"))
    .withColumn("feature_generated_at", F.current_timestamp())
)

features_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_table)

print(f"Gold feature rows written: {features_df.count()}")
print(f"Table: {gold_table}")

display(
    spark.table(gold_table)
    .select("transaction_id", "customer_id", "amount", "customer_txn_count_10min", "card_txn_count_10min", "amount_to_customer_avg_ratio", "is_cross_border", "is_high_amount", "is_velocity_risk", "is_amount_spike")
    .orderBy(F.desc("feature_generated_at"))
    .limit(10)
)
