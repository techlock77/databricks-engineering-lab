# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver Transform

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "fraud_demo_dev")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

bronze_table = f"{catalog}.{schema}.bronze_transactions"
silver_table = f"{catalog}.{schema}.silver_transactions"

def table_exists(table_name: str) -> bool:
    try:
        spark.table(table_name).limit(1).collect()
        return True
    except Exception:
        return False

bronze_df = spark.table(bronze_table)

silver_df = (
    bronze_df
    .where(F.col("transaction_id").isNotNull())
    .where(F.col("customer_id").isNotNull())
    .where(F.col("amount").isNotNull())
    .where(F.col("amount") > 0)
    .withColumn("amount", F.col("amount").cast("double"))
    .withColumn("event_date", F.to_date("event_ts"))
    .withColumn("event_hour", F.hour("event_ts"))
    .withColumn("is_cross_border", (F.col("transaction_country") != F.col("customer_home_country")).cast("int"))
    .withColumn("is_high_amount", (F.col("amount") >= F.lit(1000)).cast("int"))
    .withColumn("is_late_night", ((F.col("event_hour") >= 0) & (F.col("event_hour") <= 5)).cast("int"))
    .withColumn("silver_processed_at", F.current_timestamp())
)

if table_exists(silver_table):
    existing_ids = spark.table(silver_table).select("transaction_id").distinct()
    silver_df = silver_df.join(existing_ids, on="transaction_id", how="left_anti")

new_count = silver_df.count()

if new_count > 0:
    silver_df.write.format("delta").mode("append").saveAsTable(silver_table)

print(f"New Silver records inserted: {new_count}")
print(f"Table: {silver_table}")

display(spark.table(silver_table).orderBy(F.desc("silver_processed_at")).limit(10))
