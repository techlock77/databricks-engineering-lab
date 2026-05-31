# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze Ingest

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "fraud_demo_dev")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

source_table = f"{catalog}.{schema}.source_transactions"
bronze_table = f"{catalog}.{schema}.bronze_transactions"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

def table_exists(table_name: str) -> bool:
    try:
        spark.table(table_name).limit(1).collect()
        return True
    except Exception:
        return False

source_df = spark.table(source_table)

bronze_df = (
    source_df
    .withColumn("bronze_ingested_at", F.current_timestamp())
    .withColumn("source_system", F.lit("simulated_card_processor"))
    .withColumn("raw_payload", F.to_json(F.struct(*[F.col(c) for c in source_df.columns])))
)

if table_exists(bronze_table):
    existing_ids = spark.table(bronze_table).select("transaction_id").distinct()
    bronze_df = bronze_df.join(existing_ids, on="transaction_id", how="left_anti")

new_count = bronze_df.count()

if new_count > 0:
    bronze_df.write.format("delta").mode("append").saveAsTable(bronze_table)

print(f"New Bronze records inserted: {new_count}")
print(f"Table: {bronze_table}")

display(spark.table(bronze_table).orderBy(F.desc("bronze_ingested_at")).limit(10))
