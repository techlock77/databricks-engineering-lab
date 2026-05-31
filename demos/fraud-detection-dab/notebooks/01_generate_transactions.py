# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Generate Simulated Transaction Events

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "fraud_demo_dev")
dbutils.widgets.text("num_records", "1000")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
num_records = int(dbutils.widgets.get("num_records"))

source_table = f"{catalog}.{schema}.source_transactions"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

batch_id = spark.sql("SELECT uuid() AS batch_id").first()["batch_id"]

countries = F.array(F.lit("US"), F.lit("CA"), F.lit("GB"), F.lit("IN"), F.lit("MX"))
merchant_categories = F.array(F.lit("grocery"), F.lit("fuel"), F.lit("electronics"), F.lit("travel"), F.lit("luxury"), F.lit("gaming"))
payment_channels = F.array(F.lit("card_present"), F.lit("online"), F.lit("mobile_wallet"), F.lit("atm"))

df = (
    spark.range(num_records)
    .withColumn("batch_id", F.lit(batch_id))
    .withColumn("transaction_id", F.sha2(F.concat_ws(":", F.lit(batch_id), F.col("id")), 256))
    .withColumn("customer_id", F.concat(F.lit("C"), F.lpad((F.col("id") % 250).cast("string"), 5, "0")))
    .withColumn("card_id", F.concat(F.lit("CARD"), F.lpad((F.col("id") % 400).cast("string"), 6, "0")))
    .withColumn("event_ts", F.expr("current_timestamp() - INTERVAL 1 SECOND * cast(rand() * 3600 as int)"))
    .withColumn("amount", F.round(F.when(F.rand() > 0.96, F.rand() * 4500 + 1000).otherwise(F.rand() * 250 + 5), 2))
    .withColumn("merchant_category", F.element_at(merchant_categories, (F.floor(F.rand() * 6) + 1).cast("int")))
    .withColumn("payment_channel", F.element_at(payment_channels, (F.floor(F.rand() * 4) + 1).cast("int")))
    .withColumn("transaction_country", F.element_at(countries, (F.floor(F.rand() * 5) + 1).cast("int")))
    .withColumn("customer_home_country", F.when(F.rand() > 0.12, F.lit("US")).otherwise(F.element_at(countries, (F.floor(F.rand() * 5) + 1).cast("int"))))
    .withColumn("device_id", F.concat(F.lit("D"), F.lpad((F.floor(F.rand() * 700)).cast("string"), 6, "0")))
    .withColumn("ip_address", F.concat(F.lit("10."), (F.floor(F.rand()*255)).cast("int"), F.lit("."), (F.floor(F.rand()*255)).cast("int"), F.lit("."), (F.floor(F.rand()*255)).cast("int")))
    .withColumn("source_created_at", F.current_timestamp())
    .drop("id")
)

df.write.format("delta").mode("append").saveAsTable(source_table)

print(f"Generated {df.count()} source transactions")
print(f"Batch ID: {batch_id}")
print(f"Table: {source_table}")

display(spark.table(source_table).where(F.col("batch_id") == batch_id).orderBy(F.desc("event_ts")).limit(10))
