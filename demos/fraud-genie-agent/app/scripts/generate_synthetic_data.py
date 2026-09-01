# Databricks notebook source
# MAGIC %md
# MAGIC # MuleGraph Investigator: Generate and Load Synthetic Data
# MAGIC
# MAGIC This notebook generates scaled synthetic fraud investigation data and writes
# MAGIC it to Unity Catalog Delta tables. Run this in your Databricks workspace to
# MAGIC populate the Gold tables that Genie can query.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC 1. Run `scripts/sql/01_setup_catalog_and_schema.sql` first to create the
# MAGIC    catalog, schema, and table definitions.
# MAGIC 2. This notebook assumes the standard Databricks notebook environment with
# MAGIC    `spark` and `dbutils` already available.
# MAGIC
# MAGIC **Note:** This script cannot be executed in a local sandbox without a live
# MAGIC Databricks connection. It is designed to run in your actual Databricks
# MAGIC workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration widgets
# MAGIC Edit these values to match your workspace setup.

# COMMAND ----------

dbutils.widgets.text("catalog", "mulegraph", "Catalog name")
dbutils.widgets.text("schema", "investigations", "Schema name")
dbutils.widgets.text("scale_factor", "1", "Curated nine-scenario dataset (compatibility setting)")
dbutils.widgets.text("seed", "42", "Random seed for reproducibility")
dbutils.widgets.text(
    "repo_path",
    "/Workspace/Repos/<your-username>/mulegraph-implementation",
    "Path to repo root (e.g. /Workspace/Repos/user@example.com/mulegraph-implementation)"
)

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
scale_factor = int(dbutils.widgets.get("scale_factor"))
seed = int(dbutils.widgets.get("seed"))
repo_path = dbutils.widgets.get("repo_path")

print(f"Configuration:")
print(f"  Catalog:      {catalog}")
print(f"  Schema:       {schema}")
print(f"  Scale factor: {scale_factor}")
print(f"  Seed:         {seed}")
print(f"  Repo path:    {repo_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import the pipeline module
# MAGIC
# MAGIC First tries importing without modifying sys.path (in case repo is already on path
# MAGIC via Databricks Repos). Falls back to explicit repo_path widget if needed.

# COMMAND ----------

import os
import sys

def try_import():
    """Attempt to import the pipeline module."""
    from src.pipeline.orchestrator import run_pipeline
    from src.pipeline.gold import GOLD_TABLE_NAMES
    return run_pipeline, GOLD_TABLE_NAMES

try:
    run_pipeline, GOLD_TABLE_NAMES = try_import()
    print("Successfully imported from existing sys.path (Databricks Repos).")
except ImportError:
    if "<your-username>" in repo_path:
        raise ValueError(
            f"ERROR: repo_path widget still contains placeholder '<your-username>'.\n"
            f"Please edit the 'repo_path' widget to point to your actual repo location.\n"
            f"Example: /Workspace/Repos/user@example.com/mulegraph-implementation\n"
            f"Current value: {repo_path}"
        )

    src_path = os.path.join(repo_path, "src")
    if not os.path.isdir(src_path):
        raise ValueError(
            f"ERROR: Could not find 'src' directory at {src_path}.\n"
            f"Please verify the 'repo_path' widget points to the repo root.\n"
            f"The path should contain: src/pipeline/orchestrator.py\n"
            f"Current repo_path: {repo_path}"
        )

    sys.path.insert(0, repo_path)
    try:
        run_pipeline, GOLD_TABLE_NAMES = try_import()
        print(f"Successfully imported from repo_path: {repo_path}")
    except ImportError as e:
        raise ImportError(
            f"ERROR: Failed to import from {repo_path}.\n"
            f"The 'src' directory exists but imports failed.\n"
            f"Original error: {e}\n"
            f"Please verify the repo is complete and has no missing files."
        ) from e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate synthetic data using the pipeline

# COMMAND ----------

result = run_pipeline(seed=seed, scale_factor=scale_factor)

print(f"Generated {len(result.gold['case_summary'])} independently selectable scenario cases")
print(f"Gold tables: {GOLD_TABLE_NAMES}")
print(f"case_summary rows: {len(result.gold['case_summary'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Gold tables to Unity Catalog

# COMMAND ----------

def quote_identifier(name):
    """Quote a SQL identifier with backticks, escaping any existing backticks."""
    return "`" + name.replace("`", "``") + "`"

catalog_quoted = quote_identifier(catalog)
schema_quoted = quote_identifier(schema)

spark.sql(f"USE CATALOG {catalog_quoted}")
spark.sql(f"USE SCHEMA {schema_quoted}")

for table_name in GOLD_TABLE_NAMES:
    pandas_df = result.gold[table_name]
    spark_df = spark.createDataFrame(pandas_df)

    full_table_name = f"{catalog_quoted}.{schema_quoted}.{quote_identifier(table_name)}"
    spark_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table_name)

    row_count = spark.table(full_table_name).count()
    print(f"Wrote {row_count:,} rows to {catalog}.{schema}.{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify data load

# COMMAND ----------

print("\nData load summary:")
print("-" * 60)

for table_name in GOLD_TABLE_NAMES:
    full_table_name = f"{catalog_quoted}.{schema_quoted}.{quote_identifier(table_name)}"
    count = spark.table(full_table_name).count()
    print(f"{table_name:25} {count:>10,} rows")

print("-" * 60)
print("Data generation complete. Genie can now query these tables.")
