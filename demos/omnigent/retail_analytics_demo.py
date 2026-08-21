{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "42e4ac74-9087-439e-b434-402b61ee6f84",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# Retail lakehouse analytics: bronze to forecast\n",
    "\n",
    "This self-contained demo creates synthetic point-of-sale events, builds managed Unity Catalog\n",
    "bronze/silver/gold tables, forecasts total daily net revenue, validates the results, and produces\n",
    "a Genie-ready semantic prompt. It uses only Python, PySpark, Spark SQL, and Spark ML.\n",
    "\n",
    "**Expected runtime:** a few minutes on a small Databricks Free Edition compute resource."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "82be73ff-33b8-40dd-9ed2-195c40252687",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## 0. Configuration\n",
    "\n",
    "Choose a Unity Catalog catalog in which you can create a schema and managed tables. The default\n",
    "is the catalog currently selected in the notebook. Re-running the notebook safely replaces all\n",
    "demo tables."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1787268697672,
     "inputWidgets": {},
     "nuid": "84155354-9cb6-4660-873c-336a69085728",
     "showTitle": false,
     "startTime": 1787268674134,
     "submitTime": 1787268671563,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Writing managed demo tables to workspace.retail_meetup_demo; base event count = 30,000.\n"
     ]
    }
   ],
   "source": [
    "import re\n",
    "\n",
    "from pyspark.ml.feature import VectorAssembler\n",
    "from pyspark.ml.regression import LinearRegression\n",
    "from pyspark.sql import functions as F\n",
    "from pyspark.sql import types as T\n",
    "from pyspark.sql.window import Window\n",
    "\n",
    "dbutils.widgets.text(\"catalog\", spark.catalog.currentCatalog(), \"01 Unity Catalog catalog\")\n",
    "dbutils.widgets.text(\"schema\", \"retail_meetup_demo\", \"02 Demo schema\")\n",
    "dbutils.widgets.text(\"event_count\", \"30000\", \"03 Base event count\")\n",
    "dbutils.widgets.text(\"forecast_days\", \"14\", \"04 Forecast horizon (days)\")\n",
    "\n",
    "catalog = dbutils.widgets.get(\"catalog\").strip()\n",
    "schema = dbutils.widgets.get(\"schema\").strip()\n",
    "event_count = int(dbutils.widgets.get(\"event_count\"))\n",
    "forecast_days = int(dbutils.widgets.get(\"forecast_days\"))\n",
    "\n",
    "# Identifiers are validated before interpolation into SQL/table names.\n",
    "identifier_pattern = re.compile(r\"^[A-Za-z_][A-Za-z0-9_]*$\")\n",
    "if not identifier_pattern.fullmatch(catalog) or not identifier_pattern.fullmatch(schema):\n",
    "    raise ValueError(\"Catalog and schema must contain only letters, digits, and underscores, and cannot start with a digit.\")\n",
    "if event_count < 1000 or event_count > 500000:\n",
    "    raise ValueError(\"event_count must be between 1,000 and 500,000.\")\n",
    "if forecast_days < 1 or forecast_days > 90:\n",
    "    raise ValueError(\"forecast_days must be between 1 and 90.\")\n",
    "\n",
    "namespace = f\"`{catalog}`.`{schema}`\"\n",
    "\n",
    "def table(name: str) -> str:\n",
    "    return f\"{catalog}.{schema}.{name}\"\n",
    "\n",
    "spark.sql(f\"CREATE SCHEMA IF NOT EXISTS {namespace} COMMENT 'Self-contained retail analytics meetup demo'\")\n",
    "spark.sql(f\"USE CATALOG `{catalog}`\")\n",
    "spark.sql(f\"USE SCHEMA `{schema}`\")\n",
    "\n",
    "print(f\"Writing managed demo tables to {catalog}.{schema}; base event count = {event_count:,}.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "dc76093a-8474-4918-a5f5-bf31a66d3913",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## 1. Generate synthetic retail events\n",
    "\n",
    "The data covers 120 days across stores, channels, and product categories. Demand includes a\n",
    "weekend effect and gradual trend so the dashboard and forecast have visible patterns. A small,\n",
    "deterministic set of duplicate IDs, negative quantities, invalid statuses, and missing customer\n",
    "IDs lets the silver phase demonstrate practical quality handling."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1787268710391,
     "inputWidgets": {},
     "nuid": "9f364491-78a0-48f1-8c2c-dc12b9e9878c",
     "showTitle": false,
     "startTime": 1787268697941,
     "submitTime": 1787268671582,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>event_id</th><th>event_ts</th><th>store_id</th><th>region</th><th>channel</th><th>category</th><th>product_id</th><th>customer_id</th><th>quantity</th><th>unit_price</th><th>discount_pct</th><th>payment_type</th><th>order_status</th><th>source_copy_sequence</th></tr></thead><tbody><tr><td>evt-00000000</td><td>2025-01-01T00:00:00.000Z</td><td>store-01</td><td>North</td><td>store</td><td>Grocery</td><td>sku-0001</td><td>null</td><td>-1</td><td>4.99</td><td>0.2</td><td>card</td><td>UNKNOWN</td><td>0</td></tr><tr><td>evt-00000001</td><td>2025-01-02T02:11:59.000Z</td><td>store-08</td><td>West</td><td>web</td><td>Electronics</td><td>sku-0002</td><td>cust-00017</td><td>2</td><td>6.42</td><td>0.0</td><td>cash</td><td>COMPLETED</td><td>0</td></tr><tr><td>evt-00000002</td><td>2025-01-03T04:23:58.000Z</td><td>store-03</td><td>East</td><td>mobile</td><td>Home</td><td>sku-0003</td><td>cust-00034</td><td>3</td><td>7.85</td><td>0.0</td><td>wallet</td><td>COMPLETED</td><td>0</td></tr><tr><td>evt-00000003</td><td>2025-01-04T06:35:57.000Z</td><td>store-10</td><td>South</td><td>store</td><td>Apparel</td><td>sku-0004</td><td>cust-00051</td><td>2</td><td>9.28</td><td>0.0</td><td>card</td><td>COMPLETED</td><td>0</td></tr><tr><td>evt-00000004</td><td>2025-01-05T08:47:56.000Z</td><td>store-05</td><td>North</td><td>web</td><td>Beauty</td><td>sku-0005</td><td>cust-00068</td><td>3</td><td>10.71</td><td>0.1</td><td>cash</td><td>COMPLETED</td><td>0</td></tr><tr><td>evt-00000005</td><td>2025-01-06T10:59:55.000Z</td><td>store-12</td><td>West</td><td>store</td><td>Sports</td><td>sku-0006</td><td>cust-00085</td><td>3</td><td>12.14</td><td>0.0</td><td>wallet</td><td>COMPLETED</td><td>0</td></tr><tr><td>evt-00000006</td><td>2025-01-07T13:11:54.000Z</td><td>store-07</td><td>East</td><td>web</td><td>Grocery</td><td>sku-0007</td><td>cust-00102</td><td>1</td><td>13.57</td><td>0.0</td><td>card</td><td>COMPLETED</td><td>0</td></tr><tr><td>evt-00000007</td><td>2025-01-08T15:23:53.000Z</td><td>store-02</td><td>South</td><td>mobile</td><td>Electronics</td><td>sku-0008</td><td>cust-00119</td><td>2</td><td>15.0</td><td>0.0</td><td>cash</td><td>COMPLETED</td><td>0</td></tr><tr><td>evt-00000008</td><td>2025-01-09T17:35:52.000Z</td><td>store-09</td><td>North</td><td>store</td><td>Home</td><td>sku-0009</td><td>cust-00136</td><td>3</td><td>16.43</td><td>0.1</td><td>wallet</td><td>COMPLETED</td><td>0</td></tr><tr><td>evt-00000009</td><td>2025-01-10T19:47:51.000Z</td><td>store-04</td><td>West</td><td>web</td><td>Apparel</td><td>sku-0010</td><td>cust-00153</td><td>1</td><td>17.86</td><td>0.0</td><td>card</td><td>COMPLETED</td><td>0</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "evt-00000000",
         "2025-01-01T00:00:00.000Z",
         "store-01",
         "North",
         "store",
         "Grocery",
         "sku-0001",
         null,
         -1,
         4.99,
         0.2,
         "card",
         "UNKNOWN",
         0
        ],
        [
         "evt-00000001",
         "2025-01-02T02:11:59.000Z",
         "store-08",
         "West",
         "web",
         "Electronics",
         "sku-0002",
         "cust-00017",
         2,
         6.42,
         0.0,
         "cash",
         "COMPLETED",
         0
        ],
        [
         "evt-00000002",
         "2025-01-03T04:23:58.000Z",
         "store-03",
         "East",
         "mobile",
         "Home",
         "sku-0003",
         "cust-00034",
         3,
         7.85,
         0.0,
         "wallet",
         "COMPLETED",
         0
        ],
        [
         "evt-00000003",
         "2025-01-04T06:35:57.000Z",
         "store-10",
         "South",
         "store",
         "Apparel",
         "sku-0004",
         "cust-00051",
         2,
         9.28,
         0.0,
         "card",
         "COMPLETED",
         0
        ],
        [
         "evt-00000004",
         "2025-01-05T08:47:56.000Z",
         "store-05",
         "North",
         "web",
         "Beauty",
         "sku-0005",
         "cust-00068",
         3,
         10.71,
         0.1,
         "cash",
         "COMPLETED",
         0
        ],
        [
         "evt-00000005",
         "2025-01-06T10:59:55.000Z",
         "store-12",
         "West",
         "store",
         "Sports",
         "sku-0006",
         "cust-00085",
         3,
         12.14,
         0.0,
         "wallet",
         "COMPLETED",
         0
        ],
        [
         "evt-00000006",
         "2025-01-07T13:11:54.000Z",
         "store-07",
         "East",
         "web",
         "Grocery",
         "sku-0007",
         "cust-00102",
         1,
         13.57,
         0.0,
         "card",
         "COMPLETED",
         0
        ],
        [
         "evt-00000007",
         "2025-01-08T15:23:53.000Z",
         "store-02",
         "South",
         "mobile",
         "Electronics",
         "sku-0008",
         "cust-00119",
         2,
         15.0,
         0.0,
         "cash",
         "COMPLETED",
         0
        ],
        [
         "evt-00000008",
         "2025-01-09T17:35:52.000Z",
         "store-09",
         "North",
         "store",
         "Home",
         "sku-0009",
         "cust-00136",
         3,
         16.43,
         0.1,
         "wallet",
         "COMPLETED",
         0
        ],
        [
         "evt-00000009",
         "2025-01-10T19:47:51.000Z",
         "store-04",
         "West",
         "web",
         "Apparel",
         "sku-0010",
         "cust-00153",
         1,
         17.86,
         0.0,
         "card",
         "COMPLETED",
         0
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "event_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "event_ts",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "store_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "region",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "channel",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "category",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "product_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "quantity",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "unit_price",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "discount_pct",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "payment_type",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "order_status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "source_copy_sequence",
         "type": "\"integer\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Generated 30,150 input rows, including deterministic data-quality cases.\n"
     ]
    }
   ],
   "source": [
    "categories = F.array(*[F.lit(x) for x in [\"Grocery\", \"Electronics\", \"Home\", \"Apparel\", \"Beauty\", \"Sports\"]])\n",
    "regions = F.array(*[F.lit(x) for x in [\"North\", \"South\", \"East\", \"West\"]])\n",
    "channels = F.array(*[F.lit(x) for x in [\"store\", \"web\", \"mobile\"]])\n",
    "payment_types = F.array(*[F.lit(x) for x in [\"card\", \"cash\", \"wallet\"]])\n",
    "\n",
    "base = (\n",
    "    spark.range(event_count)\n",
    "    .withColumn(\"day_number\", (F.col(\"id\") % 120).cast(\"int\"))\n",
    "    .withColumn(\"second_in_day\", ((F.col(\"id\") * 7919) % 86400).cast(\"long\"))\n",
    "    .withColumn(\"store_number\", ((F.col(\"id\") * 7 + F.floor(F.col(\"id\") / 120)) % 12 + 1).cast(\"int\"))\n",
    "    .withColumn(\n",
    "        \"event_ts\",\n",
    "        F.to_timestamp(\n",
    "            F.from_unixtime(\n",
    "                F.unix_timestamp(F.lit(\"2025-01-01 00:00:00\"))\n",
    "                + F.col(\"day_number\") * F.lit(86400)\n",
    "                + F.col(\"second_in_day\")\n",
    "            )\n",
    "        ),\n",
    "    )\n",
    "    .withColumn(\"event_id\", F.format_string(\"evt-%08d\", F.col(\"id\")))\n",
    "    .withColumn(\"store_id\", F.format_string(\"store-%02d\", F.col(\"store_number\")))\n",
    "    .withColumn(\"region\", F.element_at(regions, ((F.col(\"store_number\") - 1) % 4 + 1).cast(\"int\")))\n",
    "    .withColumn(\"channel\", F.element_at(channels, ((F.floor(F.col(\"id\") / 5) + F.col(\"day_number\")) % 3 + 1).cast(\"int\")))\n",
    "    .withColumn(\"category\", F.element_at(categories, ((F.floor(F.col(\"id\") / 11) + F.col(\"day_number\")) % 6 + 1).cast(\"int\")))\n",
    "    .withColumn(\"product_id\", F.format_string(\"sku-%04d\", (F.col(\"id\") % 240) + 1))\n",
    "    .withColumn(\n",
    "        \"customer_id\",\n",
    "        F.when(F.col(\"id\") % 137 == 0, F.lit(None).cast(\"string\")).otherwise(\n",
    "            F.format_string(\"cust-%05d\", (F.col(\"id\") * 17) % 5000)\n",
    "        ),\n",
    "    )\n",
    "    .withColumn(\n",
    "        \"quantity\",\n",
    "        F.when(F.col(\"id\") % 503 == 0, -1).otherwise(\n",
    "            (F.col(\"id\") % 3 + 1).cast(\"int\")\n",
    "            + F.when(F.dayofweek(\"event_ts\").isin(1, 7), 1).otherwise(0)\n",
    "            + F.when(F.col(\"day_number\") >= 80, 1).otherwise(0)\n",
    "        ),\n",
    "    )\n",
    "    .withColumn(\"unit_price\", F.round(F.lit(4.99) + (F.col(\"id\") % 90) * F.lit(1.35) + F.col(\"day_number\") * F.lit(0.08), 2))\n",
    "    .withColumn(\"discount_pct\", F.when(F.col(\"id\") % 10 == 0, F.lit(0.20)).when(F.col(\"id\") % 4 == 0, F.lit(0.10)).otherwise(F.lit(0.0)))\n",
    "    .withColumn(\"payment_type\", F.element_at(payment_types, ((F.col(\"id\") * 7) % 3 + 1).cast(\"int\")))\n",
    "    .withColumn(\"order_status\", F.when(F.col(\"id\") % 431 == 0, \"UNKNOWN\").when(F.col(\"id\") % 29 == 0, \"RETURNED\").otherwise(\"COMPLETED\"))\n",
    "    .select(\n",
    "        \"event_id\", \"event_ts\", \"store_id\", \"region\", \"channel\", \"category\", \"product_id\",\n",
    "        \"customer_id\", \"quantity\", \"unit_price\", \"discount_pct\", \"payment_type\", \"order_status\"\n",
    "    )\n",
    ")\n",
    "\n",
    "# Repeat a small subset with the same business key to exercise deduplication. The source copy sequence\n",
    "# provides a deterministic tie-breaker when two records have the same key and ingestion timestamp.\n",
    "base_records = base.withColumn(\"source_copy_sequence\", F.lit(0))\n",
    "duplicates = (\n",
    "    base.where(F.regexp_extract(\"event_id\", r\"(\\d+)$\", 1).cast(\"long\") % 200 == 0)\n",
    "    .withColumn(\"source_copy_sequence\", F.lit(1))\n",
    ")\n",
    "synthetic_events = base_records.unionByName(duplicates)\n",
    "\n",
    "display(synthetic_events.limit(10))\n",
    "print(f\"Generated {synthetic_events.count():,} input rows, including deterministic data-quality cases.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "1e81601d-1a2f-457d-80f6-2f9a4539308b",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## 2. Bronze: retain events as received\n",
    "\n",
    "Bronze preserves the source columns and adds ingestion metadata. `saveAsTable` creates a Unity\n",
    "Catalog **managed Delta table** because no external path is supplied."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1787268731013,
     "inputWidgets": {},
     "nuid": "0d4fa5f0-ce41-435d-99e8-53900e7b2ab6",
     "showTitle": false,
     "startTime": 1787268710731,
     "submitTime": 1787268671600,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>event_id</th><th>event_ts</th><th>store_id</th><th>region</th><th>channel</th><th>category</th><th>product_id</th><th>customer_id</th><th>quantity</th><th>unit_price</th><th>discount_pct</th><th>payment_type</th><th>order_status</th><th>source_copy_sequence</th><th>ingested_at</th><th>source_system</th><th>ingestion_date</th></tr></thead><tbody><tr><td>evt-00011250</td><td>2025-04-01T02:52:30.000Z</td><td>store-04</td><td>West</td><td>store</td><td>Home</td><td>sku-0211</td><td>cust-01250</td><td>2</td><td>12.19</td><td>0.2</td><td>card</td><td>COMPLETED</td><td>0</td><td>2026-08-20T23:31:56.304Z</td><td>synthetic_pos</td><td>2026-08-20</td></tr><tr><td>evt-00011251</td><td>2025-04-02T05:04:29.000Z</td><td>store-11</td><td>East</td><td>web</td><td>Apparel</td><td>sku-0212</td><td>cust-01267</td><td>3</td><td>13.62</td><td>0.0</td><td>cash</td><td>COMPLETED</td><td>0</td><td>2026-08-20T23:31:56.304Z</td><td>synthetic_pos</td><td>2026-08-20</td></tr><tr><td>evt-00011252</td><td>2025-04-03T07:16:28.000Z</td><td>store-06</td><td>South</td><td>mobile</td><td>Beauty</td><td>sku-0213</td><td>cust-01284</td><td>4</td><td>15.05</td><td>0.1</td><td>wallet</td><td>RETURNED</td><td>0</td><td>2026-08-20T23:31:56.304Z</td><td>synthetic_pos</td><td>2026-08-20</td></tr><tr><td>evt-00011253</td><td>2025-04-04T09:28:27.000Z</td><td>store-01</td><td>North</td><td>store</td><td>Grocery</td><td>sku-0214</td><td>cust-01301</td><td>2</td><td>16.48</td><td>0.0</td><td>card</td><td>COMPLETED</td><td>0</td><td>2026-08-20T23:31:56.304Z</td><td>synthetic_pos</td><td>2026-08-20</td></tr><tr><td>evt-00011254</td><td>2025-04-05T11:40:26.000Z</td><td>store-08</td><td>West</td><td>web</td><td>Electronics</td><td>sku-0215</td><td>cust-01318</td><td>4</td><td>17.91</td><td>0.0</td><td>cash</td><td>COMPLETED</td><td>0</td><td>2026-08-20T23:31:56.304Z</td><td>synthetic_pos</td><td>2026-08-20</td></tr><tr><td>evt-00011255</td><td>2025-04-06T13:52:25.000Z</td><td>store-03</td><td>East</td><td>store</td><td>Home</td><td>sku-0216</td><td>cust-01335</td><td>5</td><td>19.34</td><td>0.0</td><td>wallet</td><td>COMPLETED</td><td>0</td><td>2026-08-20T23:31:56.304Z</td><td>synthetic_pos</td><td>2026-08-20</td></tr><tr><td>evt-00011256</td><td>2025-04-07T16:04:24.000Z</td><td>store-10</td><td>South</td><td>web</td><td>Apparel</td><td>sku-0217</td><td>cust-01352</td><td>2</td><td>20.77</td><td>0.1</td><td>card</td><td>COMPLETED</td><td>0</td><td>2026-08-20T23:31:56.304Z</td><td>synthetic_pos</td><td>2026-08-20</td></tr><tr><td>evt-00011257</td><td>2025-04-08T18:16:23.000Z</td><td>store-05</td><td>North</td><td>mobile</td><td>Beauty</td><td>sku-0218</td><td>cust-01369</td><td>3</td><td>22.2</td><td>0.0</td><td>cash</td><td>COMPLETED</td><td>0</td><td>2026-08-20T23:31:56.304Z</td><td>synthetic_pos</td><td>2026-08-20</td></tr><tr><td>evt-00011258</td><td>2025-04-09T20:28:22.000Z</td><td>store-12</td><td>West</td><td>store</td><td>Sports</td><td>sku-0219</td><td>cust-01386</td><td>4</td><td>23.63</td><td>0.0</td><td>wallet</td><td>COMPLETED</td><td>0</td><td>2026-08-20T23:31:56.304Z</td><td>synthetic_pos</td><td>2026-08-20</td></tr><tr><td>evt-00011259</td><td>2025-04-10T22:40:21.000Z</td><td>store-07</td><td>East</td><td>web</td><td>Grocery</td><td>sku-0220</td><td>cust-01403</td><td>2</td><td>25.06</td><td>0.0</td><td>card</td><td>COMPLETED</td><td>0</td><td>2026-08-20T23:31:56.304Z</td><td>synthetic_pos</td><td>2026-08-20</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "evt-00011250",
         "2025-04-01T02:52:30.000Z",
         "store-04",
         "West",
         "store",
         "Home",
         "sku-0211",
         "cust-01250",
         2,
         12.19,
         0.2,
         "card",
         "COMPLETED",
         0,
         "2026-08-20T23:31:56.304Z",
         "synthetic_pos",
         "2026-08-20"
        ],
        [
         "evt-00011251",
         "2025-04-02T05:04:29.000Z",
         "store-11",
         "East",
         "web",
         "Apparel",
         "sku-0212",
         "cust-01267",
         3,
         13.62,
         0.0,
         "cash",
         "COMPLETED",
         0,
         "2026-08-20T23:31:56.304Z",
         "synthetic_pos",
         "2026-08-20"
        ],
        [
         "evt-00011252",
         "2025-04-03T07:16:28.000Z",
         "store-06",
         "South",
         "mobile",
         "Beauty",
         "sku-0213",
         "cust-01284",
         4,
         15.05,
         0.1,
         "wallet",
         "RETURNED",
         0,
         "2026-08-20T23:31:56.304Z",
         "synthetic_pos",
         "2026-08-20"
        ],
        [
         "evt-00011253",
         "2025-04-04T09:28:27.000Z",
         "store-01",
         "North",
         "store",
         "Grocery",
         "sku-0214",
         "cust-01301",
         2,
         16.48,
         0.0,
         "card",
         "COMPLETED",
         0,
         "2026-08-20T23:31:56.304Z",
         "synthetic_pos",
         "2026-08-20"
        ],
        [
         "evt-00011254",
         "2025-04-05T11:40:26.000Z",
         "store-08",
         "West",
         "web",
         "Electronics",
         "sku-0215",
         "cust-01318",
         4,
         17.91,
         0.0,
         "cash",
         "COMPLETED",
         0,
         "2026-08-20T23:31:56.304Z",
         "synthetic_pos",
         "2026-08-20"
        ],
        [
         "evt-00011255",
         "2025-04-06T13:52:25.000Z",
         "store-03",
         "East",
         "store",
         "Home",
         "sku-0216",
         "cust-01335",
         5,
         19.34,
         0.0,
         "wallet",
         "COMPLETED",
         0,
         "2026-08-20T23:31:56.304Z",
         "synthetic_pos",
         "2026-08-20"
        ],
        [
         "evt-00011256",
         "2025-04-07T16:04:24.000Z",
         "store-10",
         "South",
         "web",
         "Apparel",
         "sku-0217",
         "cust-01352",
         2,
         20.77,
         0.1,
         "card",
         "COMPLETED",
         0,
         "2026-08-20T23:31:56.304Z",
         "synthetic_pos",
         "2026-08-20"
        ],
        [
         "evt-00011257",
         "2025-04-08T18:16:23.000Z",
         "store-05",
         "North",
         "mobile",
         "Beauty",
         "sku-0218",
         "cust-01369",
         3,
         22.2,
         0.0,
         "cash",
         "COMPLETED",
         0,
         "2026-08-20T23:31:56.304Z",
         "synthetic_pos",
         "2026-08-20"
        ],
        [
         "evt-00011258",
         "2025-04-09T20:28:22.000Z",
         "store-12",
         "West",
         "store",
         "Sports",
         "sku-0219",
         "cust-01386",
         4,
         23.63,
         0.0,
         "wallet",
         "COMPLETED",
         0,
         "2026-08-20T23:31:56.304Z",
         "synthetic_pos",
         "2026-08-20"
        ],
        [
         "evt-00011259",
         "2025-04-10T22:40:21.000Z",
         "store-07",
         "East",
         "web",
         "Grocery",
         "sku-0220",
         "cust-01403",
         2,
         25.06,
         0.0,
         "card",
         "COMPLETED",
         0,
         "2026-08-20T23:31:56.304Z",
         "synthetic_pos",
         "2026-08-20"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "event_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "event_ts",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "store_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "region",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "channel",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "category",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "product_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "quantity",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "unit_price",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "discount_pct",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "payment_type",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "order_status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "source_copy_sequence",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "ingested_at",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "source_system",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "ingestion_date",
         "type": "\"date\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "bronze = (\n",
    "    synthetic_events\n",
    "    .withColumn(\"ingested_at\", F.current_timestamp())\n",
    "    .withColumn(\"source_system\", F.lit(\"synthetic_pos\"))\n",
    "    .withColumn(\"ingestion_date\", F.current_date())\n",
    ")\n",
    "\n",
    "(\n",
    "    bronze.write\n",
    "    .format(\"delta\")\n",
    "    .mode(\"overwrite\")\n",
    "    .option(\"overwriteSchema\", \"true\")\n",
    "    .saveAsTable(table(\"bronze_retail_events\"))\n",
    ")\n",
    "\n",
    "spark.sql(f\"COMMENT ON TABLE {namespace}.`bronze_retail_events` IS 'Raw synthetic point-of-sale events with ingestion metadata'\")\n",
    "display(spark.table(table(\"bronze_retail_events\")).limit(10))"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "578abbc7-861b-4b3e-8ae8-9cf1d34adc6c",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## 3. Silver: validate, deduplicate, and enrich\n",
    "\n",
    "Silver keeps valid completed/returned transactions, removes duplicate business keys, labels\n",
    "anonymous shoppers, and derives signed revenue. Returns therefore reduce units and revenue."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1787268739366,
     "inputWidgets": {},
     "nuid": "d7b657d4-4d08-486c-b667-9ede0d389007",
     "showTitle": false,
     "startTime": 1787268731048,
     "submitTime": 1787268671620,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>event_id</th><th>event_ts</th><th>store_id</th><th>region</th><th>channel</th><th>category</th><th>product_id</th><th>customer_id</th><th>quantity</th><th>unit_price</th><th>discount_pct</th><th>payment_type</th><th>order_status</th><th>source_copy_sequence</th><th>ingested_at</th><th>source_system</th><th>ingestion_date</th><th>event_date</th><th>gross_revenue</th><th>net_revenue</th><th>signed_units</th></tr></thead><tbody><tr><td>evt-00029880</td><td>2025-01-01T15:42:00.000Z</td><td>store-10</td><td>South</td><td>store</td><td>Beauty</td><td>sku-0121</td><td>cust-02960</td><td>1</td><td>4.99</td><td>0.2</td><td>card</td><td>COMPLETED</td><td>0</td><td>2026-08-20T23:31:56.304Z</td><td>synthetic_pos</td><td>2026-08-20</td><td>2025-01-01</td><td>4.99</td><td>3.99</td><td>1</td></tr><tr><td>evt-00029760</td><td>2025-01-01T15:44:00.000Z</td><td>store-09</td><td>North</td><td>store</td><td>Sports</td><td>sku-0001</td><td>cust-00920</td><td>1</td><td>85.99</td><td>0.2</td><td>card</td><td>COMPLETED</td><td>0</td><td>2026-08-20T23:31:56.304Z</td><td>synthetic_pos</td><td>2026-08-20</td><td>2025-01-01</td><td>85.99</td><td>68.79</td><td>1</td></tr><tr><td>evt-00029640</td><td>2025-01-01T15:46:00.000Z</td><td>store-08</td><td>West</td><td>store</td><td>Grocery</td><td>sku-0121</td><td>cust-03880</td><td>1</td><td>45.49</td><td>0.2</td><td>card</td><td>COMPLETED</td><td>0</td><td>2026-08-20T23:31:56.304Z</td><td>synthetic_pos</td><td>2026-08-20</td><td>2025-01-01</td><td>45.49</td><td>36.39</td><td>1</td></tr><tr><td>evt-00029520</td><td>2025-01-01T15:48:00.000Z</td><td>store-07</td><td>East</td><td>store</td><td>Electronics</td><td>sku-0001</td><td>cust-01840</td><td>1</td><td>4.99</td><td>0.2</td><td>card</td><td>COMPLETED</td><td>0</td><td>2026-08-20T23:31:56.304Z</td><td>synthetic_pos</td><td>2026-08-20</td><td>2025-01-01</td><td>4.99</td><td>3.99</td><td>1</td></tr><tr><td>evt-00029400</td><td>2025-01-01T15:50:00.000Z</td><td>store-06</td><td>South</td><td>store</td><td>Home</td><td>sku-0121</td><td>cust-04800</td><td>1</td><td>85.99</td><td>0.2</td><td>card</td><td>COMPLETED</td><td>0</td><td>2026-08-20T23:31:56.304Z</td><td>synthetic_pos</td><td>2026-08-20</td><td>2025-01-01</td><td>85.99</td><td>68.79</td><td>1</td></tr><tr><td>evt-00029280</td><td>2025-01-01T15:52:00.000Z</td><td>store-05</td><td>North</td><td>store</td><td>Apparel</td><td>sku-0001</td><td>cust-02760</td><td>1</td><td>45.49</td><td>0.2</td><td>card</td><td>COMPLETED</td><td>0</td><td>2026-08-20T23:31:56.304Z</td><td>synthetic_pos</td><td>2026-08-20</td><td>2025-01-01</td><td>45.49</td><td>36.39</td><td>1</td></tr><tr><td>evt-00029160</td><td>2025-01-01T15:54:00.000Z</td><td>store-04</td><td>West</td><td>store</td><td>Beauty</td><td>sku-0121</td><td>cust-00720</td><td>1</td><td>4.99</td><td>0.2</td><td>card</td><td>COMPLETED</td><td>0</td><td>2026-08-20T23:31:56.304Z</td><td>synthetic_pos</td><td>2026-08-20</td><td>2025-01-01</td><td>4.99</td><td>3.99</td><td>1</td></tr><tr><td>evt-00029040</td><td>2025-01-01T15:56:00.000Z</td><td>store-03</td><td>East</td><td>store</td><td>Grocery</td><td>sku-0001</td><td>cust-03680</td><td>1</td><td>85.99</td><td>0.2</td><td>card</td><td>COMPLETED</td><td>0</td><td>2026-08-20T23:31:56.304Z</td><td>synthetic_pos</td><td>2026-08-20</td><td>2025-01-01</td><td>85.99</td><td>68.79</td><td>1</td></tr><tr><td>evt-00028920</td><td>2025-01-01T15:58:00.000Z</td><td>store-02</td><td>South</td><td>store</td><td>Electronics</td><td>sku-0121</td><td>cust-01640</td><td>1</td><td>45.49</td><td>0.2</td><td>card</td><td>COMPLETED</td><td>0</td><td>2026-08-20T23:31:56.304Z</td><td>synthetic_pos</td><td>2026-08-20</td><td>2025-01-01</td><td>45.49</td><td>36.39</td><td>1</td></tr><tr><td>evt-00028800</td><td>2025-01-01T16:00:00.000Z</td><td>store-01</td><td>North</td><td>store</td><td>Home</td><td>sku-0001</td><td>cust-04600</td><td>1</td><td>4.99</td><td>0.2</td><td>card</td><td>COMPLETED</td><td>0</td><td>2026-08-20T23:31:56.304Z</td><td>synthetic_pos</td><td>2026-08-20</td><td>2025-01-01</td><td>4.99</td><td>3.99</td><td>1</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "evt-00029880",
         "2025-01-01T15:42:00.000Z",
         "store-10",
         "South",
         "store",
         "Beauty",
         "sku-0121",
         "cust-02960",
         1,
         4.99,
         0.2,
         "card",
         "COMPLETED",
         0,
         "2026-08-20T23:31:56.304Z",
         "synthetic_pos",
         "2026-08-20",
         "2025-01-01",
         4.99,
         3.99,
         1
        ],
        [
         "evt-00029760",
         "2025-01-01T15:44:00.000Z",
         "store-09",
         "North",
         "store",
         "Sports",
         "sku-0001",
         "cust-00920",
         1,
         85.99,
         0.2,
         "card",
         "COMPLETED",
         0,
         "2026-08-20T23:31:56.304Z",
         "synthetic_pos",
         "2026-08-20",
         "2025-01-01",
         85.99,
         68.79,
         1
        ],
        [
         "evt-00029640",
         "2025-01-01T15:46:00.000Z",
         "store-08",
         "West",
         "store",
         "Grocery",
         "sku-0121",
         "cust-03880",
         1,
         45.49,
         0.2,
         "card",
         "COMPLETED",
         0,
         "2026-08-20T23:31:56.304Z",
         "synthetic_pos",
         "2026-08-20",
         "2025-01-01",
         45.49,
         36.39,
         1
        ],
        [
         "evt-00029520",
         "2025-01-01T15:48:00.000Z",
         "store-07",
         "East",
         "store",
         "Electronics",
         "sku-0001",
         "cust-01840",
         1,
         4.99,
         0.2,
         "card",
         "COMPLETED",
         0,
         "2026-08-20T23:31:56.304Z",
         "synthetic_pos",
         "2026-08-20",
         "2025-01-01",
         4.99,
         3.99,
         1
        ],
        [
         "evt-00029400",
         "2025-01-01T15:50:00.000Z",
         "store-06",
         "South",
         "store",
         "Home",
         "sku-0121",
         "cust-04800",
         1,
         85.99,
         0.2,
         "card",
         "COMPLETED",
         0,
         "2026-08-20T23:31:56.304Z",
         "synthetic_pos",
         "2026-08-20",
         "2025-01-01",
         85.99,
         68.79,
         1
        ],
        [
         "evt-00029280",
         "2025-01-01T15:52:00.000Z",
         "store-05",
         "North",
         "store",
         "Apparel",
         "sku-0001",
         "cust-02760",
         1,
         45.49,
         0.2,
         "card",
         "COMPLETED",
         0,
         "2026-08-20T23:31:56.304Z",
         "synthetic_pos",
         "2026-08-20",
         "2025-01-01",
         45.49,
         36.39,
         1
        ],
        [
         "evt-00029160",
         "2025-01-01T15:54:00.000Z",
         "store-04",
         "West",
         "store",
         "Beauty",
         "sku-0121",
         "cust-00720",
         1,
         4.99,
         0.2,
         "card",
         "COMPLETED",
         0,
         "2026-08-20T23:31:56.304Z",
         "synthetic_pos",
         "2026-08-20",
         "2025-01-01",
         4.99,
         3.99,
         1
        ],
        [
         "evt-00029040",
         "2025-01-01T15:56:00.000Z",
         "store-03",
         "East",
         "store",
         "Grocery",
         "sku-0001",
         "cust-03680",
         1,
         85.99,
         0.2,
         "card",
         "COMPLETED",
         0,
         "2026-08-20T23:31:56.304Z",
         "synthetic_pos",
         "2026-08-20",
         "2025-01-01",
         85.99,
         68.79,
         1
        ],
        [
         "evt-00028920",
         "2025-01-01T15:58:00.000Z",
         "store-02",
         "South",
         "store",
         "Electronics",
         "sku-0121",
         "cust-01640",
         1,
         45.49,
         0.2,
         "card",
         "COMPLETED",
         0,
         "2026-08-20T23:31:56.304Z",
         "synthetic_pos",
         "2026-08-20",
         "2025-01-01",
         45.49,
         36.39,
         1
        ],
        [
         "evt-00028800",
         "2025-01-01T16:00:00.000Z",
         "store-01",
         "North",
         "store",
         "Home",
         "sku-0001",
         "cust-04600",
         1,
         4.99,
         0.2,
         "card",
         "COMPLETED",
         0,
         "2026-08-20T23:31:56.304Z",
         "synthetic_pos",
         "2026-08-20",
         "2025-01-01",
         4.99,
         3.99,
         1
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "event_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "event_ts",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "store_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "region",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "channel",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "category",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "product_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "quantity",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "unit_price",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "discount_pct",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "payment_type",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "order_status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "source_copy_sequence",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "ingested_at",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "source_system",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "ingestion_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "event_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "gross_revenue",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "net_revenue",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "signed_units",
         "type": "\"integer\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "dedupe_window = Window.partitionBy(\"event_id\").orderBy(\n",
    "    F.col(\"ingested_at\").desc(),\n",
    "    F.col(\"source_copy_sequence\").asc(),\n",
    ")\n",
    "\n",
    "silver = (\n",
    "    spark.table(table(\"bronze_retail_events\"))\n",
    "    .withColumn(\"dedupe_rank\", F.row_number().over(dedupe_window))\n",
    "    .where(F.col(\"dedupe_rank\") == 1)\n",
    "    .drop(\"dedupe_rank\")\n",
    "    .where(\n",
    "        (F.col(\"quantity\") > 0)\n",
    "        & (F.col(\"unit_price\") > 0)\n",
    "        & F.col(\"discount_pct\").between(0.0, 1.0)\n",
    "        & F.col(\"order_status\").isin(\"COMPLETED\", \"RETURNED\")\n",
    "    )\n",
    "    .withColumn(\"customer_id\", F.coalesce(\"customer_id\", F.lit(\"anonymous\")))\n",
    "    .withColumn(\"event_date\", F.to_date(\"event_ts\"))\n",
    "    .withColumn(\"gross_revenue\", F.round(F.col(\"quantity\") * F.col(\"unit_price\"), 2))\n",
    "    .withColumn(\n",
    "        \"net_revenue\",\n",
    "        F.round(\n",
    "            F.col(\"quantity\") * F.col(\"unit_price\") * (1 - F.col(\"discount_pct\"))\n",
    "            * F.when(F.col(\"order_status\") == \"RETURNED\", -1).otherwise(1),\n",
    "            2,\n",
    "        ),\n",
    "    )\n",
    "    .withColumn(\"signed_units\", F.col(\"quantity\") * F.when(F.col(\"order_status\") == \"RETURNED\", -1).otherwise(1))\n",
    ")\n",
    "\n",
    "(\n",
    "    silver.write\n",
    "    .format(\"delta\")\n",
    "    .mode(\"overwrite\")\n",
    "    .option(\"overwriteSchema\", \"true\")\n",
    "    .saveAsTable(table(\"silver_retail_events\"))\n",
    ")\n",
    "\n",
    "spark.sql(f\"COMMENT ON TABLE {namespace}.`silver_retail_events` IS 'Deduplicated, validated retail events enriched with signed units and revenue'\")\n",
    "display(spark.table(table(\"silver_retail_events\")).orderBy(\"event_ts\").limit(10))"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "9bc160c0-412c-480b-8a5f-b3cfa9f9a57e",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## 4. Gold: dashboard-ready retail metrics\n",
    "\n",
    "These managed tables provide daily executive KPIs, category trends, and store/channel drilldowns."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1787268978523,
     "inputWidgets": {},
     "nuid": "94f75772-5634-4e8a-a2b1-9e7e62f6fdc6",
     "showTitle": false,
     "startTime": 1787268960339,
     "submitTime": 1787268960277,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>event_date</th><th>category</th><th>transactions</th><th>net_units</th><th>net_revenue</th><th>avg_discount_pct</th><th>return_rate</th></tr></thead><tbody><tr><td>2025-04-30</td><td>Grocery</td><td>45</td><td>172</td><td>16357.52</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-04-30</td><td>Apparel</td><td>46</td><td>176</td><td>16248.16</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-04-30</td><td>Home</td><td>46</td><td>176</td><td>16248.16</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-04-30</td><td>Electronics</td><td>45</td><td>164</td><td>16090.24</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-30</td><td>Beauty</td><td>44</td><td>160</td><td>15227.6</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-04-30</td><td>Sports</td><td>23</td><td>84</td><td>7423.44</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-29</td><td>Home</td><td>46</td><td>132</td><td>12483.36</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-04-29</td><td>Apparel</td><td>45</td><td>123</td><td>11770.29</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-29</td><td>Electronics</td><td>46</td><td>132</td><td>11754.36</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-04-29</td><td>Grocery</td><td>45</td><td>123</td><td>11405.79</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-29</td><td>Beauty</td><td>44</td><td>120</td><td>11249.1</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-04-29</td><td>Sports</td><td>23</td><td>63</td><td>5598.99</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-28</td><td>Electronics</td><td>46</td><td>88</td><td>8034.4</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-04-28</td><td>Grocery</td><td>46</td><td>84</td><td>7669.2</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-28</td><td>Apparel</td><td>45</td><td>86</td><td>7527.8</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-04-28</td><td>Beauty</td><td>44</td><td>80</td><td>7466.0</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-04-28</td><td>Home</td><td>45</td><td>82</td><td>7405.6</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-28</td><td>Sports</td><td>23</td><td>42</td><td>4077.6</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-27</td><td>Home</td><td>45</td><td>215</td><td>17390.06</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-04-27</td><td>Apparel</td><td>44</td><td>210</td><td>17167.89</td><td>0.1</td><td>0.0227</td></tr><tr><td>2025-04-27</td><td>Grocery</td><td>46</td><td>210</td><td>16985.64</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-04-27</td><td>Electronics</td><td>45</td><td>205</td><td>16763.47</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-04-27</td><td>Beauty</td><td>46</td><td>210</td><td>16621.14</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-04-27</td><td>Sports</td><td>23</td><td>105</td><td>8310.57</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-04-26</td><td>Beauty</td><td>45</td><td>172</td><td>15211.68</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-04-26</td><td>Home</td><td>45</td><td>172</td><td>15049.68</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-04-26</td><td>Grocery</td><td>45</td><td>164</td><td>14990.16</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-26</td><td>Apparel</td><td>46</td><td>168</td><td>14857.92</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-26</td><td>Electronics</td><td>44</td><td>168</td><td>14371.92</td><td>0.0</td><td>0.0227</td></tr><tr><td>2025-04-26</td><td>Sports</td><td>23</td><td>84</td><td>7428.96</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-25</td><td>Electronics</td><td>45</td><td>86</td><td>7725.86</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-04-25</td><td>Home</td><td>46</td><td>84</td><td>7470.84</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-25</td><td>Apparel</td><td>45</td><td>86</td><td>7401.86</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-04-25</td><td>Beauty</td><td>46</td><td>84</td><td>7227.84</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-25</td><td>Grocery</td><td>45</td><td>82</td><td>6972.82</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-25</td><td>Sports</td><td>22</td><td>40</td><td>3318.4</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-04-24</td><td>Apparel</td><td>46</td><td>176</td><td>15548.08</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-04-24</td><td>Home</td><td>46</td><td>176</td><td>14576.08</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-04-24</td><td>Beauty</td><td>45</td><td>164</td><td>14197.12</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-24</td><td>Electronics</td><td>46</td><td>168</td><td>14053.44</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-24</td><td>Grocery</td><td>44</td><td>160</td><td>13692.8</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-04-24</td><td>Sports</td><td>22</td><td>80</td><td>7008.4</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-04-23</td><td>Home</td><td>46</td><td>132</td><td>10106.44</td><td>0.1</td><td>0.0217</td></tr><tr><td>2025-04-23</td><td>Electronics</td><td>46</td><td>126</td><td>9652.03</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-04-23</td><td>Grocery</td><td>46</td><td>126</td><td>9542.69</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-04-23</td><td>Beauty</td><td>45</td><td>129</td><td>9441.82</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-04-23</td><td>Apparel</td><td>44</td><td>120</td><td>8869.56</td><td>0.1</td><td>0.0455</td></tr><tr><td>2025-04-23</td><td>Sports</td><td>22</td><td>60</td><td>4872.2</td><td>0.1</td><td>0.0455</td></tr><tr><td>2025-04-22</td><td>Beauty</td><td>45</td><td>86</td><td>7275.92</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-04-22</td><td>Electronics</td><td>46</td><td>88</td><td>7198.36</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-04-22</td><td>Apparel</td><td>45</td><td>86</td><td>7194.92</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-04-22</td><td>Grocery</td><td>46</td><td>84</td><td>6705.48</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-22</td><td>Home</td><td>45</td><td>82</td><td>6702.04</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-22</td><td>Sports</td><td>22</td><td>40</td><td>3227.8</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-04-21</td><td>Grocery</td><td>46</td><td>168</td><td>11314.26</td><td>0.2</td><td>0.0435</td></tr><tr><td>2025-04-21</td><td>Home</td><td>45</td><td>172</td><td>11055.99</td><td>0.2</td><td>0.0222</td></tr><tr><td>2025-04-21</td><td>Electronics</td><td>45</td><td>164</td><td>10794.93</td><td>0.2</td><td>0.0444</td></tr><tr><td>2025-04-21</td><td>Beauty</td><td>46</td><td>168</td><td>10666.26</td><td>0.2</td><td>0.0435</td></tr><tr><td>2025-04-21</td><td>Apparel</td><td>43</td><td>164</td><td>10406.13</td><td>0.2</td><td>0.0233</td></tr><tr><td>2025-04-21</td><td>Sports</td><td>23</td><td>84</td><td>5203.53</td><td>0.2</td><td>0.0435</td></tr><tr><td>2025-04-20</td><td>Home</td><td>44</td><td>168</td><td>14064.48</td><td>0.0</td><td>0.0227</td></tr><tr><td>2025-04-20</td><td>Beauty</td><td>46</td><td>176</td><td>14055.36</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-04-20</td><td>Apparel</td><td>46</td><td>168</td><td>13416.48</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-20</td><td>Electronics</td><td>45</td><td>164</td><td>12935.04</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-20</td><td>Grocery</td><td>45</td><td>164</td><td>12611.04</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-20</td><td>Sports</td><td>23</td><td>84</td><td>6546.24</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-19</td><td>Beauty</td><td>45</td><td>129</td><td>9433.73</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-04-19</td><td>Apparel</td><td>46</td><td>132</td><td>9098.74</td><td>0.1</td><td>0.0217</td></tr><tr><td>2025-04-19</td><td>Grocery</td><td>45</td><td>123</td><td>8791.51</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-04-19</td><td>Electronics</td><td>45</td><td>123</td><td>8682.16</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-04-19</td><td>Home</td><td>45</td><td>123</td><td>8354.11</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-04-19</td><td>Sports</td><td>23</td><td>63</td><td>4556.31</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-04-18</td><td>Home</td><td>45</td><td>172</td><td>13730.0</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-04-18</td><td>Apparel</td><td>45</td><td>172</td><td>13406.0</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-04-18</td><td>Grocery</td><td>45</td><td>164</td><td>12952.0</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-18</td><td>Electronics</td><td>46</td><td>168</td><td>12612.0</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-18</td><td>Beauty</td><td>45</td><td>164</td><td>11980.0</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-18</td><td>Sports</td><td>23</td><td>92</td><td>7084.0</td><td>0.0</td><td>0.0</td></tr><tr><td>2025-04-17</td><td>Beauty</td><td>45</td><td>129</td><td>10113.03</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-04-17</td><td>Home</td><td>45</td><td>129</td><td>9627.03</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-04-17</td><td>Grocery</td><td>46</td><td>126</td><td>9521.82</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-17</td><td>Electronics</td><td>46</td><td>126</td><td>9278.82</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-17</td><td>Apparel</td><td>45</td><td>123</td><td>9052.11</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-17</td><td>Sports</td><td>23</td><td>63</td><td>4882.41</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-16</td><td>Apparel</td><td>45</td><td>86</td><td>6457.04</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-04-16</td><td>Electronics</td><td>45</td><td>82</td><td>6322.48</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-16</td><td>Grocery</td><td>46</td><td>84</td><td>6227.76</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-16</td><td>Beauty</td><td>45</td><td>86</td><td>6133.04</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-04-16</td><td>Home</td><td>44</td><td>80</td><td>5850.2</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-04-16</td><td>Sports</td><td>23</td><td>42</td><td>2870.88</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-15</td><td>Apparel</td><td>46</td><td>176</td><td>11809.04</td><td>0.1</td><td>0.0217</td></tr><tr><td>2025-04-15</td><td>Beauty</td><td>46</td><td>168</td><td>11139.72</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-04-15</td><td>Home</td><td>45</td><td>164</td><td>10877.96</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-04-15</td><td>Grocery</td><td>45</td><td>164</td><td>10586.36</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-04-15</td><td>Electronics</td><td>44</td><td>160</td><td>9887.2</td><td>0.1</td><td>0.0455</td></tr><tr><td>2025-04-15</td><td>Sports</td><td>23</td><td>92</td><td>6020.48</td><td>0.1</td><td>0.0</td></tr><tr><td>2025-04-14</td><td>Home</td><td>46</td><td>132</td><td>9408.96</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-04-14</td><td>Beauty</td><td>46</td><td>132</td><td>9287.46</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-04-14</td><td>Grocery</td><td>45</td><td>129</td><td>9195.12</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-04-14</td><td>Electronics</td><td>45</td><td>123</td><td>9131.94</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-14</td><td>Apparel</td><td>45</td><td>123</td><td>8524.44</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-14</td><td>Sports</td><td>22</td><td>60</td><td>4519.8</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-04-13</td><td>Apparel</td><td>46</td><td>132</td><td>9584.7</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-04-13</td><td>Grocery</td><td>45</td><td>129</td><td>8889.15</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-04-13</td><td>Beauty</td><td>45</td><td>129</td><td>8889.15</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-04-13</td><td>Electronics</td><td>45</td><td>123</td><td>8713.05</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-13</td><td>Home</td><td>46</td><td>126</td><td>8679.6</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-13</td><td>Sports</td><td>22</td><td>60</td><td>4069.5</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-04-12</td><td>Grocery</td><td>46</td><td>210</td><td>14773.2</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-12</td><td>Electronics</td><td>46</td><td>210</td><td>14570.7</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-12</td><td>Home</td><td>46</td><td>210</td><td>14165.7</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-12</td><td>Apparel</td><td>45</td><td>215</td><td>14102.8</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-04-12</td><td>Beauty</td><td>45</td><td>205</td><td>13823.6</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-12</td><td>Sports</td><td>22</td><td>110</td><td>7728.7</td><td>0.0</td><td>0.0</td></tr><tr><td>2025-04-11</td><td>Beauty</td><td>45</td><td>129</td><td>7107.94</td><td>0.2</td><td>0.0222</td></tr><tr><td>2025-04-11</td><td>Home</td><td>45</td><td>123</td><td>6689.18</td><td>0.2</td><td>0.0444</td></tr><tr><td>2025-04-11</td><td>Apparel</td><td>45</td><td>123</td><td>6689.18</td><td>0.2</td><td>0.0444</td></tr><tr><td>2025-04-11</td><td>Electronics</td><td>45</td><td>123</td><td>6591.98</td><td>0.2</td><td>0.0444</td></tr><tr><td>2025-04-11</td><td>Grocery</td><td>45</td><td>129</td><td>6524.74</td><td>0.2</td><td>0.0222</td></tr><tr><td>2025-04-11</td><td>Sports</td><td>23</td><td>63</td><td>3084.78</td><td>0.2</td><td>0.0435</td></tr><tr><td>2025-04-10</td><td>Beauty</td><td>46</td><td>88</td><td>5769.28</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-04-10</td><td>Grocery</td><td>45</td><td>86</td><td>5719.16</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-04-10</td><td>Apparel</td><td>45</td><td>86</td><td>5638.16</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-04-10</td><td>Electronics</td><td>45</td><td>82</td><td>5132.92</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-10</td><td>Home</td><td>45</td><td>82</td><td>5132.92</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-10</td><td>Sports</td><td>23</td><td>42</td><td>2915.52</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-09</td><td>Electronics</td><td>45</td><td>172</td><td>11192.36</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-04-09</td><td>Home</td><td>45</td><td>164</td><td>11165.32</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-09</td><td>Apparel</td><td>46</td><td>176</td><td>10962.88</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-04-09</td><td>Beauty</td><td>46</td><td>168</td><td>10449.84</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-09</td><td>Grocery</td><td>45</td><td>164</td><td>10355.32</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-09</td><td>Sports</td><td>22</td><td>88</td><td>5967.44</td><td>0.0</td><td>0.0</td></tr><tr><td>2025-04-08</td><td>Beauty</td><td>45</td><td>129</td><td>8452.8</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-04-08</td><td>Grocery</td><td>44</td><td>126</td><td>7900.2</td><td>0.0</td><td>0.0227</td></tr><tr><td>2025-04-08</td><td>Apparel</td><td>46</td><td>126</td><td>7900.2</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-08</td><td>Electronics</td><td>45</td><td>123</td><td>7712.1</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-08</td><td>Home</td><td>46</td><td>126</td><td>7657.2</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-08</td><td>Sports</td><td>23</td><td>63</td><td>3828.6</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-07</td><td>Grocery</td><td>45</td><td>86</td><td>5034.07</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-04-07</td><td>Home</td><td>46</td><td>84</td><td>4777.98</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-04-07</td><td>Electronics</td><td>46</td><td>84</td><td>4632.18</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-04-07</td><td>Beauty</td><td>45</td><td>86</td><td>4450.87</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-04-07</td><td>Apparel</td><td>45</td><td>82</td><td>4448.99</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-04-07</td><td>Sports</td><td>23</td><td>42</td><td>2170.29</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-04-06</td><td>Beauty</td><td>45</td><td>205</td><td>12874.7</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-06</td><td>Apparel</td><td>45</td><td>215</td><td>12865.6</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-04-06</td><td>Electronics</td><td>46</td><td>220</td><td>12759.8</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-04-06</td><td>Home</td><td>44</td><td>200</td><td>11968.0</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-04-06</td><td>Grocery</td><td>45</td><td>205</td><td>11254.7</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-06</td><td>Sports</td><td>23</td><td>115</td><td>7084.1</td><td>0.0</td><td>0.0</td></tr><tr><td>2025-04-05</td><td>Grocery</td><td>46</td><td>176</td><td>10604.16</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-04-05</td><td>Electronics</td><td>45</td><td>172</td><td>10208.52</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-04-05</td><td>Beauty</td><td>46</td><td>176</td><td>9956.16</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-04-05</td><td>Apparel</td><td>45</td><td>164</td><td>9417.24</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-05</td><td>Home</td><td>45</td><td>164</td><td>8931.24</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-05</td><td>Sports</td><td>23</td><td>84</td><td>5068.44</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-04</td><td>Apparel</td><td>46</td><td>84</td><td>4948.32</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-04</td><td>Beauty</td><td>46</td><td>88</td><td>4933.24</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-04-04</td><td>Home</td><td>45</td><td>82</td><td>4753.36</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-04-04</td><td>Grocery</td><td>44</td><td>84</td><td>4705.32</td><td>0.0</td><td>0.0227</td></tr><tr><td>2025-04-04</td><td>Electronics</td><td>44</td><td>80</td><td>4477.4</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-04-04</td><td>Sports</td><td>23</td><td>42</td><td>2393.16</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-03</td><td>Electronics</td><td>45</td><td>172</td><td>8744.94</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-04-03</td><td>Beauty</td><td>45</td><td>164</td><td>8344.98</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-04-03</td><td>Apparel</td><td>46</td><td>168</td><td>8253.36</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-04-03</td><td>Grocery</td><td>45</td><td>164</td><td>8199.18</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-04-03</td><td>Home</td><td>46</td><td>168</td><td>8107.56</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-04-03</td><td>Sports</td><td>21</td><td>84</td><td>4345.38</td><td>0.1</td><td>0.0</td></tr><tr><td>2025-04-02</td><td>Electronics</td><td>46</td><td>132</td><td>7386.84</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-04-02</td><td>Apparel</td><td>46</td><td>126</td><td>7183.62</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-02</td><td>Home</td><td>46</td><td>126</td><td>6940.62</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-04-02</td><td>Grocery</td><td>45</td><td>129</td><td>6859.98</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-04-02</td><td>Beauty</td><td>45</td><td>129</td><td>6859.98</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-04-02</td><td>Sports</td><td>22</td><td>60</td><td>2882.7</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-04-01</td><td>Grocery</td><td>46</td><td>88</td><td>3709.2</td><td>0.2</td><td>0.0217</td></tr><tr><td>2025-04-01</td><td>Beauty</td><td>45</td><td>86</td><td>3689.7</td><td>0.2</td><td>0.0222</td></tr><tr><td>2025-04-01</td><td>Home</td><td>46</td><td>88</td><td>3644.4</td><td>0.2</td><td>0.0217</td></tr><tr><td>2025-04-01</td><td>Electronics</td><td>46</td><td>84</td><td>3346.2</td><td>0.2</td><td>0.0435</td></tr><tr><td>2025-04-01</td><td>Apparel</td><td>44</td><td>80</td><td>3242.4</td><td>0.2</td><td>0.0455</td></tr><tr><td>2025-04-01</td><td>Sports</td><td>21</td><td>38</td><td>1731.3</td><td>0.2</td><td>0.0476</td></tr><tr><td>2025-03-31</td><td>Electronics</td><td>46</td><td>176</td><td>15825.76</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-03-31</td><td>Apparel</td><td>45</td><td>164</td><td>15696.64</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-31</td><td>Grocery</td><td>46</td><td>168</td><td>15253.68</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-31</td><td>Beauty</td><td>45</td><td>164</td><td>15210.64</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-31</td><td>Home</td><td>45</td><td>164</td><td>14886.64</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-31</td><td>Sports</td><td>23</td><td>92</td><td>8441.92</td><td>0.0</td><td>0.0</td></tr><tr><td>2025-03-30</td><td>Grocery</td><td>46</td><td>176</td><td>14599.96</td><td>0.1</td><td>0.0217</td></tr><tr><td>2025-03-30</td><td>Electronics</td><td>45</td><td>172</td><td>13837.37</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-03-30</td><td>Beauty</td><td>46</td><td>168</td><td>13657.98</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-03-30</td><td>Apparel</td><td>45</td><td>164</td><td>12895.39</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-03-30</td><td>Home</td><td>44</td><td>160</td><td>12861.8</td><td>0.1</td><td>0.0455</td></tr><tr><td>2025-03-30</td><td>Sports</td><td>22</td><td>80</td><td>7087.0</td><td>0.1</td><td>0.0455</td></tr><tr><td>2025-03-29</td><td>Home</td><td>45</td><td>129</td><td>11589.6</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-29</td><td>Apparel</td><td>46</td><td>126</td><td>11444.4</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-29</td><td>Electronics</td><td>45</td><td>123</td><td>11177.7</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-29</td><td>Grocery</td><td>44</td><td>126</td><td>10958.4</td><td>0.0</td><td>0.0227</td></tr><tr><td>2025-03-29</td><td>Beauty</td><td>45</td><td>123</td><td>10813.2</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-29</td><td>Sports</td><td>23</td><td>63</td><td>5479.2</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-28</td><td>Home</td><td>46</td><td>176</td><td>15070.72</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-03-28</td><td>Apparel</td><td>46</td><td>168</td><td>15018.96</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-28</td><td>Grocery</td><td>45</td><td>164</td><td>14993.08</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-28</td><td>Electronics</td><td>45</td><td>172</td><td>14882.84</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-28</td><td>Beauty</td><td>45</td><td>164</td><td>14021.08</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-28</td><td>Sports</td><td>23</td><td>92</td><td>8047.24</td><td>0.0</td><td>0.0</td></tr><tr><td>2025-03-27</td><td>Electronics</td><td>46</td><td>132</td><td>11600.28</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-03-27</td><td>Home</td><td>46</td><td>126</td><td>11327.04</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-27</td><td>Grocery</td><td>45</td><td>129</td><td>10734.66</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-27</td><td>Apparel</td><td>45</td><td>123</td><td>10582.92</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-27</td><td>Beauty</td><td>44</td><td>120</td><td>10203.3</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-03-27</td><td>Sports</td><td>22</td><td>60</td><td>5040.9</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-03-26</td><td>Grocery</td><td>46</td><td>88</td><td>6701.2</td><td>0.1</td><td>0.0217</td></tr><tr><td>2025-03-26</td><td>Beauty</td><td>45</td><td>82</td><td>6463.0</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-03-26</td><td>Electronics</td><td>46</td><td>84</td><td>6396.6</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-03-26</td><td>Apparel</td><td>45</td><td>82</td><td>6317.2</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-03-26</td><td>Home</td><td>45</td><td>86</td><td>6257.3</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-03-26</td><td>Sports</td><td>23</td><td>42</td><td>3271.2</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-03-25</td><td>Home</td><td>45</td><td>172</td><td>14468.96</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-25</td><td>Electronics</td><td>45</td><td>172</td><td>14306.96</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-25</td><td>Grocery</td><td>46</td><td>168</td><td>14298.24</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-25</td><td>Apparel</td><td>45</td><td>164</td><td>13479.52</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-25</td><td>Beauty</td><td>45</td><td>164</td><td>13317.52</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-25</td><td>Sports</td><td>23</td><td>84</td><td>7311.12</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-24</td><td>Beauty</td><td>46</td><td>126</td><td>10665.0</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-24</td><td>Apparel</td><td>45</td><td>129</td><td>10545.75</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-24</td><td>Electronics</td><td>45</td><td>129</td><td>10424.25</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-24</td><td>Grocery</td><td>45</td><td>129</td><td>10302.75</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-24</td><td>Home</td><td>44</td><td>120</td><td>10053.0</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-03-24</td><td>Sports</td><td>23</td><td>63</td><td>5271.75</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-23</td><td>Grocery</td><td>45</td><td>129</td><td>10725.78</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-23</td><td>Home</td><td>46</td><td>132</td><td>10480.74</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-03-23</td><td>Electronics</td><td>45</td><td>123</td><td>10243.86</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-23</td><td>Beauty</td><td>46</td><td>126</td><td>10120.32</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-23</td><td>Apparel</td><td>46</td><td>126</td><td>9998.82</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-23</td><td>Sports</td><td>22</td><td>60</td><td>4454.7</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-03-22</td><td>Home</td><td>46</td><td>220</td><td>14370.64</td><td>0.2</td><td>0.0217</td></tr><tr><td>2025-03-22</td><td>Apparel</td><td>45</td><td>205</td><td>13261.96</td><td>0.2</td><td>0.0444</td></tr><tr><td>2025-03-22</td><td>Beauty</td><td>45</td><td>205</td><td>13099.96</td><td>0.2</td><td>0.0444</td></tr><tr><td>2025-03-22</td><td>Electronics</td><td>45</td><td>215</td><td>13083.08</td><td>0.2</td><td>0.0222</td></tr><tr><td>2025-03-22</td><td>Grocery</td><td>45</td><td>205</td><td>12775.96</td><td>0.2</td><td>0.0444</td></tr><tr><td>2025-03-22</td><td>Sports</td><td>22</td><td>100</td><td>6149.2</td><td>0.2</td><td>0.0455</td></tr><tr><td>2025-03-21</td><td>Electronics</td><td>46</td><td>88</td><td>6897.48</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-03-21</td><td>Grocery</td><td>46</td><td>84</td><td>6587.64</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-21</td><td>Beauty</td><td>45</td><td>82</td><td>6513.72</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-21</td><td>Apparel</td><td>45</td><td>86</td><td>6418.56</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-21</td><td>Home</td><td>46</td><td>84</td><td>6344.64</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-21</td><td>Sports</td><td>22</td><td>40</td><td>3260.4</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-03-20</td><td>Apparel</td><td>44</td><td>42</td><td>3314.76</td><td>0.0</td><td>0.0227</td></tr><tr><td>2025-03-20</td><td>Home</td><td>45</td><td>43</td><td>3309.79</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-20</td><td>Grocery</td><td>46</td><td>44</td><td>3304.82</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-03-20</td><td>Electronics</td><td>46</td><td>42</td><td>3193.26</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-20</td><td>Beauty</td><td>45</td><td>41</td><td>2995.73</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-20</td><td>Sports</td><td>23</td><td>21</td><td>1596.63</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-19</td><td>Grocery</td><td>46</td><td>126</td><td>9642.6</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-19</td><td>Electronics</td><td>45</td><td>129</td><td>9501.9</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-19</td><td>Beauty</td><td>46</td><td>126</td><td>9399.6</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-19</td><td>Home</td><td>44</td><td>126</td><td>9278.1</td><td>0.0</td><td>0.0227</td></tr><tr><td>2025-03-19</td><td>Apparel</td><td>44</td><td>120</td><td>8952.0</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-03-19</td><td>Sports</td><td>23</td><td>63</td><td>4821.3</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-18</td><td>Electronics</td><td>45</td><td>86</td><td>5955.13</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-03-18</td><td>Apparel</td><td>46</td><td>88</td><td>5795.24</td><td>0.1</td><td>0.0217</td></tr><tr><td>2025-03-18</td><td>Beauty</td><td>46</td><td>84</td><td>5531.82</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-03-18</td><td>Home</td><td>45</td><td>82</td><td>5473.01</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-03-18</td><td>Grocery</td><td>45</td><td>82</td><td>5327.21</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-03-18</td><td>Sports</td><td>23</td><td>42</td><td>2547.21</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-03-17</td><td>Apparel</td><td>46</td><td>44</td><td>3318.56</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-03-17</td><td>Grocery</td><td>45</td><td>41</td><td>2941.34</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-17</td><td>Beauty</td><td>45</td><td>41</td><td>2941.34</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-17</td><td>Home</td><td>44</td><td>42</td><td>2932.08</td><td>0.0</td><td>0.0227</td></tr><tr><td>2025-03-17</td><td>Electronics</td><td>45</td><td>41</td><td>2819.84</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-17</td><td>Sports</td><td>23</td><td>21</td><td>1587.54</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-16</td><td>Electronics</td><td>46</td><td>176</td><td>12698.56</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-03-16</td><td>Home</td><td>46</td><td>176</td><td>12698.56</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-03-16</td><td>Beauty</td><td>45</td><td>172</td><td>11931.32</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-16</td><td>Grocery</td><td>45</td><td>164</td><td>11368.84</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-16</td><td>Apparel</td><td>45</td><td>164</td><td>10882.84</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-16</td><td>Sports</td><td>23</td><td>84</td><td>6392.04</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-15</td><td>Apparel</td><td>45</td><td>129</td><td>9250.02</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-15</td><td>Electronics</td><td>46</td><td>132</td><td>9092.16</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-03-15</td><td>Beauty</td><td>45</td><td>129</td><td>9007.02</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-15</td><td>Grocery</td><td>46</td><td>126</td><td>8435.88</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-15</td><td>Home</td><td>44</td><td>120</td><td>8144.1</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-03-15</td><td>Sports</td><td>23</td><td>63</td><td>4217.94</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-14</td><td>Grocery</td><td>46</td><td>42</td><td>2695.62</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-03-14</td><td>Home</td><td>45</td><td>43</td><td>2646.98</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-03-14</td><td>Apparel</td><td>45</td><td>43</td><td>2574.08</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-03-14</td><td>Beauty</td><td>46</td><td>42</td><td>2513.37</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-03-14</td><td>Electronics</td><td>45</td><td>41</td><td>2489.11</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-03-14</td><td>Sports</td><td>22</td><td>20</td><td>1177.75</td><td>0.1</td><td>0.0455</td></tr><tr><td>2025-03-13</td><td>Beauty</td><td>46</td><td>132</td><td>8836.14</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-03-13</td><td>Home</td><td>44</td><td>126</td><td>8561.52</td><td>0.0</td><td>0.0227</td></tr><tr><td>2025-03-13</td><td>Apparel</td><td>46</td><td>126</td><td>8440.02</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-13</td><td>Electronics</td><td>45</td><td>123</td><td>8241.96</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-13</td><td>Grocery</td><td>45</td><td>123</td><td>7634.46</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-13</td><td>Sports</td><td>23</td><td>63</td><td>4037.76</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-12</td><td>Beauty</td><td>45</td><td>86</td><td>4573.22</td><td>0.2</td><td>0.0222</td></tr><tr><td>2025-03-12</td><td>Apparel</td><td>46</td><td>88</td><td>4482.16</td><td>0.2</td><td>0.0217</td></tr><tr><td>2025-03-12</td><td>Grocery</td><td>45</td><td>82</td><td>4431.34</td><td>0.2</td><td>0.0444</td></tr><tr><td>2025-03-12</td><td>Electronics</td><td>44</td><td>84</td><td>4405.08</td><td>0.2</td><td>0.0227</td></tr><tr><td>2025-03-12</td><td>Home</td><td>46</td><td>84</td><td>4145.88</td><td>0.2</td><td>0.0435</td></tr><tr><td>2025-03-12</td><td>Sports</td><td>22</td><td>40</td><td>2131.6</td><td>0.2</td><td>0.0455</td></tr><tr><td>2025-03-11</td><td>Home</td><td>45</td><td>43</td><td>2796.88</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-11</td><td>Apparel</td><td>46</td><td>44</td><td>2779.04</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-03-11</td><td>Grocery</td><td>45</td><td>41</td><td>2630.06</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-11</td><td>Electronics</td><td>46</td><td>42</td><td>2612.22</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-11</td><td>Beauty</td><td>45</td><td>41</td><td>2468.06</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-11</td><td>Sports</td><td>22</td><td>22</td><td>1430.02</td><td>0.0</td><td>0.0</td></tr><tr><td>2025-03-10</td><td>Beauty</td><td>45</td><td>129</td><td>7604.21</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-03-10</td><td>Home</td><td>46</td><td>132</td><td>7114.78</td><td>0.1</td><td>0.0217</td></tr><tr><td>2025-03-10</td><td>Grocery</td><td>46</td><td>126</td><td>7109.49</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-03-10</td><td>Electronics</td><td>45</td><td>123</td><td>6833.47</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-03-10</td><td>Apparel</td><td>45</td><td>123</td><td>6724.12</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-03-10</td><td>Sports</td><td>22</td><td>60</td><td>3333.4</td><td>0.1</td><td>0.0455</td></tr><tr><td>2025-03-09</td><td>Apparel</td><td>45</td><td>129</td><td>8021.7</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-09</td><td>Electronics</td><td>45</td><td>129</td><td>7900.2</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-09</td><td>Grocery</td><td>46</td><td>126</td><td>7719.3</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-09</td><td>Home</td><td>45</td><td>123</td><td>7538.4</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-09</td><td>Beauty</td><td>45</td><td>129</td><td>7414.2</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-09</td><td>Sports</td><td>23</td><td>63</td><td>3555.9</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-08</td><td>Apparel</td><td>45</td><td>86</td><td>5143.82</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-08</td><td>Beauty</td><td>46</td><td>84</td><td>5107.08</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-08</td><td>Home</td><td>45</td><td>86</td><td>5062.82</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-08</td><td>Grocery</td><td>46</td><td>84</td><td>4783.08</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-08</td><td>Electronics</td><td>44</td><td>80</td><td>4385.6</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-03-08</td><td>Sports</td><td>23</td><td>46</td><td>2789.02</td><td>0.0</td><td>0.0</td></tr><tr><td>2025-03-07</td><td>Beauty</td><td>46</td><td>132</td><td>7582.08</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-03-07</td><td>Electronics</td><td>45</td><td>123</td><td>7551.12</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-07</td><td>Home</td><td>45</td><td>129</td><td>7288.26</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-07</td><td>Grocery</td><td>43</td><td>123</td><td>7186.62</td><td>0.0</td><td>0.0233</td></tr><tr><td>2025-03-07</td><td>Apparel</td><td>46</td><td>126</td><td>6994.44</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-07</td><td>Sports</td><td>23</td><td>63</td><td>3740.22</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-06</td><td>Apparel</td><td>45</td><td>86</td><td>4553.96</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-03-06</td><td>Home</td><td>46</td><td>84</td><td>4234.44</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-03-06</td><td>Beauty</td><td>45</td><td>86</td><td>4189.46</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-03-06</td><td>Grocery</td><td>45</td><td>82</td><td>4133.62</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-03-06</td><td>Electronics</td><td>45</td><td>82</td><td>4060.72</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-03-06</td><td>Sports</td><td>23</td><td>42</td><td>2117.22</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-03-05</td><td>Electronics</td><td>46</td><td>42</td><td>2373.36</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-05</td><td>Home</td><td>46</td><td>42</td><td>2292.36</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-05</td><td>Beauty</td><td>45</td><td>41</td><td>2278.28</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-05</td><td>Grocery</td><td>44</td><td>40</td><td>2264.2</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-03-05</td><td>Apparel</td><td>45</td><td>43</td><td>2184.94</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-05</td><td>Sports</td><td>23</td><td>23</td><td>1295.84</td><td>0.0</td><td>0.0</td></tr><tr><td>2025-03-04</td><td>Beauty</td><td>45</td><td>129</td><td>6977.85</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-04</td><td>Electronics</td><td>46</td><td>126</td><td>6939.9</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-04</td><td>Apparel</td><td>45</td><td>123</td><td>6901.95</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-04</td><td>Home</td><td>44</td><td>126</td><td>6818.4</td><td>0.0</td><td>0.0227</td></tr><tr><td>2025-03-04</td><td>Grocery</td><td>46</td><td>132</td><td>6772.8</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-03-04</td><td>Sports</td><td>23</td><td>63</td><td>2983.95</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-03</td><td>Beauty</td><td>46</td><td>88</td><td>4632.36</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-03-03</td><td>Grocery</td><td>45</td><td>86</td><td>4528.92</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-03</td><td>Apparel</td><td>45</td><td>86</td><td>4366.92</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-03</td><td>Home</td><td>45</td><td>82</td><td>4160.04</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-03</td><td>Electronics</td><td>45</td><td>82</td><td>3917.04</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-03</td><td>Sports</td><td>23</td><td>42</td><td>2334.24</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-02</td><td>Home</td><td>45</td><td>82</td><td>3493.26</td><td>0.2</td><td>0.0444</td></tr><tr><td>2025-03-02</td><td>Apparel</td><td>46</td><td>88</td><td>3475.44</td><td>0.2</td><td>0.0217</td></tr><tr><td>2025-03-02</td><td>Electronics</td><td>45</td><td>82</td><td>3363.66</td><td>0.2</td><td>0.0444</td></tr><tr><td>2025-03-02</td><td>Beauty</td><td>45</td><td>82</td><td>3234.06</td><td>0.2</td><td>0.0444</td></tr><tr><td>2025-03-02</td><td>Grocery</td><td>45</td><td>82</td><td>3169.26</td><td>0.2</td><td>0.0444</td></tr><tr><td>2025-03-02</td><td>Sports</td><td>23</td><td>46</td><td>1915.38</td><td>0.2</td><td>0.0</td></tr><tr><td>2025-03-01</td><td>Grocery</td><td>45</td><td>172</td><td>15855.92</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-03-01</td><td>Beauty</td><td>46</td><td>176</td><td>15241.36</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-03-01</td><td>Home</td><td>46</td><td>168</td><td>14850.48</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-01</td><td>Apparel</td><td>46</td><td>168</td><td>14688.48</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-03-01</td><td>Electronics</td><td>45</td><td>164</td><td>14655.04</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-03-01</td><td>Sports</td><td>21</td><td>76</td><td>7277.36</td><td>0.0</td><td>0.0476</td></tr><tr><td>2025-02-28</td><td>Apparel</td><td>46</td><td>84</td><td>7467.12</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-28</td><td>Electronics</td><td>46</td><td>84</td><td>7467.12</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-28</td><td>Beauty</td><td>44</td><td>84</td><td>7467.12</td><td>0.0</td><td>0.0227</td></tr><tr><td>2025-02-28</td><td>Home</td><td>46</td><td>84</td><td>7386.12</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-28</td><td>Grocery</td><td>45</td><td>86</td><td>7237.98</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-02-28</td><td>Sports</td><td>22</td><td>40</td><td>3517.2</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-02-27</td><td>Electronics</td><td>46</td><td>44</td><td>3806.0</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-02-27</td><td>Grocery</td><td>46</td><td>42</td><td>3754.5</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-27</td><td>Apparel</td><td>45</td><td>43</td><td>3719.5</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-02-27</td><td>Home</td><td>46</td><td>42</td><td>3511.5</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-27</td><td>Beauty</td><td>44</td><td>40</td><td>3379.0</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-02-27</td><td>Sports</td><td>22</td><td>22</td><td>1862.5</td><td>0.0</td><td>0.0</td></tr><tr><td>2025-02-26</td><td>Grocery</td><td>46</td><td>132</td><td>9887.66</td><td>0.1</td><td>0.0217</td></tr><tr><td>2025-02-26</td><td>Home</td><td>45</td><td>123</td><td>9854.69</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-02-26</td><td>Electronics</td><td>46</td><td>126</td><td>9537.63</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-02-26</td><td>Beauty</td><td>44</td><td>126</td><td>9537.63</td><td>0.1</td><td>0.0227</td></tr><tr><td>2025-02-26</td><td>Apparel</td><td>45</td><td>123</td><td>9526.64</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-02-26</td><td>Sports</td><td>23</td><td>63</td><td>4714.14</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-02-25</td><td>Beauty</td><td>46</td><td>88</td><td>7684.32</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-02-25</td><td>Grocery</td><td>46</td><td>88</td><td>7279.32</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-02-25</td><td>Electronics</td><td>45</td><td>82</td><td>6858.48</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-25</td><td>Apparel</td><td>44</td><td>80</td><td>6691.2</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-02-25</td><td>Home</td><td>45</td><td>82</td><td>6615.48</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-25</td><td>Sports</td><td>23</td><td>42</td><td>3593.88</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-24</td><td>Electronics</td><td>44</td><td>42</td><td>3493.32</td><td>0.0</td><td>0.0227</td></tr><tr><td>2025-02-24</td><td>Apparel</td><td>46</td><td>42</td><td>3452.82</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-24</td><td>Grocery</td><td>45</td><td>41</td><td>3451.61</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-24</td><td>Home</td><td>45</td><td>41</td><td>3411.11</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-24</td><td>Beauty</td><td>46</td><td>42</td><td>3290.82</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-24</td><td>Sports</td><td>23</td><td>23</td><td>1890.83</td><td>0.0</td><td>0.0</td></tr><tr><td>2025-02-23</td><td>Beauty</td><td>45</td><td>172</td><td>14380.16</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-02-23</td><td>Home</td><td>46</td><td>168</td><td>13895.04</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-23</td><td>Grocery</td><td>45</td><td>172</td><td>13732.16</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-02-23</td><td>Electronics</td><td>45</td><td>172</td><td>13408.16</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-02-23</td><td>Apparel</td><td>45</td><td>164</td><td>12761.92</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-23</td><td>Sports</td><td>23</td><td>84</td><td>6947.52</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-22</td><td>Electronics</td><td>46</td><td>126</td><td>9435.6</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-02-22</td><td>Beauty</td><td>45</td><td>129</td><td>9103.11</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-02-22</td><td>Home</td><td>46</td><td>126</td><td>8998.22</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-02-22</td><td>Grocery</td><td>44</td><td>126</td><td>8998.22</td><td>0.1</td><td>0.0227</td></tr><tr><td>2025-02-22</td><td>Apparel</td><td>45</td><td>123</td><td>8674.64</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-02-22</td><td>Sports</td><td>23</td><td>63</td><td>4171.07</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-02-21</td><td>Grocery</td><td>46</td><td>42</td><td>3272.64</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-21</td><td>Electronics</td><td>46</td><td>44</td><td>3266.48</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-02-21</td><td>Apparel</td><td>44</td><td>40</td><td>3238.3</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-02-21</td><td>Home</td><td>45</td><td>41</td><td>3235.22</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-21</td><td>Beauty</td><td>45</td><td>41</td><td>3194.72</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-21</td><td>Sports</td><td>23</td><td>23</td><td>1792.16</td><td>0.0</td><td>0.0</td></tr><tr><td>2025-02-20</td><td>Grocery</td><td>46</td><td>132</td><td>8077.52</td><td>0.2</td><td>0.0217</td></tr><tr><td>2025-02-20</td><td>Electronics</td><td>45</td><td>129</td><td>7991.14</td><td>0.2</td><td>0.0222</td></tr><tr><td>2025-02-20</td><td>Beauty</td><td>46</td><td>126</td><td>7807.56</td><td>0.2</td><td>0.0435</td></tr><tr><td>2025-02-20</td><td>Home</td><td>45</td><td>123</td><td>7429.58</td><td>0.2</td><td>0.0444</td></tr><tr><td>2025-02-20</td><td>Apparel</td><td>45</td><td>123</td><td>7137.98</td><td>0.2</td><td>0.0444</td></tr><tr><td>2025-02-20</td><td>Sports</td><td>23</td><td>63</td><td>4146.78</td><td>0.2</td><td>0.0435</td></tr><tr><td>2025-02-19</td><td>Beauty</td><td>46</td><td>88</td><td>6605.28</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-02-19</td><td>Apparel</td><td>46</td><td>84</td><td>6548.04</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-19</td><td>Grocery</td><td>45</td><td>86</td><td>6374.16</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-02-19</td><td>Home</td><td>44</td><td>84</td><td>6305.04</td><td>0.0</td><td>0.0227</td></tr><tr><td>2025-02-19</td><td>Electronics</td><td>45</td><td>82</td><td>6235.92</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-19</td><td>Sports</td><td>23</td><td>42</td><td>3071.52</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-18</td><td>Grocery</td><td>45</td><td>41</td><td>2826.42</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-02-18</td><td>Apparel</td><td>46</td><td>42</td><td>2783.34</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-02-18</td><td>Electronics</td><td>45</td><td>43</td><td>2776.71</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-02-18</td><td>Beauty</td><td>46</td><td>42</td><td>2746.89</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-02-18</td><td>Home</td><td>45</td><td>41</td><td>2644.17</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-02-18</td><td>Sports</td><td>22</td><td>22</td><td>1494.39</td><td>0.1</td><td>0.0</td></tr><tr><td>2025-02-17</td><td>Electronics</td><td>45</td><td>129</td><td>9556.8</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-02-17</td><td>Home</td><td>46</td><td>126</td><td>9340.2</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-17</td><td>Apparel</td><td>46</td><td>126</td><td>9218.7</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-17</td><td>Beauty</td><td>45</td><td>123</td><td>8880.6</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-17</td><td>Grocery</td><td>45</td><td>129</td><td>8827.8</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-02-17</td><td>Sports</td><td>22</td><td>60</td><td>4089.0</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-02-16</td><td>Grocery</td><td>46</td><td>132</td><td>9463.14</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-02-16</td><td>Apparel</td><td>45</td><td>123</td><td>8947.71</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-16</td><td>Beauty</td><td>45</td><td>123</td><td>8826.21</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-16</td><td>Home</td><td>45</td><td>129</td><td>8764.83</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-02-16</td><td>Electronics</td><td>46</td><td>126</td><td>8674.02</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-16</td><td>Sports</td><td>21</td><td>57</td><td>4276.89</td><td>0.0</td><td>0.0476</td></tr><tr><td>2025-02-15</td><td>Electronics</td><td>46</td><td>88</td><td>6182.92</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-02-15</td><td>Home</td><td>45</td><td>86</td><td>6125.24</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-02-15</td><td>Grocery</td><td>46</td><td>84</td><td>5824.56</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-15</td><td>Apparel</td><td>45</td><td>82</td><td>5442.88</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-15</td><td>Beauty</td><td>45</td><td>82</td><td>5442.88</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-15</td><td>Sports</td><td>23</td><td>46</td><td>3270.64</td><td>0.0</td><td>0.0</td></tr><tr><td>2025-02-14</td><td>Apparel</td><td>45</td><td>129</td><td>7993.83</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-02-14</td><td>Grocery</td><td>46</td><td>132</td><td>7958.49</td><td>0.1</td><td>0.0217</td></tr><tr><td>2025-02-14</td><td>Beauty</td><td>45</td><td>123</td><td>7955.16</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-02-14</td><td>Electronics</td><td>45</td><td>129</td><td>7884.48</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-02-14</td><td>Home</td><td>44</td><td>120</td><td>7334.4</td><td>0.1</td><td>0.0455</td></tr><tr><td>2025-02-14</td><td>Sports</td><td>23</td><td>63</td><td>3741.21</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-02-13</td><td>Grocery</td><td>45</td><td>86</td><td>6041.28</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-02-13</td><td>Home</td><td>45</td><td>86</td><td>5636.28</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-02-13</td><td>Apparel</td><td>46</td><td>84</td><td>5584.32</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-13</td><td>Beauty</td><td>46</td><td>84</td><td>5422.32</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-13</td><td>Electronics</td><td>44</td><td>80</td><td>5318.4</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-02-13</td><td>Sports</td><td>23</td><td>42</td><td>2630.16</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-12</td><td>Home</td><td>46</td><td>44</td><td>3024.2</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-02-12</td><td>Beauty</td><td>45</td><td>41</td><td>2788.55</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-12</td><td>Electronics</td><td>45</td><td>43</td><td>2675.65</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-02-12</td><td>Apparel</td><td>45</td><td>41</td><td>2626.55</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-12</td><td>Grocery</td><td>45</td><td>41</td><td>2545.55</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-12</td><td>Sports</td><td>23</td><td>21</td><td>1325.55</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-11</td><td>Electronics</td><td>44</td><td>126</td><td>8380.62</td><td>0.0</td><td>0.0227</td></tr><tr><td>2025-02-11</td><td>Grocery</td><td>45</td><td>129</td><td>8328.48</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-02-11</td><td>Apparel</td><td>45</td><td>129</td><td>8085.48</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-02-11</td><td>Beauty</td><td>45</td><td>123</td><td>7946.76</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-11</td><td>Home</td><td>46</td><td>126</td><td>7530.12</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-11</td><td>Sports</td><td>23</td><td>63</td><td>4129.56</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-10</td><td>Home</td><td>45</td><td>86</td><td>4472.9</td><td>0.2</td><td>0.0222</td></tr><tr><td>2025-02-10</td><td>Grocery</td><td>46</td><td>88</td><td>4378.0</td><td>0.2</td><td>0.0217</td></tr><tr><td>2025-02-10</td><td>Apparel</td><td>45</td><td>86</td><td>4343.3</td><td>0.2</td><td>0.0222</td></tr><tr><td>2025-02-10</td><td>Electronics</td><td>46</td><td>84</td><td>4049.4</td><td>0.2</td><td>0.0435</td></tr><tr><td>2025-02-10</td><td>Beauty</td><td>45</td><td>82</td><td>3820.3</td><td>0.2</td><td>0.0444</td></tr><tr><td>2025-02-10</td><td>Sports</td><td>23</td><td>42</td><td>2089.5</td><td>0.2</td><td>0.0435</td></tr><tr><td>2025-02-09</td><td>Electronics</td><td>44</td><td>84</td><td>5265.84</td><td>0.0</td><td>0.0227</td></tr><tr><td>2025-02-09</td><td>Beauty</td><td>46</td><td>84</td><td>5184.84</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-09</td><td>Home</td><td>45</td><td>86</td><td>5144.36</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-02-09</td><td>Grocery</td><td>45</td><td>82</td><td>4982.32</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-09</td><td>Apparel</td><td>45</td><td>82</td><td>4820.32</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-09</td><td>Sports</td><td>23</td><td>42</td><td>2632.92</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-08</td><td>Electronics</td><td>45</td><td>172</td><td>10690.76</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-02-08</td><td>Apparel</td><td>46</td><td>176</td><td>10604.08</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-02-08</td><td>Grocery</td><td>44</td><td>168</td><td>10291.44</td><td>0.0</td><td>0.0227</td></tr><tr><td>2025-02-08</td><td>Home</td><td>45</td><td>164</td><td>9730.12</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-08</td><td>Beauty</td><td>46</td><td>168</td><td>9643.44</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-08</td><td>Sports</td><td>23</td><td>84</td><td>4497.72</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-07</td><td>Apparel</td><td>46</td><td>88</td><td>5257.2</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-02-07</td><td>Grocery</td><td>45</td><td>86</td><td>5060.4</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-02-07</td><td>Home</td><td>46</td><td>88</td><td>5014.2</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-02-07</td><td>Beauty</td><td>45</td><td>82</td><td>4747.8</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-07</td><td>Electronics</td><td>45</td><td>82</td><td>4423.8</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-07</td><td>Sports</td><td>22</td><td>40</td><td>2559.0</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-02-06</td><td>Electronics</td><td>46</td><td>44</td><td>2345.43</td><td>0.1</td><td>0.0217</td></tr><tr><td>2025-02-06</td><td>Home</td><td>45</td><td>43</td><td>2221.71</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-02-06</td><td>Beauty</td><td>45</td><td>43</td><td>2185.26</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-02-06</td><td>Apparel</td><td>46</td><td>42</td><td>2025.09</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-02-06</td><td>Grocery</td><td>44</td><td>40</td><td>1959.9</td><td>0.1</td><td>0.0455</td></tr><tr><td>2025-02-06</td><td>Sports</td><td>22</td><td>20</td><td>1089.3</td><td>0.1</td><td>0.0455</td></tr><tr><td>2025-02-05</td><td>Apparel</td><td>45</td><td>129</td><td>7586.16</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-02-05</td><td>Electronics</td><td>46</td><td>132</td><td>7022.28</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-02-05</td><td>Beauty</td><td>45</td><td>123</td><td>6891.42</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-05</td><td>Home</td><td>46</td><td>126</td><td>6813.54</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-05</td><td>Grocery</td><td>46</td><td>126</td><td>6813.54</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-05</td><td>Sports</td><td>22</td><td>60</td><td>3180.9</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-02-04</td><td>Home</td><td>45</td><td>86</td><td>4772.46</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-02-04</td><td>Grocery</td><td>45</td><td>82</td><td>4639.02</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-04</td><td>Electronics</td><td>45</td><td>82</td><td>4477.02</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-04</td><td>Apparel</td><td>45</td><td>86</td><td>4367.46</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-02-04</td><td>Beauty</td><td>45</td><td>82</td><td>4315.02</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-04</td><td>Sports</td><td>23</td><td>42</td><td>2170.62</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-03</td><td>Beauty</td><td>46</td><td>44</td><td>2376.92</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-02-03</td><td>Home</td><td>45</td><td>43</td><td>2284.24</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-02-03</td><td>Electronics</td><td>45</td><td>43</td><td>2243.74</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-02-03</td><td>Apparel</td><td>45</td><td>41</td><td>2179.88</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-03</td><td>Grocery</td><td>46</td><td>42</td><td>2070.06</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-02-03</td><td>Sports</td><td>22</td><td>20</td><td>881.6</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-02-02</td><td>Grocery</td><td>45</td><td>164</td><td>8073.9</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-02-02</td><td>Apparel</td><td>46</td><td>176</td><td>8038.8</td><td>0.1</td><td>0.0217</td></tr><tr><td>2025-02-02</td><td>Beauty</td><td>46</td><td>176</td><td>8038.8</td><td>0.1</td><td>0.0217</td></tr><tr><td>2025-02-02</td><td>Electronics</td><td>45</td><td>172</td><td>7710.3</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-02-02</td><td>Home</td><td>45</td><td>164</td><td>7053.3</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-02-02</td><td>Sports</td><td>23</td><td>84</td><td>4128.3</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-02-01</td><td>Home</td><td>46</td><td>132</td><td>6996.24</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-02-01</td><td>Apparel</td><td>46</td><td>132</td><td>6267.24</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-02-01</td><td>Grocery</td><td>45</td><td>123</td><td>5944.86</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-01</td><td>Electronics</td><td>45</td><td>123</td><td>5944.86</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-02-01</td><td>Beauty</td><td>44</td><td>120</td><td>5918.4</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-02-01</td><td>Sports</td><td>22</td><td>60</td><td>3080.7</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-01-31</td><td>Beauty</td><td>45</td><td>43</td><td>1744.53</td><td>0.2</td><td>0.0222</td></tr><tr><td>2025-01-31</td><td>Electronics</td><td>46</td><td>42</td><td>1609.02</td><td>0.2</td><td>0.0435</td></tr><tr><td>2025-01-31</td><td>Apparel</td><td>45</td><td>41</td><td>1603.11</td><td>0.2</td><td>0.0444</td></tr><tr><td>2025-01-31</td><td>Grocery</td><td>45</td><td>41</td><td>1603.11</td><td>0.2</td><td>0.0444</td></tr><tr><td>2025-01-31</td><td>Home</td><td>46</td><td>44</td><td>1556.04</td><td>0.2</td><td>0.0217</td></tr><tr><td>2025-01-31</td><td>Sports</td><td>23</td><td>21</td><td>772.11</td><td>0.2</td><td>0.0435</td></tr><tr><td>2025-01-30</td><td>Beauty</td><td>45</td><td>129</td><td>11339.34</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-01-30</td><td>Apparel</td><td>44</td><td>126</td><td>11078.46</td><td>0.0</td><td>0.0227</td></tr><tr><td>2025-01-30</td><td>Electronics</td><td>46</td><td>126</td><td>10956.96</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-30</td><td>Home</td><td>45</td><td>123</td><td>10817.58</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-30</td><td>Grocery</td><td>46</td><td>126</td><td>10470.96</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-30</td><td>Sports</td><td>22</td><td>60</td><td>5217.6</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-01-29</td><td>Grocery</td><td>46</td><td>84</td><td>6611.7</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-01-29</td><td>Apparel</td><td>45</td><td>86</td><td>6546.95</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-01-29</td><td>Electronics</td><td>45</td><td>82</td><td>6530.65</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-01-29</td><td>Home</td><td>45</td><td>86</td><td>6474.05</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-01-29</td><td>Beauty</td><td>45</td><td>82</td><td>6311.95</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-01-29</td><td>Sports</td><td>23</td><td>46</td><td>3467.95</td><td>0.1</td><td>0.0</td></tr><tr><td>2025-01-28</td><td>Home</td><td>45</td><td>43</td><td>3737.8</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-01-28</td><td>Beauty</td><td>46</td><td>44</td><td>3659.9</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-01-28</td><td>Apparel</td><td>46</td><td>42</td><td>3613.2</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-28</td><td>Grocery</td><td>45</td><td>41</td><td>3407.6</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-28</td><td>Electronics</td><td>45</td><td>41</td><td>3367.1</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-28</td><td>Sports</td><td>23</td><td>21</td><td>1685.1</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-27</td><td>Beauty</td><td>45</td><td>129</td><td>11150.43</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-01-27</td><td>Apparel</td><td>46</td><td>132</td><td>10547.94</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-01-27</td><td>Home</td><td>46</td><td>126</td><td>10173.42</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-27</td><td>Electronics</td><td>45</td><td>123</td><td>10046.91</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-27</td><td>Grocery</td><td>44</td><td>120</td><td>9798.9</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-01-27</td><td>Sports</td><td>22</td><td>60</td><td>5203.2</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-01-26</td><td>Apparel</td><td>46</td><td>132</td><td>10845.18</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-01-26</td><td>Home</td><td>46</td><td>126</td><td>10357.74</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-26</td><td>Electronics</td><td>46</td><td>126</td><td>10236.24</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-26</td><td>Grocery</td><td>45</td><td>123</td><td>10114.02</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-26</td><td>Beauty</td><td>45</td><td>123</td><td>9628.02</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-26</td><td>Sports</td><td>22</td><td>66</td><td>5240.34</td><td>0.0</td><td>0.0</td></tr><tr><td>2025-01-25</td><td>Grocery</td><td>46</td><td>88</td><td>6321.04</td><td>0.1</td><td>0.0217</td></tr><tr><td>2025-01-25</td><td>Home</td><td>46</td><td>88</td><td>6248.14</td><td>0.1</td><td>0.0217</td></tr><tr><td>2025-01-25</td><td>Beauty</td><td>43</td><td>82</td><td>5962.96</td><td>0.1</td><td>0.0233</td></tr><tr><td>2025-01-25</td><td>Apparel</td><td>45</td><td>82</td><td>5817.16</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-01-25</td><td>Electronics</td><td>46</td><td>84</td><td>5815.02</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-01-25</td><td>Sports</td><td>22</td><td>40</td><td>2946.1</td><td>0.1</td><td>0.0455</td></tr><tr><td>2025-01-24</td><td>Electronics</td><td>46</td><td>126</td><td>10240.38</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-24</td><td>Beauty</td><td>45</td><td>129</td><td>10111.02</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-01-24</td><td>Apparel</td><td>45</td><td>129</td><td>9989.52</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-01-24</td><td>Grocery</td><td>46</td><td>126</td><td>9754.38</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-24</td><td>Home</td><td>44</td><td>120</td><td>9527.1</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-01-24</td><td>Sports</td><td>23</td><td>63</td><td>4573.44</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-23</td><td>Apparel</td><td>45</td><td>86</td><td>6941.7</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-01-23</td><td>Grocery</td><td>46</td><td>84</td><td>6382.8</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-23</td><td>Beauty</td><td>45</td><td>82</td><td>6309.9</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-23</td><td>Home</td><td>45</td><td>82</td><td>6228.9</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-23</td><td>Electronics</td><td>45</td><td>82</td><td>6066.9</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-23</td><td>Sports</td><td>23</td><td>46</td><td>3539.7</td><td>0.0</td><td>0.0</td></tr><tr><td>2025-01-22</td><td>Grocery</td><td>45</td><td>43</td><td>3287.86</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-01-22</td><td>Beauty</td><td>46</td><td>44</td><td>3241.88</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-01-22</td><td>Home</td><td>45</td><td>41</td><td>3096.32</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-22</td><td>Electronics</td><td>44</td><td>40</td><td>3020.8</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-01-22</td><td>Apparel</td><td>45</td><td>41</td><td>2974.82</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-22</td><td>Sports</td><td>23</td><td>21</td><td>1707.42</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-21</td><td>Apparel</td><td>46</td><td>132</td><td>8018.48</td><td>0.2</td><td>0.0217</td></tr><tr><td>2025-01-21</td><td>Beauty</td><td>45</td><td>129</td><td>7743.46</td><td>0.2</td><td>0.0222</td></tr><tr><td>2025-01-21</td><td>Electronics</td><td>45</td><td>123</td><td>7485.02</td><td>0.2</td><td>0.0444</td></tr><tr><td>2025-01-21</td><td>Grocery</td><td>45</td><td>129</td><td>7354.66</td><td>0.2</td><td>0.0222</td></tr><tr><td>2025-01-21</td><td>Home</td><td>46</td><td>126</td><td>7274.04</td><td>0.2</td><td>0.0435</td></tr><tr><td>2025-01-21</td><td>Sports</td><td>23</td><td>63</td><td>3637.02</td><td>0.2</td><td>0.0435</td></tr><tr><td>2025-01-20</td><td>Apparel</td><td>44</td><td>88</td><td>6394.08</td><td>0.0</td><td>0.0</td></tr><tr><td>2025-01-20</td><td>Grocery</td><td>44</td><td>80</td><td>6217.8</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-01-20</td><td>Electronics</td><td>46</td><td>84</td><td>6103.44</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-20</td><td>Home</td><td>46</td><td>84</td><td>5941.44</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-20</td><td>Beauty</td><td>45</td><td>82</td><td>5796.12</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-20</td><td>Sports</td><td>23</td><td>46</td><td>3342.36</td><td>0.0</td><td>0.0</td></tr><tr><td>2025-01-19</td><td>Home</td><td>45</td><td>82</td><td>6083.86</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-19</td><td>Beauty</td><td>44</td><td>84</td><td>6064.32</td><td>0.0</td><td>0.0227</td></tr><tr><td>2025-01-19</td><td>Electronics</td><td>46</td><td>84</td><td>5983.32</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-19</td><td>Grocery</td><td>46</td><td>88</td><td>5944.24</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-01-19</td><td>Apparel</td><td>45</td><td>82</td><td>5840.86</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-19</td><td>Sports</td><td>23</td><td>42</td><td>2910.66</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-18</td><td>Beauty</td><td>46</td><td>176</td><td>12608.8</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-01-18</td><td>Grocery</td><td>46</td><td>176</td><td>12284.8</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-01-18</td><td>Apparel</td><td>44</td><td>160</td><td>11330.0</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-01-18</td><td>Electronics</td><td>45</td><td>164</td><td>11285.2</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-18</td><td>Home</td><td>45</td><td>164</td><td>10799.2</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-18</td><td>Sports</td><td>23</td><td>84</td><td>6025.2</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-17</td><td>Electronics</td><td>45</td><td>86</td><td>5292.01</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-01-17</td><td>Apparel</td><td>45</td><td>86</td><td>5219.11</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-01-17</td><td>Home</td><td>44</td><td>80</td><td>5214.4</td><td>0.1</td><td>0.0455</td></tr><tr><td>2025-01-17</td><td>Grocery</td><td>45</td><td>82</td><td>5118.77</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-01-17</td><td>Beauty</td><td>46</td><td>84</td><td>4950.24</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-01-17</td><td>Sports</td><td>23</td><td>46</td><td>2830.61</td><td>0.1</td><td>0.0</td></tr><tr><td>2025-01-16</td><td>Beauty</td><td>46</td><td>44</td><td>3066.86</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-01-16</td><td>Electronics</td><td>45</td><td>43</td><td>2837.92</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-01-16</td><td>Home</td><td>46</td><td>42</td><td>2811.48</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-16</td><td>Grocery</td><td>45</td><td>43</td><td>2797.42</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-01-16</td><td>Apparel</td><td>46</td><td>42</td><td>2770.98</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-16</td><td>Sports</td><td>22</td><td>20</td><td>1338.8</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-01-15</td><td>Grocery</td><td>45</td><td>129</td><td>8815.29</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-01-15</td><td>Electronics</td><td>46</td><td>126</td><td>8497.26</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-15</td><td>Apparel</td><td>46</td><td>126</td><td>8132.76</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-15</td><td>Home</td><td>44</td><td>126</td><td>8132.76</td><td>0.0</td><td>0.0227</td></tr><tr><td>2025-01-15</td><td>Beauty</td><td>45</td><td>129</td><td>8086.29</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-01-15</td><td>Sports</td><td>22</td><td>60</td><td>3566.1</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-01-14</td><td>Home</td><td>46</td><td>84</td><td>5463.72</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-14</td><td>Electronics</td><td>46</td><td>88</td><td>5396.04</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-01-14</td><td>Apparel</td><td>45</td><td>82</td><td>5335.56</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-14</td><td>Beauty</td><td>45</td><td>82</td><td>5335.56</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-14</td><td>Grocery</td><td>45</td><td>82</td><td>5092.56</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-14</td><td>Sports</td><td>22</td><td>44</td><td>2900.52</td><td>0.0</td><td>0.0</td></tr><tr><td>2025-01-13</td><td>Electronics</td><td>46</td><td>44</td><td>2517.61</td><td>0.1</td><td>0.0217</td></tr><tr><td>2025-01-13</td><td>Grocery</td><td>46</td><td>44</td><td>2517.61</td><td>0.1</td><td>0.0217</td></tr><tr><td>2025-01-13</td><td>Beauty</td><td>45</td><td>43</td><td>2424.77</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-01-13</td><td>Apparel</td><td>45</td><td>41</td><td>2202.64</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-01-13</td><td>Home</td><td>44</td><td>40</td><td>2146.25</td><td>0.1</td><td>0.0455</td></tr><tr><td>2025-01-13</td><td>Sports</td><td>23</td><td>21</td><td>1220.64</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-01-12</td><td>Home</td><td>45</td><td>172</td><td>10691.84</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-01-12</td><td>Apparel</td><td>45</td><td>164</td><td>10688.08</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-12</td><td>Grocery</td><td>46</td><td>176</td><td>10612.72</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-01-12</td><td>Beauty</td><td>45</td><td>172</td><td>10043.84</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-01-12</td><td>Electronics</td><td>44</td><td>160</td><td>9795.2</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-01-12</td><td>Sports</td><td>23</td><td>84</td><td>5142.48</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-11</td><td>Electronics</td><td>45</td><td>129</td><td>6073.3</td><td>0.2</td><td>0.0222</td></tr><tr><td>2025-01-11</td><td>Beauty</td><td>46</td><td>126</td><td>6027.0</td><td>0.2</td><td>0.0435</td></tr><tr><td>2025-01-11</td><td>Grocery</td><td>45</td><td>123</td><td>5980.7</td><td>0.2</td><td>0.0444</td></tr><tr><td>2025-01-11</td><td>Home</td><td>45</td><td>123</td><td>5883.5</td><td>0.2</td><td>0.0444</td></tr><tr><td>2025-01-11</td><td>Apparel</td><td>46</td><td>126</td><td>5832.6</td><td>0.2</td><td>0.0435</td></tr><tr><td>2025-01-11</td><td>Sports</td><td>23</td><td>69</td><td>3397.7</td><td>0.2</td><td>0.0</td></tr><tr><td>2025-01-10</td><td>Electronics</td><td>44</td><td>42</td><td>2572.62</td><td>0.0</td><td>0.0227</td></tr><tr><td>2025-01-10</td><td>Apparel</td><td>46</td><td>42</td><td>2532.12</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-10</td><td>Home</td><td>46</td><td>42</td><td>2451.12</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-10</td><td>Grocery</td><td>45</td><td>43</td><td>2387.98</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-01-10</td><td>Beauty</td><td>45</td><td>41</td><td>2352.26</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-10</td><td>Sports</td><td>23</td><td>21</td><td>1104.06</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-09</td><td>Grocery</td><td>45</td><td>129</td><td>6828.23</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-01-09</td><td>Beauty</td><td>45</td><td>129</td><td>6609.53</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-01-09</td><td>Home</td><td>46</td><td>132</td><td>6544.54</td><td>0.1</td><td>0.0217</td></tr><tr><td>2025-01-09</td><td>Apparel</td><td>45</td><td>123</td><td>6411.46</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-01-09</td><td>Electronics</td><td>45</td><td>123</td><td>5864.71</td><td>0.1</td><td>0.0444</td></tr><tr><td>2025-01-09</td><td>Sports</td><td>23</td><td>63</td><td>3446.61</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-01-08</td><td>Electronics</td><td>45</td><td>86</td><td>4935.0</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-01-08</td><td>Home</td><td>45</td><td>86</td><td>4854.0</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-01-08</td><td>Grocery</td><td>46</td><td>84</td><td>4500.0</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-08</td><td>Beauty</td><td>45</td><td>82</td><td>4470.0</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-08</td><td>Apparel</td><td>45</td><td>82</td><td>4227.0</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-08</td><td>Sports</td><td>23</td><td>46</td><td>2634.0</td><td>0.0</td><td>0.0</td></tr><tr><td>2025-01-07</td><td>Beauty</td><td>46</td><td>42</td><td>2351.94</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-07</td><td>Electronics</td><td>45</td><td>43</td><td>2284.51</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-01-07</td><td>Grocery</td><td>44</td><td>42</td><td>2270.94</td><td>0.0</td><td>0.0227</td></tr><tr><td>2025-01-07</td><td>Apparel</td><td>45</td><td>41</td><td>2257.37</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-07</td><td>Home</td><td>45</td><td>41</td><td>2135.87</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-07</td><td>Sports</td><td>23</td><td>21</td><td>1094.97</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-06</td><td>Grocery</td><td>45</td><td>129</td><td>7155.06</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-01-06</td><td>Home</td><td>45</td><td>129</td><td>6912.06</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-01-06</td><td>Beauty</td><td>46</td><td>126</td><td>6511.14</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-06</td><td>Electronics</td><td>45</td><td>123</td><td>6474.72</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-06</td><td>Apparel</td><td>46</td><td>126</td><td>6389.64</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-06</td><td>Sports</td><td>23</td><td>63</td><td>3073.32</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-05</td><td>Home</td><td>46</td><td>132</td><td>6302.58</td><td>0.1</td><td>0.0217</td></tr><tr><td>2025-01-05</td><td>Beauty</td><td>46</td><td>126</td><td>6135.39</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-01-05</td><td>Apparel</td><td>46</td><td>126</td><td>5916.69</td><td>0.1</td><td>0.0435</td></tr><tr><td>2025-01-05</td><td>Electronics</td><td>45</td><td>129</td><td>5836.26</td><td>0.1</td><td>0.0222</td></tr><tr><td>2025-01-05</td><td>Grocery</td><td>44</td><td>120</td><td>4984.05</td><td>0.1</td><td>0.0455</td></tr><tr><td>2025-01-05</td><td>Sports</td><td>22</td><td>66</td><td>3041.94</td><td>0.1</td><td>0.0</td></tr><tr><td>2025-01-04</td><td>Grocery</td><td>45</td><td>86</td><td>4524.08</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-01-04</td><td>Electronics</td><td>46</td><td>88</td><td>4380.64</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-01-04</td><td>Apparel</td><td>45</td><td>86</td><td>4200.08</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-01-04</td><td>Beauty</td><td>45</td><td>82</td><td>4000.96</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-04</td><td>Home</td><td>46</td><td>84</td><td>3938.52</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-04</td><td>Sports</td><td>22</td><td>40</td><td>2072.2</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-01-03</td><td>Home</td><td>46</td><td>132</td><td>6746.7</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-01-03</td><td>Apparel</td><td>45</td><td>123</td><td>6068.55</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-03</td><td>Grocery</td><td>45</td><td>129</td><td>5994.15</td><td>0.0</td><td>0.0222</td></tr><tr><td>2025-01-03</td><td>Electronics</td><td>46</td><td>126</td><td>5970.6</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-03</td><td>Beauty</td><td>45</td><td>123</td><td>5582.55</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-03</td><td>Sports</td><td>22</td><td>60</td><td>3022.5</td><td>0.0</td><td>0.0455</td></tr><tr><td>2025-01-02</td><td>Electronics</td><td>46</td><td>88</td><td>4209.96</td><td>0.0</td><td>0.0217</td></tr><tr><td>2025-01-02</td><td>Grocery</td><td>46</td><td>84</td><td>4022.28</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-02</td><td>Beauty</td><td>45</td><td>82</td><td>4009.44</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-02</td><td>Apparel</td><td>45</td><td>82</td><td>3766.44</td><td>0.0</td><td>0.0444</td></tr><tr><td>2025-01-02</td><td>Home</td><td>44</td><td>84</td><td>3617.28</td><td>0.0</td><td>0.0227</td></tr><tr><td>2025-01-02</td><td>Sports</td><td>23</td><td>42</td><td>1889.64</td><td>0.0</td><td>0.0435</td></tr><tr><td>2025-01-01</td><td>Apparel</td><td>45</td><td>43</td><td>1629.57</td><td>0.2</td><td>0.0222</td></tr><tr><td>2025-01-01</td><td>Electronics</td><td>45</td><td>43</td><td>1597.17</td><td>0.2</td><td>0.0222</td></tr><tr><td>2025-01-01</td><td>Grocery</td><td>45</td><td>43</td><td>1564.77</td><td>0.2</td><td>0.0222</td></tr><tr><td>2025-01-01</td><td>Home</td><td>45</td><td>41</td><td>1524.39</td><td>0.2</td><td>0.0444</td></tr><tr><td>2025-01-01</td><td>Beauty</td><td>46</td><td>42</td><td>1398.78</td><td>0.2</td><td>0.0435</td></tr><tr><td>2025-01-01</td><td>Sports</td><td>23</td><td>21</td><td>699.39</td><td>0.2</td><td>0.0435</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "2025-04-30",
         "Grocery",
         45,
         172,
         16357.52,
         0.0,
         0.0222
        ],
        [
         "2025-04-30",
         "Apparel",
         46,
         176,
         16248.16,
         0.0,
         0.0217
        ],
        [
         "2025-04-30",
         "Home",
         46,
         176,
         16248.16,
         0.0,
         0.0217
        ],
        [
         "2025-04-30",
         "Electronics",
         45,
         164,
         16090.24,
         0.0,
         0.0444
        ],
        [
         "2025-04-30",
         "Beauty",
         44,
         160,
         15227.6,
         0.0,
         0.0455
        ],
        [
         "2025-04-30",
         "Sports",
         23,
         84,
         7423.44,
         0.0,
         0.0435
        ],
        [
         "2025-04-29",
         "Home",
         46,
         132,
         12483.36,
         0.0,
         0.0217
        ],
        [
         "2025-04-29",
         "Apparel",
         45,
         123,
         11770.29,
         0.0,
         0.0444
        ],
        [
         "2025-04-29",
         "Electronics",
         46,
         132,
         11754.36,
         0.0,
         0.0217
        ],
        [
         "2025-04-29",
         "Grocery",
         45,
         123,
         11405.79,
         0.0,
         0.0444
        ],
        [
         "2025-04-29",
         "Beauty",
         44,
         120,
         11249.1,
         0.0,
         0.0455
        ],
        [
         "2025-04-29",
         "Sports",
         23,
         63,
         5598.99,
         0.0,
         0.0435
        ],
        [
         "2025-04-28",
         "Electronics",
         46,
         88,
         8034.4,
         0.0,
         0.0217
        ],
        [
         "2025-04-28",
         "Grocery",
         46,
         84,
         7669.2,
         0.0,
         0.0435
        ],
        [
         "2025-04-28",
         "Apparel",
         45,
         86,
         7527.8,
         0.0,
         0.0222
        ],
        [
         "2025-04-28",
         "Beauty",
         44,
         80,
         7466.0,
         0.0,
         0.0455
        ],
        [
         "2025-04-28",
         "Home",
         45,
         82,
         7405.6,
         0.0,
         0.0444
        ],
        [
         "2025-04-28",
         "Sports",
         23,
         42,
         4077.6,
         0.0,
         0.0435
        ],
        [
         "2025-04-27",
         "Home",
         45,
         215,
         17390.06,
         0.1,
         0.0222
        ],
        [
         "2025-04-27",
         "Apparel",
         44,
         210,
         17167.89,
         0.1,
         0.0227
        ],
        [
         "2025-04-27",
         "Grocery",
         46,
         210,
         16985.64,
         0.1,
         0.0435
        ],
        [
         "2025-04-27",
         "Electronics",
         45,
         205,
         16763.47,
         0.1,
         0.0444
        ],
        [
         "2025-04-27",
         "Beauty",
         46,
         210,
         16621.14,
         0.1,
         0.0435
        ],
        [
         "2025-04-27",
         "Sports",
         23,
         105,
         8310.57,
         0.1,
         0.0435
        ],
        [
         "2025-04-26",
         "Beauty",
         45,
         172,
         15211.68,
         0.0,
         0.0222
        ],
        [
         "2025-04-26",
         "Home",
         45,
         172,
         15049.68,
         0.0,
         0.0222
        ],
        [
         "2025-04-26",
         "Grocery",
         45,
         164,
         14990.16,
         0.0,
         0.0444
        ],
        [
         "2025-04-26",
         "Apparel",
         46,
         168,
         14857.92,
         0.0,
         0.0435
        ],
        [
         "2025-04-26",
         "Electronics",
         44,
         168,
         14371.92,
         0.0,
         0.0227
        ],
        [
         "2025-04-26",
         "Sports",
         23,
         84,
         7428.96,
         0.0,
         0.0435
        ],
        [
         "2025-04-25",
         "Electronics",
         45,
         86,
         7725.86,
         0.0,
         0.0222
        ],
        [
         "2025-04-25",
         "Home",
         46,
         84,
         7470.84,
         0.0,
         0.0435
        ],
        [
         "2025-04-25",
         "Apparel",
         45,
         86,
         7401.86,
         0.0,
         0.0222
        ],
        [
         "2025-04-25",
         "Beauty",
         46,
         84,
         7227.84,
         0.0,
         0.0435
        ],
        [
         "2025-04-25",
         "Grocery",
         45,
         82,
         6972.82,
         0.0,
         0.0444
        ],
        [
         "2025-04-25",
         "Sports",
         22,
         40,
         3318.4,
         0.0,
         0.0455
        ],
        [
         "2025-04-24",
         "Apparel",
         46,
         176,
         15548.08,
         0.0,
         0.0217
        ],
        [
         "2025-04-24",
         "Home",
         46,
         176,
         14576.08,
         0.0,
         0.0217
        ],
        [
         "2025-04-24",
         "Beauty",
         45,
         164,
         14197.12,
         0.0,
         0.0444
        ],
        [
         "2025-04-24",
         "Electronics",
         46,
         168,
         14053.44,
         0.0,
         0.0435
        ],
        [
         "2025-04-24",
         "Grocery",
         44,
         160,
         13692.8,
         0.0,
         0.0455
        ],
        [
         "2025-04-24",
         "Sports",
         22,
         80,
         7008.4,
         0.0,
         0.0455
        ],
        [
         "2025-04-23",
         "Home",
         46,
         132,
         10106.44,
         0.1,
         0.0217
        ],
        [
         "2025-04-23",
         "Electronics",
         46,
         126,
         9652.03,
         0.1,
         0.0435
        ],
        [
         "2025-04-23",
         "Grocery",
         46,
         126,
         9542.69,
         0.1,
         0.0435
        ],
        [
         "2025-04-23",
         "Beauty",
         45,
         129,
         9441.82,
         0.1,
         0.0222
        ],
        [
         "2025-04-23",
         "Apparel",
         44,
         120,
         8869.56,
         0.1,
         0.0455
        ],
        [
         "2025-04-23",
         "Sports",
         22,
         60,
         4872.2,
         0.1,
         0.0455
        ],
        [
         "2025-04-22",
         "Beauty",
         45,
         86,
         7275.92,
         0.0,
         0.0222
        ],
        [
         "2025-04-22",
         "Electronics",
         46,
         88,
         7198.36,
         0.0,
         0.0217
        ],
        [
         "2025-04-22",
         "Apparel",
         45,
         86,
         7194.92,
         0.0,
         0.0222
        ],
        [
         "2025-04-22",
         "Grocery",
         46,
         84,
         6705.48,
         0.0,
         0.0435
        ],
        [
         "2025-04-22",
         "Home",
         45,
         82,
         6702.04,
         0.0,
         0.0444
        ],
        [
         "2025-04-22",
         "Sports",
         22,
         40,
         3227.8,
         0.0,
         0.0455
        ],
        [
         "2025-04-21",
         "Grocery",
         46,
         168,
         11314.26,
         0.2,
         0.0435
        ],
        [
         "2025-04-21",
         "Home",
         45,
         172,
         11055.99,
         0.2,
         0.0222
        ],
        [
         "2025-04-21",
         "Electronics",
         45,
         164,
         10794.93,
         0.2,
         0.0444
        ],
        [
         "2025-04-21",
         "Beauty",
         46,
         168,
         10666.26,
         0.2,
         0.0435
        ],
        [
         "2025-04-21",
         "Apparel",
         43,
         164,
         10406.13,
         0.2,
         0.0233
        ],
        [
         "2025-04-21",
         "Sports",
         23,
         84,
         5203.53,
         0.2,
         0.0435
        ],
        [
         "2025-04-20",
         "Home",
         44,
         168,
         14064.48,
         0.0,
         0.0227
        ],
        [
         "2025-04-20",
         "Beauty",
         46,
         176,
         14055.36,
         0.0,
         0.0217
        ],
        [
         "2025-04-20",
         "Apparel",
         46,
         168,
         13416.48,
         0.0,
         0.0435
        ],
        [
         "2025-04-20",
         "Electronics",
         45,
         164,
         12935.04,
         0.0,
         0.0444
        ],
        [
         "2025-04-20",
         "Grocery",
         45,
         164,
         12611.04,
         0.0,
         0.0444
        ],
        [
         "2025-04-20",
         "Sports",
         23,
         84,
         6546.24,
         0.0,
         0.0435
        ],
        [
         "2025-04-19",
         "Beauty",
         45,
         129,
         9433.73,
         0.1,
         0.0222
        ],
        [
         "2025-04-19",
         "Apparel",
         46,
         132,
         9098.74,
         0.1,
         0.0217
        ],
        [
         "2025-04-19",
         "Grocery",
         45,
         123,
         8791.51,
         0.1,
         0.0444
        ],
        [
         "2025-04-19",
         "Electronics",
         45,
         123,
         8682.16,
         0.1,
         0.0444
        ],
        [
         "2025-04-19",
         "Home",
         45,
         123,
         8354.11,
         0.1,
         0.0444
        ],
        [
         "2025-04-19",
         "Sports",
         23,
         63,
         4556.31,
         0.1,
         0.0435
        ],
        [
         "2025-04-18",
         "Home",
         45,
         172,
         13730.0,
         0.0,
         0.0222
        ],
        [
         "2025-04-18",
         "Apparel",
         45,
         172,
         13406.0,
         0.0,
         0.0222
        ],
        [
         "2025-04-18",
         "Grocery",
         45,
         164,
         12952.0,
         0.0,
         0.0444
        ],
        [
         "2025-04-18",
         "Electronics",
         46,
         168,
         12612.0,
         0.0,
         0.0435
        ],
        [
         "2025-04-18",
         "Beauty",
         45,
         164,
         11980.0,
         0.0,
         0.0444
        ],
        [
         "2025-04-18",
         "Sports",
         23,
         92,
         7084.0,
         0.0,
         0.0
        ],
        [
         "2025-04-17",
         "Beauty",
         45,
         129,
         10113.03,
         0.0,
         0.0222
        ],
        [
         "2025-04-17",
         "Home",
         45,
         129,
         9627.03,
         0.0,
         0.0222
        ],
        [
         "2025-04-17",
         "Grocery",
         46,
         126,
         9521.82,
         0.0,
         0.0435
        ],
        [
         "2025-04-17",
         "Electronics",
         46,
         126,
         9278.82,
         0.0,
         0.0435
        ],
        [
         "2025-04-17",
         "Apparel",
         45,
         123,
         9052.11,
         0.0,
         0.0444
        ],
        [
         "2025-04-17",
         "Sports",
         23,
         63,
         4882.41,
         0.0,
         0.0435
        ],
        [
         "2025-04-16",
         "Apparel",
         45,
         86,
         6457.04,
         0.0,
         0.0222
        ],
        [
         "2025-04-16",
         "Electronics",
         45,
         82,
         6322.48,
         0.0,
         0.0444
        ],
        [
         "2025-04-16",
         "Grocery",
         46,
         84,
         6227.76,
         0.0,
         0.0435
        ],
        [
         "2025-04-16",
         "Beauty",
         45,
         86,
         6133.04,
         0.0,
         0.0222
        ],
        [
         "2025-04-16",
         "Home",
         44,
         80,
         5850.2,
         0.0,
         0.0455
        ],
        [
         "2025-04-16",
         "Sports",
         23,
         42,
         2870.88,
         0.0,
         0.0435
        ],
        [
         "2025-04-15",
         "Apparel",
         46,
         176,
         11809.04,
         0.1,
         0.0217
        ],
        [
         "2025-04-15",
         "Beauty",
         46,
         168,
         11139.72,
         0.1,
         0.0435
        ],
        [
         "2025-04-15",
         "Home",
         45,
         164,
         10877.96,
         0.1,
         0.0444
        ],
        [
         "2025-04-15",
         "Grocery",
         45,
         164,
         10586.36,
         0.1,
         0.0444
        ],
        [
         "2025-04-15",
         "Electronics",
         44,
         160,
         9887.2,
         0.1,
         0.0455
        ],
        [
         "2025-04-15",
         "Sports",
         23,
         92,
         6020.48,
         0.1,
         0.0
        ],
        [
         "2025-04-14",
         "Home",
         46,
         132,
         9408.96,
         0.0,
         0.0217
        ],
        [
         "2025-04-14",
         "Beauty",
         46,
         132,
         9287.46,
         0.0,
         0.0217
        ],
        [
         "2025-04-14",
         "Grocery",
         45,
         129,
         9195.12,
         0.0,
         0.0222
        ],
        [
         "2025-04-14",
         "Electronics",
         45,
         123,
         9131.94,
         0.0,
         0.0444
        ],
        [
         "2025-04-14",
         "Apparel",
         45,
         123,
         8524.44,
         0.0,
         0.0444
        ],
        [
         "2025-04-14",
         "Sports",
         22,
         60,
         4519.8,
         0.0,
         0.0455
        ],
        [
         "2025-04-13",
         "Apparel",
         46,
         132,
         9584.7,
         0.0,
         0.0217
        ],
        [
         "2025-04-13",
         "Grocery",
         45,
         129,
         8889.15,
         0.0,
         0.0222
        ],
        [
         "2025-04-13",
         "Beauty",
         45,
         129,
         8889.15,
         0.0,
         0.0222
        ],
        [
         "2025-04-13",
         "Electronics",
         45,
         123,
         8713.05,
         0.0,
         0.0444
        ],
        [
         "2025-04-13",
         "Home",
         46,
         126,
         8679.6,
         0.0,
         0.0435
        ],
        [
         "2025-04-13",
         "Sports",
         22,
         60,
         4069.5,
         0.0,
         0.0455
        ],
        [
         "2025-04-12",
         "Grocery",
         46,
         210,
         14773.2,
         0.0,
         0.0435
        ],
        [
         "2025-04-12",
         "Electronics",
         46,
         210,
         14570.7,
         0.0,
         0.0435
        ],
        [
         "2025-04-12",
         "Home",
         46,
         210,
         14165.7,
         0.0,
         0.0435
        ],
        [
         "2025-04-12",
         "Apparel",
         45,
         215,
         14102.8,
         0.0,
         0.0222
        ],
        [
         "2025-04-12",
         "Beauty",
         45,
         205,
         13823.6,
         0.0,
         0.0444
        ],
        [
         "2025-04-12",
         "Sports",
         22,
         110,
         7728.7,
         0.0,
         0.0
        ],
        [
         "2025-04-11",
         "Beauty",
         45,
         129,
         7107.94,
         0.2,
         0.0222
        ],
        [
         "2025-04-11",
         "Home",
         45,
         123,
         6689.18,
         0.2,
         0.0444
        ],
        [
         "2025-04-11",
         "Apparel",
         45,
         123,
         6689.18,
         0.2,
         0.0444
        ],
        [
         "2025-04-11",
         "Electronics",
         45,
         123,
         6591.98,
         0.2,
         0.0444
        ],
        [
         "2025-04-11",
         "Grocery",
         45,
         129,
         6524.74,
         0.2,
         0.0222
        ],
        [
         "2025-04-11",
         "Sports",
         23,
         63,
         3084.78,
         0.2,
         0.0435
        ],
        [
         "2025-04-10",
         "Beauty",
         46,
         88,
         5769.28,
         0.0,
         0.0217
        ],
        [
         "2025-04-10",
         "Grocery",
         45,
         86,
         5719.16,
         0.0,
         0.0222
        ],
        [
         "2025-04-10",
         "Apparel",
         45,
         86,
         5638.16,
         0.0,
         0.0222
        ],
        [
         "2025-04-10",
         "Electronics",
         45,
         82,
         5132.92,
         0.0,
         0.0444
        ],
        [
         "2025-04-10",
         "Home",
         45,
         82,
         5132.92,
         0.0,
         0.0444
        ],
        [
         "2025-04-10",
         "Sports",
         23,
         42,
         2915.52,
         0.0,
         0.0435
        ],
        [
         "2025-04-09",
         "Electronics",
         45,
         172,
         11192.36,
         0.0,
         0.0222
        ],
        [
         "2025-04-09",
         "Home",
         45,
         164,
         11165.32,
         0.0,
         0.0444
        ],
        [
         "2025-04-09",
         "Apparel",
         46,
         176,
         10962.88,
         0.0,
         0.0217
        ],
        [
         "2025-04-09",
         "Beauty",
         46,
         168,
         10449.84,
         0.0,
         0.0435
        ],
        [
         "2025-04-09",
         "Grocery",
         45,
         164,
         10355.32,
         0.0,
         0.0444
        ],
        [
         "2025-04-09",
         "Sports",
         22,
         88,
         5967.44,
         0.0,
         0.0
        ],
        [
         "2025-04-08",
         "Beauty",
         45,
         129,
         8452.8,
         0.0,
         0.0222
        ],
        [
         "2025-04-08",
         "Grocery",
         44,
         126,
         7900.2,
         0.0,
         0.0227
        ],
        [
         "2025-04-08",
         "Apparel",
         46,
         126,
         7900.2,
         0.0,
         0.0435
        ],
        [
         "2025-04-08",
         "Electronics",
         45,
         123,
         7712.1,
         0.0,
         0.0444
        ],
        [
         "2025-04-08",
         "Home",
         46,
         126,
         7657.2,
         0.0,
         0.0435
        ],
        [
         "2025-04-08",
         "Sports",
         23,
         63,
         3828.6,
         0.0,
         0.0435
        ],
        [
         "2025-04-07",
         "Grocery",
         45,
         86,
         5034.07,
         0.1,
         0.0222
        ],
        [
         "2025-04-07",
         "Home",
         46,
         84,
         4777.98,
         0.1,
         0.0435
        ],
        [
         "2025-04-07",
         "Electronics",
         46,
         84,
         4632.18,
         0.1,
         0.0435
        ],
        [
         "2025-04-07",
         "Beauty",
         45,
         86,
         4450.87,
         0.1,
         0.0222
        ],
        [
         "2025-04-07",
         "Apparel",
         45,
         82,
         4448.99,
         0.1,
         0.0444
        ],
        [
         "2025-04-07",
         "Sports",
         23,
         42,
         2170.29,
         0.1,
         0.0435
        ],
        [
         "2025-04-06",
         "Beauty",
         45,
         205,
         12874.7,
         0.0,
         0.0444
        ],
        [
         "2025-04-06",
         "Apparel",
         45,
         215,
         12865.6,
         0.0,
         0.0222
        ],
        [
         "2025-04-06",
         "Electronics",
         46,
         220,
         12759.8,
         0.0,
         0.0217
        ],
        [
         "2025-04-06",
         "Home",
         44,
         200,
         11968.0,
         0.0,
         0.0455
        ],
        [
         "2025-04-06",
         "Grocery",
         45,
         205,
         11254.7,
         0.0,
         0.0444
        ],
        [
         "2025-04-06",
         "Sports",
         23,
         115,
         7084.1,
         0.0,
         0.0
        ],
        [
         "2025-04-05",
         "Grocery",
         46,
         176,
         10604.16,
         0.0,
         0.0217
        ],
        [
         "2025-04-05",
         "Electronics",
         45,
         172,
         10208.52,
         0.0,
         0.0222
        ],
        [
         "2025-04-05",
         "Beauty",
         46,
         176,
         9956.16,
         0.0,
         0.0217
        ],
        [
         "2025-04-05",
         "Apparel",
         45,
         164,
         9417.24,
         0.0,
         0.0444
        ],
        [
         "2025-04-05",
         "Home",
         45,
         164,
         8931.24,
         0.0,
         0.0444
        ],
        [
         "2025-04-05",
         "Sports",
         23,
         84,
         5068.44,
         0.0,
         0.0435
        ],
        [
         "2025-04-04",
         "Apparel",
         46,
         84,
         4948.32,
         0.0,
         0.0435
        ],
        [
         "2025-04-04",
         "Beauty",
         46,
         88,
         4933.24,
         0.0,
         0.0217
        ],
        [
         "2025-04-04",
         "Home",
         45,
         82,
         4753.36,
         0.0,
         0.0444
        ],
        [
         "2025-04-04",
         "Grocery",
         44,
         84,
         4705.32,
         0.0,
         0.0227
        ],
        [
         "2025-04-04",
         "Electronics",
         44,
         80,
         4477.4,
         0.0,
         0.0455
        ],
        [
         "2025-04-04",
         "Sports",
         23,
         42,
         2393.16,
         0.0,
         0.0435
        ],
        [
         "2025-04-03",
         "Electronics",
         45,
         172,
         8744.94,
         0.1,
         0.0222
        ],
        [
         "2025-04-03",
         "Beauty",
         45,
         164,
         8344.98,
         0.1,
         0.0444
        ],
        [
         "2025-04-03",
         "Apparel",
         46,
         168,
         8253.36,
         0.1,
         0.0435
        ],
        [
         "2025-04-03",
         "Grocery",
         45,
         164,
         8199.18,
         0.1,
         0.0444
        ],
        [
         "2025-04-03",
         "Home",
         46,
         168,
         8107.56,
         0.1,
         0.0435
        ],
        [
         "2025-04-03",
         "Sports",
         21,
         84,
         4345.38,
         0.1,
         0.0
        ],
        [
         "2025-04-02",
         "Electronics",
         46,
         132,
         7386.84,
         0.0,
         0.0217
        ],
        [
         "2025-04-02",
         "Apparel",
         46,
         126,
         7183.62,
         0.0,
         0.0435
        ],
        [
         "2025-04-02",
         "Home",
         46,
         126,
         6940.62,
         0.0,
         0.0435
        ],
        [
         "2025-04-02",
         "Grocery",
         45,
         129,
         6859.98,
         0.0,
         0.0222
        ],
        [
         "2025-04-02",
         "Beauty",
         45,
         129,
         6859.98,
         0.0,
         0.0222
        ],
        [
         "2025-04-02",
         "Sports",
         22,
         60,
         2882.7,
         0.0,
         0.0455
        ],
        [
         "2025-04-01",
         "Grocery",
         46,
         88,
         3709.2,
         0.2,
         0.0217
        ],
        [
         "2025-04-01",
         "Beauty",
         45,
         86,
         3689.7,
         0.2,
         0.0222
        ],
        [
         "2025-04-01",
         "Home",
         46,
         88,
         3644.4,
         0.2,
         0.0217
        ],
        [
         "2025-04-01",
         "Electronics",
         46,
         84,
         3346.2,
         0.2,
         0.0435
        ],
        [
         "2025-04-01",
         "Apparel",
         44,
         80,
         3242.4,
         0.2,
         0.0455
        ],
        [
         "2025-04-01",
         "Sports",
         21,
         38,
         1731.3,
         0.2,
         0.0476
        ],
        [
         "2025-03-31",
         "Electronics",
         46,
         176,
         15825.76,
         0.0,
         0.0217
        ],
        [
         "2025-03-31",
         "Apparel",
         45,
         164,
         15696.64,
         0.0,
         0.0444
        ],
        [
         "2025-03-31",
         "Grocery",
         46,
         168,
         15253.68,
         0.0,
         0.0435
        ],
        [
         "2025-03-31",
         "Beauty",
         45,
         164,
         15210.64,
         0.0,
         0.0444
        ],
        [
         "2025-03-31",
         "Home",
         45,
         164,
         14886.64,
         0.0,
         0.0444
        ],
        [
         "2025-03-31",
         "Sports",
         23,
         92,
         8441.92,
         0.0,
         0.0
        ],
        [
         "2025-03-30",
         "Grocery",
         46,
         176,
         14599.96,
         0.1,
         0.0217
        ],
        [
         "2025-03-30",
         "Electronics",
         45,
         172,
         13837.37,
         0.1,
         0.0222
        ],
        [
         "2025-03-30",
         "Beauty",
         46,
         168,
         13657.98,
         0.1,
         0.0435
        ],
        [
         "2025-03-30",
         "Apparel",
         45,
         164,
         12895.39,
         0.1,
         0.0444
        ],
        [
         "2025-03-30",
         "Home",
         44,
         160,
         12861.8,
         0.1,
         0.0455
        ],
        [
         "2025-03-30",
         "Sports",
         22,
         80,
         7087.0,
         0.1,
         0.0455
        ],
        [
         "2025-03-29",
         "Home",
         45,
         129,
         11589.6,
         0.0,
         0.0222
        ],
        [
         "2025-03-29",
         "Apparel",
         46,
         126,
         11444.4,
         0.0,
         0.0435
        ],
        [
         "2025-03-29",
         "Electronics",
         45,
         123,
         11177.7,
         0.0,
         0.0444
        ],
        [
         "2025-03-29",
         "Grocery",
         44,
         126,
         10958.4,
         0.0,
         0.0227
        ],
        [
         "2025-03-29",
         "Beauty",
         45,
         123,
         10813.2,
         0.0,
         0.0444
        ],
        [
         "2025-03-29",
         "Sports",
         23,
         63,
         5479.2,
         0.0,
         0.0435
        ],
        [
         "2025-03-28",
         "Home",
         46,
         176,
         15070.72,
         0.0,
         0.0217
        ],
        [
         "2025-03-28",
         "Apparel",
         46,
         168,
         15018.96,
         0.0,
         0.0435
        ],
        [
         "2025-03-28",
         "Grocery",
         45,
         164,
         14993.08,
         0.0,
         0.0444
        ],
        [
         "2025-03-28",
         "Electronics",
         45,
         172,
         14882.84,
         0.0,
         0.0222
        ],
        [
         "2025-03-28",
         "Beauty",
         45,
         164,
         14021.08,
         0.0,
         0.0444
        ],
        [
         "2025-03-28",
         "Sports",
         23,
         92,
         8047.24,
         0.0,
         0.0
        ],
        [
         "2025-03-27",
         "Electronics",
         46,
         132,
         11600.28,
         0.0,
         0.0217
        ],
        [
         "2025-03-27",
         "Home",
         46,
         126,
         11327.04,
         0.0,
         0.0435
        ],
        [
         "2025-03-27",
         "Grocery",
         45,
         129,
         10734.66,
         0.0,
         0.0222
        ],
        [
         "2025-03-27",
         "Apparel",
         45,
         123,
         10582.92,
         0.0,
         0.0444
        ],
        [
         "2025-03-27",
         "Beauty",
         44,
         120,
         10203.3,
         0.0,
         0.0455
        ],
        [
         "2025-03-27",
         "Sports",
         22,
         60,
         5040.9,
         0.0,
         0.0455
        ],
        [
         "2025-03-26",
         "Grocery",
         46,
         88,
         6701.2,
         0.1,
         0.0217
        ],
        [
         "2025-03-26",
         "Beauty",
         45,
         82,
         6463.0,
         0.1,
         0.0444
        ],
        [
         "2025-03-26",
         "Electronics",
         46,
         84,
         6396.6,
         0.1,
         0.0435
        ],
        [
         "2025-03-26",
         "Apparel",
         45,
         82,
         6317.2,
         0.1,
         0.0444
        ],
        [
         "2025-03-26",
         "Home",
         45,
         86,
         6257.3,
         0.1,
         0.0222
        ],
        [
         "2025-03-26",
         "Sports",
         23,
         42,
         3271.2,
         0.1,
         0.0435
        ],
        [
         "2025-03-25",
         "Home",
         45,
         172,
         14468.96,
         0.0,
         0.0222
        ],
        [
         "2025-03-25",
         "Electronics",
         45,
         172,
         14306.96,
         0.0,
         0.0222
        ],
        [
         "2025-03-25",
         "Grocery",
         46,
         168,
         14298.24,
         0.0,
         0.0435
        ],
        [
         "2025-03-25",
         "Apparel",
         45,
         164,
         13479.52,
         0.0,
         0.0444
        ],
        [
         "2025-03-25",
         "Beauty",
         45,
         164,
         13317.52,
         0.0,
         0.0444
        ],
        [
         "2025-03-25",
         "Sports",
         23,
         84,
         7311.12,
         0.0,
         0.0435
        ],
        [
         "2025-03-24",
         "Beauty",
         46,
         126,
         10665.0,
         0.0,
         0.0435
        ],
        [
         "2025-03-24",
         "Apparel",
         45,
         129,
         10545.75,
         0.0,
         0.0222
        ],
        [
         "2025-03-24",
         "Electronics",
         45,
         129,
         10424.25,
         0.0,
         0.0222
        ],
        [
         "2025-03-24",
         "Grocery",
         45,
         129,
         10302.75,
         0.0,
         0.0222
        ],
        [
         "2025-03-24",
         "Home",
         44,
         120,
         10053.0,
         0.0,
         0.0455
        ],
        [
         "2025-03-24",
         "Sports",
         23,
         63,
         5271.75,
         0.0,
         0.0435
        ],
        [
         "2025-03-23",
         "Grocery",
         45,
         129,
         10725.78,
         0.0,
         0.0222
        ],
        [
         "2025-03-23",
         "Home",
         46,
         132,
         10480.74,
         0.0,
         0.0217
        ],
        [
         "2025-03-23",
         "Electronics",
         45,
         123,
         10243.86,
         0.0,
         0.0444
        ],
        [
         "2025-03-23",
         "Beauty",
         46,
         126,
         10120.32,
         0.0,
         0.0435
        ],
        [
         "2025-03-23",
         "Apparel",
         46,
         126,
         9998.82,
         0.0,
         0.0435
        ],
        [
         "2025-03-23",
         "Sports",
         22,
         60,
         4454.7,
         0.0,
         0.0455
        ],
        [
         "2025-03-22",
         "Home",
         46,
         220,
         14370.64,
         0.2,
         0.0217
        ],
        [
         "2025-03-22",
         "Apparel",
         45,
         205,
         13261.96,
         0.2,
         0.0444
        ],
        [
         "2025-03-22",
         "Beauty",
         45,
         205,
         13099.96,
         0.2,
         0.0444
        ],
        [
         "2025-03-22",
         "Electronics",
         45,
         215,
         13083.08,
         0.2,
         0.0222
        ],
        [
         "2025-03-22",
         "Grocery",
         45,
         205,
         12775.96,
         0.2,
         0.0444
        ],
        [
         "2025-03-22",
         "Sports",
         22,
         100,
         6149.2,
         0.2,
         0.0455
        ],
        [
         "2025-03-21",
         "Electronics",
         46,
         88,
         6897.48,
         0.0,
         0.0217
        ],
        [
         "2025-03-21",
         "Grocery",
         46,
         84,
         6587.64,
         0.0,
         0.0435
        ],
        [
         "2025-03-21",
         "Beauty",
         45,
         82,
         6513.72,
         0.0,
         0.0444
        ],
        [
         "2025-03-21",
         "Apparel",
         45,
         86,
         6418.56,
         0.0,
         0.0222
        ],
        [
         "2025-03-21",
         "Home",
         46,
         84,
         6344.64,
         0.0,
         0.0435
        ],
        [
         "2025-03-21",
         "Sports",
         22,
         40,
         3260.4,
         0.0,
         0.0455
        ],
        [
         "2025-03-20",
         "Apparel",
         44,
         42,
         3314.76,
         0.0,
         0.0227
        ],
        [
         "2025-03-20",
         "Home",
         45,
         43,
         3309.79,
         0.0,
         0.0222
        ],
        [
         "2025-03-20",
         "Grocery",
         46,
         44,
         3304.82,
         0.0,
         0.0217
        ],
        [
         "2025-03-20",
         "Electronics",
         46,
         42,
         3193.26,
         0.0,
         0.0435
        ],
        [
         "2025-03-20",
         "Beauty",
         45,
         41,
         2995.73,
         0.0,
         0.0444
        ],
        [
         "2025-03-20",
         "Sports",
         23,
         21,
         1596.63,
         0.0,
         0.0435
        ],
        [
         "2025-03-19",
         "Grocery",
         46,
         126,
         9642.6,
         0.0,
         0.0435
        ],
        [
         "2025-03-19",
         "Electronics",
         45,
         129,
         9501.9,
         0.0,
         0.0222
        ],
        [
         "2025-03-19",
         "Beauty",
         46,
         126,
         9399.6,
         0.0,
         0.0435
        ],
        [
         "2025-03-19",
         "Home",
         44,
         126,
         9278.1,
         0.0,
         0.0227
        ],
        [
         "2025-03-19",
         "Apparel",
         44,
         120,
         8952.0,
         0.0,
         0.0455
        ],
        [
         "2025-03-19",
         "Sports",
         23,
         63,
         4821.3,
         0.0,
         0.0435
        ],
        [
         "2025-03-18",
         "Electronics",
         45,
         86,
         5955.13,
         0.1,
         0.0222
        ],
        [
         "2025-03-18",
         "Apparel",
         46,
         88,
         5795.24,
         0.1,
         0.0217
        ],
        [
         "2025-03-18",
         "Beauty",
         46,
         84,
         5531.82,
         0.1,
         0.0435
        ],
        [
         "2025-03-18",
         "Home",
         45,
         82,
         5473.01,
         0.1,
         0.0444
        ],
        [
         "2025-03-18",
         "Grocery",
         45,
         82,
         5327.21,
         0.1,
         0.0444
        ],
        [
         "2025-03-18",
         "Sports",
         23,
         42,
         2547.21,
         0.1,
         0.0435
        ],
        [
         "2025-03-17",
         "Apparel",
         46,
         44,
         3318.56,
         0.0,
         0.0217
        ],
        [
         "2025-03-17",
         "Grocery",
         45,
         41,
         2941.34,
         0.0,
         0.0444
        ],
        [
         "2025-03-17",
         "Beauty",
         45,
         41,
         2941.34,
         0.0,
         0.0444
        ],
        [
         "2025-03-17",
         "Home",
         44,
         42,
         2932.08,
         0.0,
         0.0227
        ],
        [
         "2025-03-17",
         "Electronics",
         45,
         41,
         2819.84,
         0.0,
         0.0444
        ],
        [
         "2025-03-17",
         "Sports",
         23,
         21,
         1587.54,
         0.0,
         0.0435
        ],
        [
         "2025-03-16",
         "Electronics",
         46,
         176,
         12698.56,
         0.0,
         0.0217
        ],
        [
         "2025-03-16",
         "Home",
         46,
         176,
         12698.56,
         0.0,
         0.0217
        ],
        [
         "2025-03-16",
         "Beauty",
         45,
         172,
         11931.32,
         0.0,
         0.0222
        ],
        [
         "2025-03-16",
         "Grocery",
         45,
         164,
         11368.84,
         0.0,
         0.0444
        ],
        [
         "2025-03-16",
         "Apparel",
         45,
         164,
         10882.84,
         0.0,
         0.0444
        ],
        [
         "2025-03-16",
         "Sports",
         23,
         84,
         6392.04,
         0.0,
         0.0435
        ],
        [
         "2025-03-15",
         "Apparel",
         45,
         129,
         9250.02,
         0.0,
         0.0222
        ],
        [
         "2025-03-15",
         "Electronics",
         46,
         132,
         9092.16,
         0.0,
         0.0217
        ],
        [
         "2025-03-15",
         "Beauty",
         45,
         129,
         9007.02,
         0.0,
         0.0222
        ],
        [
         "2025-03-15",
         "Grocery",
         46,
         126,
         8435.88,
         0.0,
         0.0435
        ],
        [
         "2025-03-15",
         "Home",
         44,
         120,
         8144.1,
         0.0,
         0.0455
        ],
        [
         "2025-03-15",
         "Sports",
         23,
         63,
         4217.94,
         0.0,
         0.0435
        ],
        [
         "2025-03-14",
         "Grocery",
         46,
         42,
         2695.62,
         0.1,
         0.0435
        ],
        [
         "2025-03-14",
         "Home",
         45,
         43,
         2646.98,
         0.1,
         0.0222
        ],
        [
         "2025-03-14",
         "Apparel",
         45,
         43,
         2574.08,
         0.1,
         0.0222
        ],
        [
         "2025-03-14",
         "Beauty",
         46,
         42,
         2513.37,
         0.1,
         0.0435
        ],
        [
         "2025-03-14",
         "Electronics",
         45,
         41,
         2489.11,
         0.1,
         0.0444
        ],
        [
         "2025-03-14",
         "Sports",
         22,
         20,
         1177.75,
         0.1,
         0.0455
        ],
        [
         "2025-03-13",
         "Beauty",
         46,
         132,
         8836.14,
         0.0,
         0.0217
        ],
        [
         "2025-03-13",
         "Home",
         44,
         126,
         8561.52,
         0.0,
         0.0227
        ],
        [
         "2025-03-13",
         "Apparel",
         46,
         126,
         8440.02,
         0.0,
         0.0435
        ],
        [
         "2025-03-13",
         "Electronics",
         45,
         123,
         8241.96,
         0.0,
         0.0444
        ],
        [
         "2025-03-13",
         "Grocery",
         45,
         123,
         7634.46,
         0.0,
         0.0444
        ],
        [
         "2025-03-13",
         "Sports",
         23,
         63,
         4037.76,
         0.0,
         0.0435
        ],
        [
         "2025-03-12",
         "Beauty",
         45,
         86,
         4573.22,
         0.2,
         0.0222
        ],
        [
         "2025-03-12",
         "Apparel",
         46,
         88,
         4482.16,
         0.2,
         0.0217
        ],
        [
         "2025-03-12",
         "Grocery",
         45,
         82,
         4431.34,
         0.2,
         0.0444
        ],
        [
         "2025-03-12",
         "Electronics",
         44,
         84,
         4405.08,
         0.2,
         0.0227
        ],
        [
         "2025-03-12",
         "Home",
         46,
         84,
         4145.88,
         0.2,
         0.0435
        ],
        [
         "2025-03-12",
         "Sports",
         22,
         40,
         2131.6,
         0.2,
         0.0455
        ],
        [
         "2025-03-11",
         "Home",
         45,
         43,
         2796.88,
         0.0,
         0.0222
        ],
        [
         "2025-03-11",
         "Apparel",
         46,
         44,
         2779.04,
         0.0,
         0.0217
        ],
        [
         "2025-03-11",
         "Grocery",
         45,
         41,
         2630.06,
         0.0,
         0.0444
        ],
        [
         "2025-03-11",
         "Electronics",
         46,
         42,
         2612.22,
         0.0,
         0.0435
        ],
        [
         "2025-03-11",
         "Beauty",
         45,
         41,
         2468.06,
         0.0,
         0.0444
        ],
        [
         "2025-03-11",
         "Sports",
         22,
         22,
         1430.02,
         0.0,
         0.0
        ],
        [
         "2025-03-10",
         "Beauty",
         45,
         129,
         7604.21,
         0.1,
         0.0222
        ],
        [
         "2025-03-10",
         "Home",
         46,
         132,
         7114.78,
         0.1,
         0.0217
        ],
        [
         "2025-03-10",
         "Grocery",
         46,
         126,
         7109.49,
         0.1,
         0.0435
        ],
        [
         "2025-03-10",
         "Electronics",
         45,
         123,
         6833.47,
         0.1,
         0.0444
        ],
        [
         "2025-03-10",
         "Apparel",
         45,
         123,
         6724.12,
         0.1,
         0.0444
        ],
        [
         "2025-03-10",
         "Sports",
         22,
         60,
         3333.4,
         0.1,
         0.0455
        ],
        [
         "2025-03-09",
         "Apparel",
         45,
         129,
         8021.7,
         0.0,
         0.0222
        ],
        [
         "2025-03-09",
         "Electronics",
         45,
         129,
         7900.2,
         0.0,
         0.0222
        ],
        [
         "2025-03-09",
         "Grocery",
         46,
         126,
         7719.3,
         0.0,
         0.0435
        ],
        [
         "2025-03-09",
         "Home",
         45,
         123,
         7538.4,
         0.0,
         0.0444
        ],
        [
         "2025-03-09",
         "Beauty",
         45,
         129,
         7414.2,
         0.0,
         0.0222
        ],
        [
         "2025-03-09",
         "Sports",
         23,
         63,
         3555.9,
         0.0,
         0.0435
        ],
        [
         "2025-03-08",
         "Apparel",
         45,
         86,
         5143.82,
         0.0,
         0.0222
        ],
        [
         "2025-03-08",
         "Beauty",
         46,
         84,
         5107.08,
         0.0,
         0.0435
        ],
        [
         "2025-03-08",
         "Home",
         45,
         86,
         5062.82,
         0.0,
         0.0222
        ],
        [
         "2025-03-08",
         "Grocery",
         46,
         84,
         4783.08,
         0.0,
         0.0435
        ],
        [
         "2025-03-08",
         "Electronics",
         44,
         80,
         4385.6,
         0.0,
         0.0455
        ],
        [
         "2025-03-08",
         "Sports",
         23,
         46,
         2789.02,
         0.0,
         0.0
        ],
        [
         "2025-03-07",
         "Beauty",
         46,
         132,
         7582.08,
         0.0,
         0.0217
        ],
        [
         "2025-03-07",
         "Electronics",
         45,
         123,
         7551.12,
         0.0,
         0.0444
        ],
        [
         "2025-03-07",
         "Home",
         45,
         129,
         7288.26,
         0.0,
         0.0222
        ],
        [
         "2025-03-07",
         "Grocery",
         43,
         123,
         7186.62,
         0.0,
         0.0233
        ],
        [
         "2025-03-07",
         "Apparel",
         46,
         126,
         6994.44,
         0.0,
         0.0435
        ],
        [
         "2025-03-07",
         "Sports",
         23,
         63,
         3740.22,
         0.0,
         0.0435
        ],
        [
         "2025-03-06",
         "Apparel",
         45,
         86,
         4553.96,
         0.1,
         0.0222
        ],
        [
         "2025-03-06",
         "Home",
         46,
         84,
         4234.44,
         0.1,
         0.0435
        ],
        [
         "2025-03-06",
         "Beauty",
         45,
         86,
         4189.46,
         0.1,
         0.0222
        ],
        [
         "2025-03-06",
         "Grocery",
         45,
         82,
         4133.62,
         0.1,
         0.0444
        ],
        [
         "2025-03-06",
         "Electronics",
         45,
         82,
         4060.72,
         0.1,
         0.0444
        ],
        [
         "2025-03-06",
         "Sports",
         23,
         42,
         2117.22,
         0.1,
         0.0435
        ],
        [
         "2025-03-05",
         "Electronics",
         46,
         42,
         2373.36,
         0.0,
         0.0435
        ],
        [
         "2025-03-05",
         "Home",
         46,
         42,
         2292.36,
         0.0,
         0.0435
        ],
        [
         "2025-03-05",
         "Beauty",
         45,
         41,
         2278.28,
         0.0,
         0.0444
        ],
        [
         "2025-03-05",
         "Grocery",
         44,
         40,
         2264.2,
         0.0,
         0.0455
        ],
        [
         "2025-03-05",
         "Apparel",
         45,
         43,
         2184.94,
         0.0,
         0.0222
        ],
        [
         "2025-03-05",
         "Sports",
         23,
         23,
         1295.84,
         0.0,
         0.0
        ],
        [
         "2025-03-04",
         "Beauty",
         45,
         129,
         6977.85,
         0.0,
         0.0222
        ],
        [
         "2025-03-04",
         "Electronics",
         46,
         126,
         6939.9,
         0.0,
         0.0435
        ],
        [
         "2025-03-04",
         "Apparel",
         45,
         123,
         6901.95,
         0.0,
         0.0444
        ],
        [
         "2025-03-04",
         "Home",
         44,
         126,
         6818.4,
         0.0,
         0.0227
        ],
        [
         "2025-03-04",
         "Grocery",
         46,
         132,
         6772.8,
         0.0,
         0.0217
        ],
        [
         "2025-03-04",
         "Sports",
         23,
         63,
         2983.95,
         0.0,
         0.0435
        ],
        [
         "2025-03-03",
         "Beauty",
         46,
         88,
         4632.36,
         0.0,
         0.0217
        ],
        [
         "2025-03-03",
         "Grocery",
         45,
         86,
         4528.92,
         0.0,
         0.0222
        ],
        [
         "2025-03-03",
         "Apparel",
         45,
         86,
         4366.92,
         0.0,
         0.0222
        ],
        [
         "2025-03-03",
         "Home",
         45,
         82,
         4160.04,
         0.0,
         0.0444
        ],
        [
         "2025-03-03",
         "Electronics",
         45,
         82,
         3917.04,
         0.0,
         0.0444
        ],
        [
         "2025-03-03",
         "Sports",
         23,
         42,
         2334.24,
         0.0,
         0.0435
        ],
        [
         "2025-03-02",
         "Home",
         45,
         82,
         3493.26,
         0.2,
         0.0444
        ],
        [
         "2025-03-02",
         "Apparel",
         46,
         88,
         3475.44,
         0.2,
         0.0217
        ],
        [
         "2025-03-02",
         "Electronics",
         45,
         82,
         3363.66,
         0.2,
         0.0444
        ],
        [
         "2025-03-02",
         "Beauty",
         45,
         82,
         3234.06,
         0.2,
         0.0444
        ],
        [
         "2025-03-02",
         "Grocery",
         45,
         82,
         3169.26,
         0.2,
         0.0444
        ],
        [
         "2025-03-02",
         "Sports",
         23,
         46,
         1915.38,
         0.2,
         0.0
        ],
        [
         "2025-03-01",
         "Grocery",
         45,
         172,
         15855.92,
         0.0,
         0.0222
        ],
        [
         "2025-03-01",
         "Beauty",
         46,
         176,
         15241.36,
         0.0,
         0.0217
        ],
        [
         "2025-03-01",
         "Home",
         46,
         168,
         14850.48,
         0.0,
         0.0435
        ],
        [
         "2025-03-01",
         "Apparel",
         46,
         168,
         14688.48,
         0.0,
         0.0435
        ],
        [
         "2025-03-01",
         "Electronics",
         45,
         164,
         14655.04,
         0.0,
         0.0444
        ],
        [
         "2025-03-01",
         "Sports",
         21,
         76,
         7277.36,
         0.0,
         0.0476
        ],
        [
         "2025-02-28",
         "Apparel",
         46,
         84,
         7467.12,
         0.0,
         0.0435
        ],
        [
         "2025-02-28",
         "Electronics",
         46,
         84,
         7467.12,
         0.0,
         0.0435
        ],
        [
         "2025-02-28",
         "Beauty",
         44,
         84,
         7467.12,
         0.0,
         0.0227
        ],
        [
         "2025-02-28",
         "Home",
         46,
         84,
         7386.12,
         0.0,
         0.0435
        ],
        [
         "2025-02-28",
         "Grocery",
         45,
         86,
         7237.98,
         0.0,
         0.0222
        ],
        [
         "2025-02-28",
         "Sports",
         22,
         40,
         3517.2,
         0.0,
         0.0455
        ],
        [
         "2025-02-27",
         "Electronics",
         46,
         44,
         3806.0,
         0.0,
         0.0217
        ],
        [
         "2025-02-27",
         "Grocery",
         46,
         42,
         3754.5,
         0.0,
         0.0435
        ],
        [
         "2025-02-27",
         "Apparel",
         45,
         43,
         3719.5,
         0.0,
         0.0222
        ],
        [
         "2025-02-27",
         "Home",
         46,
         42,
         3511.5,
         0.0,
         0.0435
        ],
        [
         "2025-02-27",
         "Beauty",
         44,
         40,
         3379.0,
         0.0,
         0.0455
        ],
        [
         "2025-02-27",
         "Sports",
         22,
         22,
         1862.5,
         0.0,
         0.0
        ],
        [
         "2025-02-26",
         "Grocery",
         46,
         132,
         9887.66,
         0.1,
         0.0217
        ],
        [
         "2025-02-26",
         "Home",
         45,
         123,
         9854.69,
         0.1,
         0.0444
        ],
        [
         "2025-02-26",
         "Electronics",
         46,
         126,
         9537.63,
         0.1,
         0.0435
        ],
        [
         "2025-02-26",
         "Beauty",
         44,
         126,
         9537.63,
         0.1,
         0.0227
        ],
        [
         "2025-02-26",
         "Apparel",
         45,
         123,
         9526.64,
         0.1,
         0.0444
        ],
        [
         "2025-02-26",
         "Sports",
         23,
         63,
         4714.14,
         0.1,
         0.0435
        ],
        [
         "2025-02-25",
         "Beauty",
         46,
         88,
         7684.32,
         0.0,
         0.0217
        ],
        [
         "2025-02-25",
         "Grocery",
         46,
         88,
         7279.32,
         0.0,
         0.0217
        ],
        [
         "2025-02-25",
         "Electronics",
         45,
         82,
         6858.48,
         0.0,
         0.0444
        ],
        [
         "2025-02-25",
         "Apparel",
         44,
         80,
         6691.2,
         0.0,
         0.0455
        ],
        [
         "2025-02-25",
         "Home",
         45,
         82,
         6615.48,
         0.0,
         0.0444
        ],
        [
         "2025-02-25",
         "Sports",
         23,
         42,
         3593.88,
         0.0,
         0.0435
        ],
        [
         "2025-02-24",
         "Electronics",
         44,
         42,
         3493.32,
         0.0,
         0.0227
        ],
        [
         "2025-02-24",
         "Apparel",
         46,
         42,
         3452.82,
         0.0,
         0.0435
        ],
        [
         "2025-02-24",
         "Grocery",
         45,
         41,
         3451.61,
         0.0,
         0.0444
        ],
        [
         "2025-02-24",
         "Home",
         45,
         41,
         3411.11,
         0.0,
         0.0444
        ],
        [
         "2025-02-24",
         "Beauty",
         46,
         42,
         3290.82,
         0.0,
         0.0435
        ],
        [
         "2025-02-24",
         "Sports",
         23,
         23,
         1890.83,
         0.0,
         0.0
        ],
        [
         "2025-02-23",
         "Beauty",
         45,
         172,
         14380.16,
         0.0,
         0.0222
        ],
        [
         "2025-02-23",
         "Home",
         46,
         168,
         13895.04,
         0.0,
         0.0435
        ],
        [
         "2025-02-23",
         "Grocery",
         45,
         172,
         13732.16,
         0.0,
         0.0222
        ],
        [
         "2025-02-23",
         "Electronics",
         45,
         172,
         13408.16,
         0.0,
         0.0222
        ],
        [
         "2025-02-23",
         "Apparel",
         45,
         164,
         12761.92,
         0.0,
         0.0444
        ],
        [
         "2025-02-23",
         "Sports",
         23,
         84,
         6947.52,
         0.0,
         0.0435
        ],
        [
         "2025-02-22",
         "Electronics",
         46,
         126,
         9435.6,
         0.1,
         0.0435
        ],
        [
         "2025-02-22",
         "Beauty",
         45,
         129,
         9103.11,
         0.1,
         0.0222
        ],
        [
         "2025-02-22",
         "Home",
         46,
         126,
         8998.22,
         0.1,
         0.0435
        ],
        [
         "2025-02-22",
         "Grocery",
         44,
         126,
         8998.22,
         0.1,
         0.0227
        ],
        [
         "2025-02-22",
         "Apparel",
         45,
         123,
         8674.64,
         0.1,
         0.0444
        ],
        [
         "2025-02-22",
         "Sports",
         23,
         63,
         4171.07,
         0.1,
         0.0435
        ],
        [
         "2025-02-21",
         "Grocery",
         46,
         42,
         3272.64,
         0.0,
         0.0435
        ],
        [
         "2025-02-21",
         "Electronics",
         46,
         44,
         3266.48,
         0.0,
         0.0217
        ],
        [
         "2025-02-21",
         "Apparel",
         44,
         40,
         3238.3,
         0.0,
         0.0455
        ],
        [
         "2025-02-21",
         "Home",
         45,
         41,
         3235.22,
         0.0,
         0.0444
        ],
        [
         "2025-02-21",
         "Beauty",
         45,
         41,
         3194.72,
         0.0,
         0.0444
        ],
        [
         "2025-02-21",
         "Sports",
         23,
         23,
         1792.16,
         0.0,
         0.0
        ],
        [
         "2025-02-20",
         "Grocery",
         46,
         132,
         8077.52,
         0.2,
         0.0217
        ],
        [
         "2025-02-20",
         "Electronics",
         45,
         129,
         7991.14,
         0.2,
         0.0222
        ],
        [
         "2025-02-20",
         "Beauty",
         46,
         126,
         7807.56,
         0.2,
         0.0435
        ],
        [
         "2025-02-20",
         "Home",
         45,
         123,
         7429.58,
         0.2,
         0.0444
        ],
        [
         "2025-02-20",
         "Apparel",
         45,
         123,
         7137.98,
         0.2,
         0.0444
        ],
        [
         "2025-02-20",
         "Sports",
         23,
         63,
         4146.78,
         0.2,
         0.0435
        ],
        [
         "2025-02-19",
         "Beauty",
         46,
         88,
         6605.28,
         0.0,
         0.0217
        ],
        [
         "2025-02-19",
         "Apparel",
         46,
         84,
         6548.04,
         0.0,
         0.0435
        ],
        [
         "2025-02-19",
         "Grocery",
         45,
         86,
         6374.16,
         0.0,
         0.0222
        ],
        [
         "2025-02-19",
         "Home",
         44,
         84,
         6305.04,
         0.0,
         0.0227
        ],
        [
         "2025-02-19",
         "Electronics",
         45,
         82,
         6235.92,
         0.0,
         0.0444
        ],
        [
         "2025-02-19",
         "Sports",
         23,
         42,
         3071.52,
         0.0,
         0.0435
        ],
        [
         "2025-02-18",
         "Grocery",
         45,
         41,
         2826.42,
         0.1,
         0.0444
        ],
        [
         "2025-02-18",
         "Apparel",
         46,
         42,
         2783.34,
         0.1,
         0.0435
        ],
        [
         "2025-02-18",
         "Electronics",
         45,
         43,
         2776.71,
         0.1,
         0.0222
        ],
        [
         "2025-02-18",
         "Beauty",
         46,
         42,
         2746.89,
         0.1,
         0.0435
        ],
        [
         "2025-02-18",
         "Home",
         45,
         41,
         2644.17,
         0.1,
         0.0444
        ],
        [
         "2025-02-18",
         "Sports",
         22,
         22,
         1494.39,
         0.1,
         0.0
        ],
        [
         "2025-02-17",
         "Electronics",
         45,
         129,
         9556.8,
         0.0,
         0.0222
        ],
        [
         "2025-02-17",
         "Home",
         46,
         126,
         9340.2,
         0.0,
         0.0435
        ],
        [
         "2025-02-17",
         "Apparel",
         46,
         126,
         9218.7,
         0.0,
         0.0435
        ],
        [
         "2025-02-17",
         "Beauty",
         45,
         123,
         8880.6,
         0.0,
         0.0444
        ],
        [
         "2025-02-17",
         "Grocery",
         45,
         129,
         8827.8,
         0.0,
         0.0222
        ],
        [
         "2025-02-17",
         "Sports",
         22,
         60,
         4089.0,
         0.0,
         0.0455
        ],
        [
         "2025-02-16",
         "Grocery",
         46,
         132,
         9463.14,
         0.0,
         0.0217
        ],
        [
         "2025-02-16",
         "Apparel",
         45,
         123,
         8947.71,
         0.0,
         0.0444
        ],
        [
         "2025-02-16",
         "Beauty",
         45,
         123,
         8826.21,
         0.0,
         0.0444
        ],
        [
         "2025-02-16",
         "Home",
         45,
         129,
         8764.83,
         0.0,
         0.0222
        ],
        [
         "2025-02-16",
         "Electronics",
         46,
         126,
         8674.02,
         0.0,
         0.0435
        ],
        [
         "2025-02-16",
         "Sports",
         21,
         57,
         4276.89,
         0.0,
         0.0476
        ],
        [
         "2025-02-15",
         "Electronics",
         46,
         88,
         6182.92,
         0.0,
         0.0217
        ],
        [
         "2025-02-15",
         "Home",
         45,
         86,
         6125.24,
         0.0,
         0.0222
        ],
        [
         "2025-02-15",
         "Grocery",
         46,
         84,
         5824.56,
         0.0,
         0.0435
        ],
        [
         "2025-02-15",
         "Apparel",
         45,
         82,
         5442.88,
         0.0,
         0.0444
        ],
        [
         "2025-02-15",
         "Beauty",
         45,
         82,
         5442.88,
         0.0,
         0.0444
        ],
        [
         "2025-02-15",
         "Sports",
         23,
         46,
         3270.64,
         0.0,
         0.0
        ],
        [
         "2025-02-14",
         "Apparel",
         45,
         129,
         7993.83,
         0.1,
         0.0222
        ],
        [
         "2025-02-14",
         "Grocery",
         46,
         132,
         7958.49,
         0.1,
         0.0217
        ],
        [
         "2025-02-14",
         "Beauty",
         45,
         123,
         7955.16,
         0.1,
         0.0444
        ],
        [
         "2025-02-14",
         "Electronics",
         45,
         129,
         7884.48,
         0.1,
         0.0222
        ],
        [
         "2025-02-14",
         "Home",
         44,
         120,
         7334.4,
         0.1,
         0.0455
        ],
        [
         "2025-02-14",
         "Sports",
         23,
         63,
         3741.21,
         0.1,
         0.0435
        ],
        [
         "2025-02-13",
         "Grocery",
         45,
         86,
         6041.28,
         0.0,
         0.0222
        ],
        [
         "2025-02-13",
         "Home",
         45,
         86,
         5636.28,
         0.0,
         0.0222
        ],
        [
         "2025-02-13",
         "Apparel",
         46,
         84,
         5584.32,
         0.0,
         0.0435
        ],
        [
         "2025-02-13",
         "Beauty",
         46,
         84,
         5422.32,
         0.0,
         0.0435
        ],
        [
         "2025-02-13",
         "Electronics",
         44,
         80,
         5318.4,
         0.0,
         0.0455
        ],
        [
         "2025-02-13",
         "Sports",
         23,
         42,
         2630.16,
         0.0,
         0.0435
        ],
        [
         "2025-02-12",
         "Home",
         46,
         44,
         3024.2,
         0.0,
         0.0217
        ],
        [
         "2025-02-12",
         "Beauty",
         45,
         41,
         2788.55,
         0.0,
         0.0444
        ],
        [
         "2025-02-12",
         "Electronics",
         45,
         43,
         2675.65,
         0.0,
         0.0222
        ],
        [
         "2025-02-12",
         "Apparel",
         45,
         41,
         2626.55,
         0.0,
         0.0444
        ],
        [
         "2025-02-12",
         "Grocery",
         45,
         41,
         2545.55,
         0.0,
         0.0444
        ],
        [
         "2025-02-12",
         "Sports",
         23,
         21,
         1325.55,
         0.0,
         0.0435
        ],
        [
         "2025-02-11",
         "Electronics",
         44,
         126,
         8380.62,
         0.0,
         0.0227
        ],
        [
         "2025-02-11",
         "Grocery",
         45,
         129,
         8328.48,
         0.0,
         0.0222
        ],
        [
         "2025-02-11",
         "Apparel",
         45,
         129,
         8085.48,
         0.0,
         0.0222
        ],
        [
         "2025-02-11",
         "Beauty",
         45,
         123,
         7946.76,
         0.0,
         0.0444
        ],
        [
         "2025-02-11",
         "Home",
         46,
         126,
         7530.12,
         0.0,
         0.0435
        ],
        [
         "2025-02-11",
         "Sports",
         23,
         63,
         4129.56,
         0.0,
         0.0435
        ],
        [
         "2025-02-10",
         "Home",
         45,
         86,
         4472.9,
         0.2,
         0.0222
        ],
        [
         "2025-02-10",
         "Grocery",
         46,
         88,
         4378.0,
         0.2,
         0.0217
        ],
        [
         "2025-02-10",
         "Apparel",
         45,
         86,
         4343.3,
         0.2,
         0.0222
        ],
        [
         "2025-02-10",
         "Electronics",
         46,
         84,
         4049.4,
         0.2,
         0.0435
        ],
        [
         "2025-02-10",
         "Beauty",
         45,
         82,
         3820.3,
         0.2,
         0.0444
        ],
        [
         "2025-02-10",
         "Sports",
         23,
         42,
         2089.5,
         0.2,
         0.0435
        ],
        [
         "2025-02-09",
         "Electronics",
         44,
         84,
         5265.84,
         0.0,
         0.0227
        ],
        [
         "2025-02-09",
         "Beauty",
         46,
         84,
         5184.84,
         0.0,
         0.0435
        ],
        [
         "2025-02-09",
         "Home",
         45,
         86,
         5144.36,
         0.0,
         0.0222
        ],
        [
         "2025-02-09",
         "Grocery",
         45,
         82,
         4982.32,
         0.0,
         0.0444
        ],
        [
         "2025-02-09",
         "Apparel",
         45,
         82,
         4820.32,
         0.0,
         0.0444
        ],
        [
         "2025-02-09",
         "Sports",
         23,
         42,
         2632.92,
         0.0,
         0.0435
        ],
        [
         "2025-02-08",
         "Electronics",
         45,
         172,
         10690.76,
         0.0,
         0.0222
        ],
        [
         "2025-02-08",
         "Apparel",
         46,
         176,
         10604.08,
         0.0,
         0.0217
        ],
        [
         "2025-02-08",
         "Grocery",
         44,
         168,
         10291.44,
         0.0,
         0.0227
        ],
        [
         "2025-02-08",
         "Home",
         45,
         164,
         9730.12,
         0.0,
         0.0444
        ],
        [
         "2025-02-08",
         "Beauty",
         46,
         168,
         9643.44,
         0.0,
         0.0435
        ],
        [
         "2025-02-08",
         "Sports",
         23,
         84,
         4497.72,
         0.0,
         0.0435
        ],
        [
         "2025-02-07",
         "Apparel",
         46,
         88,
         5257.2,
         0.0,
         0.0217
        ],
        [
         "2025-02-07",
         "Grocery",
         45,
         86,
         5060.4,
         0.0,
         0.0222
        ],
        [
         "2025-02-07",
         "Home",
         46,
         88,
         5014.2,
         0.0,
         0.0217
        ],
        [
         "2025-02-07",
         "Beauty",
         45,
         82,
         4747.8,
         0.0,
         0.0444
        ],
        [
         "2025-02-07",
         "Electronics",
         45,
         82,
         4423.8,
         0.0,
         0.0444
        ],
        [
         "2025-02-07",
         "Sports",
         22,
         40,
         2559.0,
         0.0,
         0.0455
        ],
        [
         "2025-02-06",
         "Electronics",
         46,
         44,
         2345.43,
         0.1,
         0.0217
        ],
        [
         "2025-02-06",
         "Home",
         45,
         43,
         2221.71,
         0.1,
         0.0222
        ],
        [
         "2025-02-06",
         "Beauty",
         45,
         43,
         2185.26,
         0.1,
         0.0222
        ],
        [
         "2025-02-06",
         "Apparel",
         46,
         42,
         2025.09,
         0.1,
         0.0435
        ],
        [
         "2025-02-06",
         "Grocery",
         44,
         40,
         1959.9,
         0.1,
         0.0455
        ],
        [
         "2025-02-06",
         "Sports",
         22,
         20,
         1089.3,
         0.1,
         0.0455
        ],
        [
         "2025-02-05",
         "Apparel",
         45,
         129,
         7586.16,
         0.0,
         0.0222
        ],
        [
         "2025-02-05",
         "Electronics",
         46,
         132,
         7022.28,
         0.0,
         0.0217
        ],
        [
         "2025-02-05",
         "Beauty",
         45,
         123,
         6891.42,
         0.0,
         0.0444
        ],
        [
         "2025-02-05",
         "Home",
         46,
         126,
         6813.54,
         0.0,
         0.0435
        ],
        [
         "2025-02-05",
         "Grocery",
         46,
         126,
         6813.54,
         0.0,
         0.0435
        ],
        [
         "2025-02-05",
         "Sports",
         22,
         60,
         3180.9,
         0.0,
         0.0455
        ],
        [
         "2025-02-04",
         "Home",
         45,
         86,
         4772.46,
         0.0,
         0.0222
        ],
        [
         "2025-02-04",
         "Grocery",
         45,
         82,
         4639.02,
         0.0,
         0.0444
        ],
        [
         "2025-02-04",
         "Electronics",
         45,
         82,
         4477.02,
         0.0,
         0.0444
        ],
        [
         "2025-02-04",
         "Apparel",
         45,
         86,
         4367.46,
         0.0,
         0.0222
        ],
        [
         "2025-02-04",
         "Beauty",
         45,
         82,
         4315.02,
         0.0,
         0.0444
        ],
        [
         "2025-02-04",
         "Sports",
         23,
         42,
         2170.62,
         0.0,
         0.0435
        ],
        [
         "2025-02-03",
         "Beauty",
         46,
         44,
         2376.92,
         0.0,
         0.0217
        ],
        [
         "2025-02-03",
         "Home",
         45,
         43,
         2284.24,
         0.0,
         0.0222
        ],
        [
         "2025-02-03",
         "Electronics",
         45,
         43,
         2243.74,
         0.0,
         0.0222
        ],
        [
         "2025-02-03",
         "Apparel",
         45,
         41,
         2179.88,
         0.0,
         0.0444
        ],
        [
         "2025-02-03",
         "Grocery",
         46,
         42,
         2070.06,
         0.0,
         0.0435
        ],
        [
         "2025-02-03",
         "Sports",
         22,
         20,
         881.6,
         0.0,
         0.0455
        ],
        [
         "2025-02-02",
         "Grocery",
         45,
         164,
         8073.9,
         0.1,
         0.0444
        ],
        [
         "2025-02-02",
         "Apparel",
         46,
         176,
         8038.8,
         0.1,
         0.0217
        ],
        [
         "2025-02-02",
         "Beauty",
         46,
         176,
         8038.8,
         0.1,
         0.0217
        ],
        [
         "2025-02-02",
         "Electronics",
         45,
         172,
         7710.3,
         0.1,
         0.0222
        ],
        [
         "2025-02-02",
         "Home",
         45,
         164,
         7053.3,
         0.1,
         0.0444
        ],
        [
         "2025-02-02",
         "Sports",
         23,
         84,
         4128.3,
         0.1,
         0.0435
        ],
        [
         "2025-02-01",
         "Home",
         46,
         132,
         6996.24,
         0.0,
         0.0217
        ],
        [
         "2025-02-01",
         "Apparel",
         46,
         132,
         6267.24,
         0.0,
         0.0217
        ],
        [
         "2025-02-01",
         "Grocery",
         45,
         123,
         5944.86,
         0.0,
         0.0444
        ],
        [
         "2025-02-01",
         "Electronics",
         45,
         123,
         5944.86,
         0.0,
         0.0444
        ],
        [
         "2025-02-01",
         "Beauty",
         44,
         120,
         5918.4,
         0.0,
         0.0455
        ],
        [
         "2025-02-01",
         "Sports",
         22,
         60,
         3080.7,
         0.0,
         0.0455
        ],
        [
         "2025-01-31",
         "Beauty",
         45,
         43,
         1744.53,
         0.2,
         0.0222
        ],
        [
         "2025-01-31",
         "Electronics",
         46,
         42,
         1609.02,
         0.2,
         0.0435
        ],
        [
         "2025-01-31",
         "Apparel",
         45,
         41,
         1603.11,
         0.2,
         0.0444
        ],
        [
         "2025-01-31",
         "Grocery",
         45,
         41,
         1603.11,
         0.2,
         0.0444
        ],
        [
         "2025-01-31",
         "Home",
         46,
         44,
         1556.04,
         0.2,
         0.0217
        ],
        [
         "2025-01-31",
         "Sports",
         23,
         21,
         772.11,
         0.2,
         0.0435
        ],
        [
         "2025-01-30",
         "Beauty",
         45,
         129,
         11339.34,
         0.0,
         0.0222
        ],
        [
         "2025-01-30",
         "Apparel",
         44,
         126,
         11078.46,
         0.0,
         0.0227
        ],
        [
         "2025-01-30",
         "Electronics",
         46,
         126,
         10956.96,
         0.0,
         0.0435
        ],
        [
         "2025-01-30",
         "Home",
         45,
         123,
         10817.58,
         0.0,
         0.0444
        ],
        [
         "2025-01-30",
         "Grocery",
         46,
         126,
         10470.96,
         0.0,
         0.0435
        ],
        [
         "2025-01-30",
         "Sports",
         22,
         60,
         5217.6,
         0.0,
         0.0455
        ],
        [
         "2025-01-29",
         "Grocery",
         46,
         84,
         6611.7,
         0.1,
         0.0435
        ],
        [
         "2025-01-29",
         "Apparel",
         45,
         86,
         6546.95,
         0.1,
         0.0222
        ],
        [
         "2025-01-29",
         "Electronics",
         45,
         82,
         6530.65,
         0.1,
         0.0444
        ],
        [
         "2025-01-29",
         "Home",
         45,
         86,
         6474.05,
         0.1,
         0.0222
        ],
        [
         "2025-01-29",
         "Beauty",
         45,
         82,
         6311.95,
         0.1,
         0.0444
        ],
        [
         "2025-01-29",
         "Sports",
         23,
         46,
         3467.95,
         0.1,
         0.0
        ],
        [
         "2025-01-28",
         "Home",
         45,
         43,
         3737.8,
         0.0,
         0.0222
        ],
        [
         "2025-01-28",
         "Beauty",
         46,
         44,
         3659.9,
         0.0,
         0.0217
        ],
        [
         "2025-01-28",
         "Apparel",
         46,
         42,
         3613.2,
         0.0,
         0.0435
        ],
        [
         "2025-01-28",
         "Grocery",
         45,
         41,
         3407.6,
         0.0,
         0.0444
        ],
        [
         "2025-01-28",
         "Electronics",
         45,
         41,
         3367.1,
         0.0,
         0.0444
        ],
        [
         "2025-01-28",
         "Sports",
         23,
         21,
         1685.1,
         0.0,
         0.0435
        ],
        [
         "2025-01-27",
         "Beauty",
         45,
         129,
         11150.43,
         0.0,
         0.0222
        ],
        [
         "2025-01-27",
         "Apparel",
         46,
         132,
         10547.94,
         0.0,
         0.0217
        ],
        [
         "2025-01-27",
         "Home",
         46,
         126,
         10173.42,
         0.0,
         0.0435
        ],
        [
         "2025-01-27",
         "Electronics",
         45,
         123,
         10046.91,
         0.0,
         0.0444
        ],
        [
         "2025-01-27",
         "Grocery",
         44,
         120,
         9798.9,
         0.0,
         0.0455
        ],
        [
         "2025-01-27",
         "Sports",
         22,
         60,
         5203.2,
         0.0,
         0.0455
        ],
        [
         "2025-01-26",
         "Apparel",
         46,
         132,
         10845.18,
         0.0,
         0.0217
        ],
        [
         "2025-01-26",
         "Home",
         46,
         126,
         10357.74,
         0.0,
         0.0435
        ],
        [
         "2025-01-26",
         "Electronics",
         46,
         126,
         10236.24,
         0.0,
         0.0435
        ],
        [
         "2025-01-26",
         "Grocery",
         45,
         123,
         10114.02,
         0.0,
         0.0444
        ],
        [
         "2025-01-26",
         "Beauty",
         45,
         123,
         9628.02,
         0.0,
         0.0444
        ],
        [
         "2025-01-26",
         "Sports",
         22,
         66,
         5240.34,
         0.0,
         0.0
        ],
        [
         "2025-01-25",
         "Grocery",
         46,
         88,
         6321.04,
         0.1,
         0.0217
        ],
        [
         "2025-01-25",
         "Home",
         46,
         88,
         6248.14,
         0.1,
         0.0217
        ],
        [
         "2025-01-25",
         "Beauty",
         43,
         82,
         5962.96,
         0.1,
         0.0233
        ],
        [
         "2025-01-25",
         "Apparel",
         45,
         82,
         5817.16,
         0.1,
         0.0444
        ],
        [
         "2025-01-25",
         "Electronics",
         46,
         84,
         5815.02,
         0.1,
         0.0435
        ],
        [
         "2025-01-25",
         "Sports",
         22,
         40,
         2946.1,
         0.1,
         0.0455
        ],
        [
         "2025-01-24",
         "Electronics",
         46,
         126,
         10240.38,
         0.0,
         0.0435
        ],
        [
         "2025-01-24",
         "Beauty",
         45,
         129,
         10111.02,
         0.0,
         0.0222
        ],
        [
         "2025-01-24",
         "Apparel",
         45,
         129,
         9989.52,
         0.0,
         0.0222
        ],
        [
         "2025-01-24",
         "Grocery",
         46,
         126,
         9754.38,
         0.0,
         0.0435
        ],
        [
         "2025-01-24",
         "Home",
         44,
         120,
         9527.1,
         0.0,
         0.0455
        ],
        [
         "2025-01-24",
         "Sports",
         23,
         63,
         4573.44,
         0.0,
         0.0435
        ],
        [
         "2025-01-23",
         "Apparel",
         45,
         86,
         6941.7,
         0.0,
         0.0222
        ],
        [
         "2025-01-23",
         "Grocery",
         46,
         84,
         6382.8,
         0.0,
         0.0435
        ],
        [
         "2025-01-23",
         "Beauty",
         45,
         82,
         6309.9,
         0.0,
         0.0444
        ],
        [
         "2025-01-23",
         "Home",
         45,
         82,
         6228.9,
         0.0,
         0.0444
        ],
        [
         "2025-01-23",
         "Electronics",
         45,
         82,
         6066.9,
         0.0,
         0.0444
        ],
        [
         "2025-01-23",
         "Sports",
         23,
         46,
         3539.7,
         0.0,
         0.0
        ],
        [
         "2025-01-22",
         "Grocery",
         45,
         43,
         3287.86,
         0.0,
         0.0222
        ],
        [
         "2025-01-22",
         "Beauty",
         46,
         44,
         3241.88,
         0.0,
         0.0217
        ],
        [
         "2025-01-22",
         "Home",
         45,
         41,
         3096.32,
         0.0,
         0.0444
        ],
        [
         "2025-01-22",
         "Electronics",
         44,
         40,
         3020.8,
         0.0,
         0.0455
        ],
        [
         "2025-01-22",
         "Apparel",
         45,
         41,
         2974.82,
         0.0,
         0.0444
        ],
        [
         "2025-01-22",
         "Sports",
         23,
         21,
         1707.42,
         0.0,
         0.0435
        ],
        [
         "2025-01-21",
         "Apparel",
         46,
         132,
         8018.48,
         0.2,
         0.0217
        ],
        [
         "2025-01-21",
         "Beauty",
         45,
         129,
         7743.46,
         0.2,
         0.0222
        ],
        [
         "2025-01-21",
         "Electronics",
         45,
         123,
         7485.02,
         0.2,
         0.0444
        ],
        [
         "2025-01-21",
         "Grocery",
         45,
         129,
         7354.66,
         0.2,
         0.0222
        ],
        [
         "2025-01-21",
         "Home",
         46,
         126,
         7274.04,
         0.2,
         0.0435
        ],
        [
         "2025-01-21",
         "Sports",
         23,
         63,
         3637.02,
         0.2,
         0.0435
        ],
        [
         "2025-01-20",
         "Apparel",
         44,
         88,
         6394.08,
         0.0,
         0.0
        ],
        [
         "2025-01-20",
         "Grocery",
         44,
         80,
         6217.8,
         0.0,
         0.0455
        ],
        [
         "2025-01-20",
         "Electronics",
         46,
         84,
         6103.44,
         0.0,
         0.0435
        ],
        [
         "2025-01-20",
         "Home",
         46,
         84,
         5941.44,
         0.0,
         0.0435
        ],
        [
         "2025-01-20",
         "Beauty",
         45,
         82,
         5796.12,
         0.0,
         0.0444
        ],
        [
         "2025-01-20",
         "Sports",
         23,
         46,
         3342.36,
         0.0,
         0.0
        ],
        [
         "2025-01-19",
         "Home",
         45,
         82,
         6083.86,
         0.0,
         0.0444
        ],
        [
         "2025-01-19",
         "Beauty",
         44,
         84,
         6064.32,
         0.0,
         0.0227
        ],
        [
         "2025-01-19",
         "Electronics",
         46,
         84,
         5983.32,
         0.0,
         0.0435
        ],
        [
         "2025-01-19",
         "Grocery",
         46,
         88,
         5944.24,
         0.0,
         0.0217
        ],
        [
         "2025-01-19",
         "Apparel",
         45,
         82,
         5840.86,
         0.0,
         0.0444
        ],
        [
         "2025-01-19",
         "Sports",
         23,
         42,
         2910.66,
         0.0,
         0.0435
        ],
        [
         "2025-01-18",
         "Beauty",
         46,
         176,
         12608.8,
         0.0,
         0.0217
        ],
        [
         "2025-01-18",
         "Grocery",
         46,
         176,
         12284.8,
         0.0,
         0.0217
        ],
        [
         "2025-01-18",
         "Apparel",
         44,
         160,
         11330.0,
         0.0,
         0.0455
        ],
        [
         "2025-01-18",
         "Electronics",
         45,
         164,
         11285.2,
         0.0,
         0.0444
        ],
        [
         "2025-01-18",
         "Home",
         45,
         164,
         10799.2,
         0.0,
         0.0444
        ],
        [
         "2025-01-18",
         "Sports",
         23,
         84,
         6025.2,
         0.0,
         0.0435
        ],
        [
         "2025-01-17",
         "Electronics",
         45,
         86,
         5292.01,
         0.1,
         0.0222
        ],
        [
         "2025-01-17",
         "Apparel",
         45,
         86,
         5219.11,
         0.1,
         0.0222
        ],
        [
         "2025-01-17",
         "Home",
         44,
         80,
         5214.4,
         0.1,
         0.0455
        ],
        [
         "2025-01-17",
         "Grocery",
         45,
         82,
         5118.77,
         0.1,
         0.0444
        ],
        [
         "2025-01-17",
         "Beauty",
         46,
         84,
         4950.24,
         0.1,
         0.0435
        ],
        [
         "2025-01-17",
         "Sports",
         23,
         46,
         2830.61,
         0.1,
         0.0
        ],
        [
         "2025-01-16",
         "Beauty",
         46,
         44,
         3066.86,
         0.0,
         0.0217
        ],
        [
         "2025-01-16",
         "Electronics",
         45,
         43,
         2837.92,
         0.0,
         0.0222
        ],
        [
         "2025-01-16",
         "Home",
         46,
         42,
         2811.48,
         0.0,
         0.0435
        ],
        [
         "2025-01-16",
         "Grocery",
         45,
         43,
         2797.42,
         0.0,
         0.0222
        ],
        [
         "2025-01-16",
         "Apparel",
         46,
         42,
         2770.98,
         0.0,
         0.0435
        ],
        [
         "2025-01-16",
         "Sports",
         22,
         20,
         1338.8,
         0.0,
         0.0455
        ],
        [
         "2025-01-15",
         "Grocery",
         45,
         129,
         8815.29,
         0.0,
         0.0222
        ],
        [
         "2025-01-15",
         "Electronics",
         46,
         126,
         8497.26,
         0.0,
         0.0435
        ],
        [
         "2025-01-15",
         "Apparel",
         46,
         126,
         8132.76,
         0.0,
         0.0435
        ],
        [
         "2025-01-15",
         "Home",
         44,
         126,
         8132.76,
         0.0,
         0.0227
        ],
        [
         "2025-01-15",
         "Beauty",
         45,
         129,
         8086.29,
         0.0,
         0.0222
        ],
        [
         "2025-01-15",
         "Sports",
         22,
         60,
         3566.1,
         0.0,
         0.0455
        ],
        [
         "2025-01-14",
         "Home",
         46,
         84,
         5463.72,
         0.0,
         0.0435
        ],
        [
         "2025-01-14",
         "Electronics",
         46,
         88,
         5396.04,
         0.0,
         0.0217
        ],
        [
         "2025-01-14",
         "Apparel",
         45,
         82,
         5335.56,
         0.0,
         0.0444
        ],
        [
         "2025-01-14",
         "Beauty",
         45,
         82,
         5335.56,
         0.0,
         0.0444
        ],
        [
         "2025-01-14",
         "Grocery",
         45,
         82,
         5092.56,
         0.0,
         0.0444
        ],
        [
         "2025-01-14",
         "Sports",
         22,
         44,
         2900.52,
         0.0,
         0.0
        ],
        [
         "2025-01-13",
         "Electronics",
         46,
         44,
         2517.61,
         0.1,
         0.0217
        ],
        [
         "2025-01-13",
         "Grocery",
         46,
         44,
         2517.61,
         0.1,
         0.0217
        ],
        [
         "2025-01-13",
         "Beauty",
         45,
         43,
         2424.77,
         0.1,
         0.0222
        ],
        [
         "2025-01-13",
         "Apparel",
         45,
         41,
         2202.64,
         0.1,
         0.0444
        ],
        [
         "2025-01-13",
         "Home",
         44,
         40,
         2146.25,
         0.1,
         0.0455
        ],
        [
         "2025-01-13",
         "Sports",
         23,
         21,
         1220.64,
         0.1,
         0.0435
        ],
        [
         "2025-01-12",
         "Home",
         45,
         172,
         10691.84,
         0.0,
         0.0222
        ],
        [
         "2025-01-12",
         "Apparel",
         45,
         164,
         10688.08,
         0.0,
         0.0444
        ],
        [
         "2025-01-12",
         "Grocery",
         46,
         176,
         10612.72,
         0.0,
         0.0217
        ],
        [
         "2025-01-12",
         "Beauty",
         45,
         172,
         10043.84,
         0.0,
         0.0222
        ],
        [
         "2025-01-12",
         "Electronics",
         44,
         160,
         9795.2,
         0.0,
         0.0455
        ],
        [
         "2025-01-12",
         "Sports",
         23,
         84,
         5142.48,
         0.0,
         0.0435
        ],
        [
         "2025-01-11",
         "Electronics",
         45,
         129,
         6073.3,
         0.2,
         0.0222
        ],
        [
         "2025-01-11",
         "Beauty",
         46,
         126,
         6027.0,
         0.2,
         0.0435
        ],
        [
         "2025-01-11",
         "Grocery",
         45,
         123,
         5980.7,
         0.2,
         0.0444
        ],
        [
         "2025-01-11",
         "Home",
         45,
         123,
         5883.5,
         0.2,
         0.0444
        ],
        [
         "2025-01-11",
         "Apparel",
         46,
         126,
         5832.6,
         0.2,
         0.0435
        ],
        [
         "2025-01-11",
         "Sports",
         23,
         69,
         3397.7,
         0.2,
         0.0
        ],
        [
         "2025-01-10",
         "Electronics",
         44,
         42,
         2572.62,
         0.0,
         0.0227
        ],
        [
         "2025-01-10",
         "Apparel",
         46,
         42,
         2532.12,
         0.0,
         0.0435
        ],
        [
         "2025-01-10",
         "Home",
         46,
         42,
         2451.12,
         0.0,
         0.0435
        ],
        [
         "2025-01-10",
         "Grocery",
         45,
         43,
         2387.98,
         0.0,
         0.0222
        ],
        [
         "2025-01-10",
         "Beauty",
         45,
         41,
         2352.26,
         0.0,
         0.0444
        ],
        [
         "2025-01-10",
         "Sports",
         23,
         21,
         1104.06,
         0.0,
         0.0435
        ],
        [
         "2025-01-09",
         "Grocery",
         45,
         129,
         6828.23,
         0.1,
         0.0222
        ],
        [
         "2025-01-09",
         "Beauty",
         45,
         129,
         6609.53,
         0.1,
         0.0222
        ],
        [
         "2025-01-09",
         "Home",
         46,
         132,
         6544.54,
         0.1,
         0.0217
        ],
        [
         "2025-01-09",
         "Apparel",
         45,
         123,
         6411.46,
         0.1,
         0.0444
        ],
        [
         "2025-01-09",
         "Electronics",
         45,
         123,
         5864.71,
         0.1,
         0.0444
        ],
        [
         "2025-01-09",
         "Sports",
         23,
         63,
         3446.61,
         0.1,
         0.0435
        ],
        [
         "2025-01-08",
         "Electronics",
         45,
         86,
         4935.0,
         0.0,
         0.0222
        ],
        [
         "2025-01-08",
         "Home",
         45,
         86,
         4854.0,
         0.0,
         0.0222
        ],
        [
         "2025-01-08",
         "Grocery",
         46,
         84,
         4500.0,
         0.0,
         0.0435
        ],
        [
         "2025-01-08",
         "Beauty",
         45,
         82,
         4470.0,
         0.0,
         0.0444
        ],
        [
         "2025-01-08",
         "Apparel",
         45,
         82,
         4227.0,
         0.0,
         0.0444
        ],
        [
         "2025-01-08",
         "Sports",
         23,
         46,
         2634.0,
         0.0,
         0.0
        ],
        [
         "2025-01-07",
         "Beauty",
         46,
         42,
         2351.94,
         0.0,
         0.0435
        ],
        [
         "2025-01-07",
         "Electronics",
         45,
         43,
         2284.51,
         0.0,
         0.0222
        ],
        [
         "2025-01-07",
         "Grocery",
         44,
         42,
         2270.94,
         0.0,
         0.0227
        ],
        [
         "2025-01-07",
         "Apparel",
         45,
         41,
         2257.37,
         0.0,
         0.0444
        ],
        [
         "2025-01-07",
         "Home",
         45,
         41,
         2135.87,
         0.0,
         0.0444
        ],
        [
         "2025-01-07",
         "Sports",
         23,
         21,
         1094.97,
         0.0,
         0.0435
        ],
        [
         "2025-01-06",
         "Grocery",
         45,
         129,
         7155.06,
         0.0,
         0.0222
        ],
        [
         "2025-01-06",
         "Home",
         45,
         129,
         6912.06,
         0.0,
         0.0222
        ],
        [
         "2025-01-06",
         "Beauty",
         46,
         126,
         6511.14,
         0.0,
         0.0435
        ],
        [
         "2025-01-06",
         "Electronics",
         45,
         123,
         6474.72,
         0.0,
         0.0444
        ],
        [
         "2025-01-06",
         "Apparel",
         46,
         126,
         6389.64,
         0.0,
         0.0435
        ],
        [
         "2025-01-06",
         "Sports",
         23,
         63,
         3073.32,
         0.0,
         0.0435
        ],
        [
         "2025-01-05",
         "Home",
         46,
         132,
         6302.58,
         0.1,
         0.0217
        ],
        [
         "2025-01-05",
         "Beauty",
         46,
         126,
         6135.39,
         0.1,
         0.0435
        ],
        [
         "2025-01-05",
         "Apparel",
         46,
         126,
         5916.69,
         0.1,
         0.0435
        ],
        [
         "2025-01-05",
         "Electronics",
         45,
         129,
         5836.26,
         0.1,
         0.0222
        ],
        [
         "2025-01-05",
         "Grocery",
         44,
         120,
         4984.05,
         0.1,
         0.0455
        ],
        [
         "2025-01-05",
         "Sports",
         22,
         66,
         3041.94,
         0.1,
         0.0
        ],
        [
         "2025-01-04",
         "Grocery",
         45,
         86,
         4524.08,
         0.0,
         0.0222
        ],
        [
         "2025-01-04",
         "Electronics",
         46,
         88,
         4380.64,
         0.0,
         0.0217
        ],
        [
         "2025-01-04",
         "Apparel",
         45,
         86,
         4200.08,
         0.0,
         0.0222
        ],
        [
         "2025-01-04",
         "Beauty",
         45,
         82,
         4000.96,
         0.0,
         0.0444
        ],
        [
         "2025-01-04",
         "Home",
         46,
         84,
         3938.52,
         0.0,
         0.0435
        ],
        [
         "2025-01-04",
         "Sports",
         22,
         40,
         2072.2,
         0.0,
         0.0455
        ],
        [
         "2025-01-03",
         "Home",
         46,
         132,
         6746.7,
         0.0,
         0.0217
        ],
        [
         "2025-01-03",
         "Apparel",
         45,
         123,
         6068.55,
         0.0,
         0.0444
        ],
        [
         "2025-01-03",
         "Grocery",
         45,
         129,
         5994.15,
         0.0,
         0.0222
        ],
        [
         "2025-01-03",
         "Electronics",
         46,
         126,
         5970.6,
         0.0,
         0.0435
        ],
        [
         "2025-01-03",
         "Beauty",
         45,
         123,
         5582.55,
         0.0,
         0.0444
        ],
        [
         "2025-01-03",
         "Sports",
         22,
         60,
         3022.5,
         0.0,
         0.0455
        ],
        [
         "2025-01-02",
         "Electronics",
         46,
         88,
         4209.96,
         0.0,
         0.0217
        ],
        [
         "2025-01-02",
         "Grocery",
         46,
         84,
         4022.28,
         0.0,
         0.0435
        ],
        [
         "2025-01-02",
         "Beauty",
         45,
         82,
         4009.44,
         0.0,
         0.0444
        ],
        [
         "2025-01-02",
         "Apparel",
         45,
         82,
         3766.44,
         0.0,
         0.0444
        ],
        [
         "2025-01-02",
         "Home",
         44,
         84,
         3617.28,
         0.0,
         0.0227
        ],
        [
         "2025-01-02",
         "Sports",
         23,
         42,
         1889.64,
         0.0,
         0.0435
        ],
        [
         "2025-01-01",
         "Apparel",
         45,
         43,
         1629.57,
         0.2,
         0.0222
        ],
        [
         "2025-01-01",
         "Electronics",
         45,
         43,
         1597.17,
         0.2,
         0.0222
        ],
        [
         "2025-01-01",
         "Grocery",
         45,
         43,
         1564.77,
         0.2,
         0.0222
        ],
        [
         "2025-01-01",
         "Home",
         45,
         41,
         1524.39,
         0.2,
         0.0444
        ],
        [
         "2025-01-01",
         "Beauty",
         46,
         42,
         1398.78,
         0.2,
         0.0435
        ],
        [
         "2025-01-01",
         "Sports",
         23,
         21,
         699.39,
         0.2,
         0.0435
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "event_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "category",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "transactions",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "net_units",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "net_revenue",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "avg_discount_pct",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "return_rate",
         "type": "\"double\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "# This small cache avoids repeatedly scanning silver during the gold aggregations and validations.\n",
    "silver_table = spark.table(table(\"silver_retail_events\"))\n",
    "\n",
    "daily_metrics = (\n",
    "    silver_table.groupBy(\"event_date\")\n",
    "    .agg(\n",
    "        F.countDistinct(\"event_id\").alias(\"transactions\"),\n",
    "        F.countDistinct(\"customer_id\").alias(\"unique_customers\"),\n",
    "        F.sum(\"signed_units\").alias(\"net_units\"),\n",
    "        F.round(F.sum(\"net_revenue\"), 2).alias(\"net_revenue\"),\n",
    "        F.round(F.avg(F.when(F.col(\"order_status\") == \"COMPLETED\", F.col(\"net_revenue\"))), 2).alias(\"avg_completed_order_value\"),\n",
    "        F.round(F.avg(F.when(F.col(\"order_status\") == \"RETURNED\", 1.0).otherwise(0.0)), 4).alias(\"return_rate\"),\n",
    "    )\n",
    ")\n",
    "\n",
    "category_metrics = (\n",
    "    silver_table.groupBy(\"event_date\", \"category\")\n",
    "    .agg(\n",
    "        F.countDistinct(\"event_id\").alias(\"transactions\"),\n",
    "        F.sum(\"signed_units\").alias(\"net_units\"),\n",
    "        F.round(F.sum(\"net_revenue\"), 2).alias(\"net_revenue\"),\n",
    "        F.round(F.avg(\"discount_pct\"), 4).alias(\"avg_discount_pct\"),\n",
    "        F.round(F.avg(F.when(F.col(\"order_status\") == \"RETURNED\", 1.0).otherwise(0.0)), 4).alias(\"return_rate\"),\n",
    "    )\n",
    ")\n",
    "\n",
    "store_channel_metrics = (\n",
    "    silver_table.groupBy(\"event_date\", \"region\", \"store_id\", \"channel\")\n",
    "    .agg(\n",
    "        F.countDistinct(\"event_id\").alias(\"transactions\"),\n",
    "        F.sum(\"signed_units\").alias(\"net_units\"),\n",
    "        F.round(F.sum(\"net_revenue\"), 2).alias(\"net_revenue\"),\n",
    "    )\n",
    ")\n",
    "\n",
    "gold_outputs = {\n",
    "    \"gold_daily_metrics\": daily_metrics,\n",
    "    \"gold_daily_category_metrics\": category_metrics,\n",
    "    \"gold_daily_store_channel_metrics\": store_channel_metrics,\n",
    "}\n",
    "for name, frame in gold_outputs.items():\n",
    "    (frame.write.format(\"delta\").mode(\"overwrite\").option(\"overwriteSchema\", \"true\").saveAsTable(table(name)))\n",
    "\n",
    "spark.sql(f\"COMMENT ON TABLE {namespace}.`gold_daily_metrics` IS 'Daily retail KPIs and net revenue used to train the forecast'\")\n",
    "spark.sql(f\"COMMENT ON TABLE {namespace}.`gold_daily_category_metrics` IS 'Daily category performance for trend and mix analysis'\")\n",
    "spark.sql(f\"COMMENT ON TABLE {namespace}.`gold_daily_store_channel_metrics` IS 'Daily store, region, and channel performance for drilldown analysis'\")\n",
    "\n",
    "display(category_metrics.orderBy(F.desc(\"event_date\"), F.desc(\"net_revenue\")))"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "9c700563-a5e0-4495-be17-aa4f91833e3d",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## 5. Forecast total daily net revenue\n",
    "\n",
    "**Forecast target:** the `net_revenue` KPI in `gold_daily_metrics`, for each of the next 14 days\n",
    "(or the widget-selected horizon). A Spark ML linear regression learns a time trend plus weekly\n",
    "seasonality encoded with sine/cosine features. This is intentionally simple, fast, and explainable;\n",
    "it is a meetup baseline rather than a production forecasting system."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1787269014579,
     "inputWidgets": {},
     "nuid": "7e0741f3-72f3-4f1a-a6e1-ece0932f77fd",
     "showTitle": false,
     "startTime": 1787269001031,
     "submitTime": 1787269000897,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Forecasting total daily net revenue from 2025-04-30 through the next 14 days.\nTraining R-squared: 0.324; training RMSE: 16,583.63\n"
     ]
    },
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>forecast_date</th><th>forecast_net_revenue</th><th>model_name</th><th>trained_through_date</th><th>forecast_generated_at</th></tr></thead><tbody><tr><td>2025-05-01</td><td>53617.28</td><td>Spark ML linear trend + weekly seasonality</td><td>2025-04-30</td><td>2026-08-20T23:36:51.953Z</td></tr><tr><td>2025-05-02</td><td>60879.45</td><td>Spark ML linear trend + weekly seasonality</td><td>2025-04-30</td><td>2026-08-20T23:36:51.953Z</td></tr><tr><td>2025-05-03</td><td>66710.28</td><td>Spark ML linear trend + weekly seasonality</td><td>2025-04-30</td><td>2026-08-20T23:36:51.953Z</td></tr><tr><td>2025-05-04</td><td>66933.86</td><td>Spark ML linear trend + weekly seasonality</td><td>2025-04-30</td><td>2026-08-20T23:36:51.953Z</td></tr><tr><td>2025-05-05</td><td>61596.64</td><td>Spark ML linear trend + weekly seasonality</td><td>2025-04-30</td><td>2026-08-20T23:36:51.953Z</td></tr><tr><td>2025-05-06</td><td>54932.48</td><td>Spark ML linear trend + weekly seasonality</td><td>2025-04-30</td><td>2026-08-20T23:36:51.953Z</td></tr><tr><td>2025-05-07</td><td>52174.44</td><td>Spark ML linear trend + weekly seasonality</td><td>2025-04-30</td><td>2026-08-20T23:36:51.953Z</td></tr><tr><td>2025-05-08</td><td>55614.2</td><td>Spark ML linear trend + weekly seasonality</td><td>2025-04-30</td><td>2026-08-20T23:36:51.953Z</td></tr><tr><td>2025-05-09</td><td>62876.36</td><td>Spark ML linear trend + weekly seasonality</td><td>2025-04-30</td><td>2026-08-20T23:36:51.953Z</td></tr><tr><td>2025-05-10</td><td>68707.19</td><td>Spark ML linear trend + weekly seasonality</td><td>2025-04-30</td><td>2026-08-20T23:36:51.953Z</td></tr><tr><td>2025-05-11</td><td>68930.77</td><td>Spark ML linear trend + weekly seasonality</td><td>2025-04-30</td><td>2026-08-20T23:36:51.953Z</td></tr><tr><td>2025-05-12</td><td>63593.56</td><td>Spark ML linear trend + weekly seasonality</td><td>2025-04-30</td><td>2026-08-20T23:36:51.953Z</td></tr><tr><td>2025-05-13</td><td>56929.4</td><td>Spark ML linear trend + weekly seasonality</td><td>2025-04-30</td><td>2026-08-20T23:36:51.953Z</td></tr><tr><td>2025-05-14</td><td>54171.35</td><td>Spark ML linear trend + weekly seasonality</td><td>2025-04-30</td><td>2026-08-20T23:36:51.953Z</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "2025-05-01",
         53617.28,
         "Spark ML linear trend + weekly seasonality",
         "2025-04-30",
         "2026-08-20T23:36:51.953Z"
        ],
        [
         "2025-05-02",
         60879.45,
         "Spark ML linear trend + weekly seasonality",
         "2025-04-30",
         "2026-08-20T23:36:51.953Z"
        ],
        [
         "2025-05-03",
         66710.28,
         "Spark ML linear trend + weekly seasonality",
         "2025-04-30",
         "2026-08-20T23:36:51.953Z"
        ],
        [
         "2025-05-04",
         66933.86,
         "Spark ML linear trend + weekly seasonality",
         "2025-04-30",
         "2026-08-20T23:36:51.953Z"
        ],
        [
         "2025-05-05",
         61596.64,
         "Spark ML linear trend + weekly seasonality",
         "2025-04-30",
         "2026-08-20T23:36:51.953Z"
        ],
        [
         "2025-05-06",
         54932.48,
         "Spark ML linear trend + weekly seasonality",
         "2025-04-30",
         "2026-08-20T23:36:51.953Z"
        ],
        [
         "2025-05-07",
         52174.44,
         "Spark ML linear trend + weekly seasonality",
         "2025-04-30",
         "2026-08-20T23:36:51.953Z"
        ],
        [
         "2025-05-08",
         55614.2,
         "Spark ML linear trend + weekly seasonality",
         "2025-04-30",
         "2026-08-20T23:36:51.953Z"
        ],
        [
         "2025-05-09",
         62876.36,
         "Spark ML linear trend + weekly seasonality",
         "2025-04-30",
         "2026-08-20T23:36:51.953Z"
        ],
        [
         "2025-05-10",
         68707.19,
         "Spark ML linear trend + weekly seasonality",
         "2025-04-30",
         "2026-08-20T23:36:51.953Z"
        ],
        [
         "2025-05-11",
         68930.77,
         "Spark ML linear trend + weekly seasonality",
         "2025-04-30",
         "2026-08-20T23:36:51.953Z"
        ],
        [
         "2025-05-12",
         63593.56,
         "Spark ML linear trend + weekly seasonality",
         "2025-04-30",
         "2026-08-20T23:36:51.953Z"
        ],
        [
         "2025-05-13",
         56929.4,
         "Spark ML linear trend + weekly seasonality",
         "2025-04-30",
         "2026-08-20T23:36:51.953Z"
        ],
        [
         "2025-05-14",
         54171.35,
         "Spark ML linear trend + weekly seasonality",
         "2025-04-30",
         "2026-08-20T23:36:51.953Z"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "forecast_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "forecast_net_revenue",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "model_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "trained_through_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "forecast_generated_at",
         "type": "\"timestamp\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "history = spark.table(table(\"gold_daily_metrics\")).select(\"event_date\", \"net_revenue\")\n",
    "date_bounds = history.agg(F.min(\"event_date\").alias(\"min_date\"), F.max(\"event_date\").alias(\"max_date\")).first()\n",
    "min_date, max_date = date_bounds[\"min_date\"], date_bounds[\"max_date\"]\n",
    "\n",
    "training = (\n",
    "    history\n",
    "    .withColumn(\"day_index\", F.datediff(\"event_date\", F.lit(min_date)).cast(\"double\"))\n",
    "    .withColumn(\"day_of_week\", F.dayofweek(\"event_date\").cast(\"double\"))\n",
    "    .withColumn(\"dow_sin\", F.sin(F.lit(2.0 * 3.141592653589793) * F.col(\"day_of_week\") / F.lit(7.0)))\n",
    "    .withColumn(\"dow_cos\", F.cos(F.lit(2.0 * 3.141592653589793) * F.col(\"day_of_week\") / F.lit(7.0)))\n",
    ")\n",
    "\n",
    "assembler = VectorAssembler(inputCols=[\"day_index\", \"dow_sin\", \"dow_cos\"], outputCol=\"features\")\n",
    "training_features = assembler.transform(training)\n",
    "model = LinearRegression(featuresCol=\"features\", labelCol=\"net_revenue\", regParam=0.1, elasticNetParam=0.0).fit(training_features)\n",
    "\n",
    "future = (\n",
    "    spark.range(1, forecast_days + 1)\n",
    "    .select(F.date_add(F.lit(max_date), F.col(\"id\").cast(\"int\")).alias(\"forecast_date\"))\n",
    "    .withColumn(\"day_index\", F.datediff(\"forecast_date\", F.lit(min_date)).cast(\"double\"))\n",
    "    .withColumn(\"day_of_week\", F.dayofweek(\"forecast_date\").cast(\"double\"))\n",
    "    .withColumn(\"dow_sin\", F.sin(F.lit(2.0 * 3.141592653589793) * F.col(\"day_of_week\") / F.lit(7.0)))\n",
    "    .withColumn(\"dow_cos\", F.cos(F.lit(2.0 * 3.141592653589793) * F.col(\"day_of_week\") / F.lit(7.0)))\n",
    ")\n",
    "\n",
    "forecast = (\n",
    "    model.transform(assembler.transform(future))\n",
    "    .select(\n",
    "        \"forecast_date\",\n",
    "        F.round(F.greatest(F.col(\"prediction\"), F.lit(0.0)), 2).alias(\"forecast_net_revenue\"),\n",
    "        F.lit(\"Spark ML linear trend + weekly seasonality\").alias(\"model_name\"),\n",
    "        F.lit(max_date).cast(\"date\").alias(\"trained_through_date\"),\n",
    "        F.current_timestamp().alias(\"forecast_generated_at\"),\n",
    "    )\n",
    ")\n",
    "\n",
    "(forecast.write.format(\"delta\").mode(\"overwrite\").option(\"overwriteSchema\", \"true\").saveAsTable(table(\"gold_daily_revenue_forecast\")))\n",
    "spark.sql(f\"COMMENT ON TABLE {namespace}.`gold_daily_revenue_forecast` IS 'Forecast of total daily net revenue after the final observed retail date'\")\n",
    "\n",
    "print(f\"Forecasting total daily net revenue from {max_date} through the next {forecast_days} days.\")\n",
    "print(f\"Training R-squared: {model.summary.r2:.3f}; training RMSE: {model.summary.rootMeanSquaredError:,.2f}\")\n",
    "display(forecast.orderBy(\"forecast_date\"))"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "7182e682-2ad5-4387-b3bb-5a37107cd690",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## 6. Persist and enforce validation checks\n",
    "\n",
    "Validation results are themselves a gold managed table, making data health visible to Genie and\n",
    "dashboards. Any failed required check stops the notebook after the evidence has been persisted."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1787269026791,
     "inputWidgets": {},
     "nuid": "c1c6cfea-df12-4780-8987-ccd3b3815774",
     "showTitle": false,
     "startTime": 1787269014653,
     "submitTime": 1787269000920,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>check_name</th><th>layer</th><th>status</th><th>actual</th><th>expected</th><th>description</th><th>checked_at</th></tr></thead><tbody><tr><td>bronze_has_expected_volume</td><td>bronze</td><td>PASS</td><td>30150</td><td>>= 30000</td><td>Bronze retained the generated event volume</td><td>2026-08-20T23:37:05.904Z</td></tr><tr><td>silver_is_not_empty</td><td>silver</td><td>PASS</td><td>29871</td><td>> 0</td><td>Cleaned data is available for analytics</td><td>2026-08-20T23:37:05.904Z</td></tr><tr><td>silver_event_ids_are_unique</td><td>silver</td><td>PASS</td><td>rows=29871, distinct=29871</td><td>equal</td><td>Duplicate business keys were removed</td><td>2026-08-20T23:37:05.904Z</td></tr><tr><td>silver_business_rules_hold</td><td>silver</td><td>PASS</td><td>0</td><td>0</td><td>Quantities/prices are positive and statuses are recognized</td><td>2026-08-20T23:37:05.904Z</td></tr><tr><td>gold_daily_revenue_reconciles</td><td>gold</td><td>PASS</td><td>gold=4963888.5, silver=4963888.5</td><td>difference < 0.01</td><td>Gold total reconciles to silver</td><td>2026-08-20T23:37:05.904Z</td></tr><tr><td>gold_category_metrics_not_empty</td><td>gold</td><td>PASS</td><td>720</td><td>> 0</td><td>Category metrics are available for reporting</td><td>2026-08-20T23:37:05.904Z</td></tr><tr><td>gold_category_revenue_reconciles</td><td>gold</td><td>PASS</td><td>category=4963888.5, silver=4963888.5</td><td>difference < 0.01</td><td>Category revenue reconciles to silver</td><td>2026-08-20T23:37:05.904Z</td></tr><tr><td>gold_store_channel_metrics_not_empty</td><td>gold</td><td>PASS</td><td>1440</td><td>> 0</td><td>Store and channel metrics are available for reporting</td><td>2026-08-20T23:37:05.904Z</td></tr><tr><td>gold_store_channel_revenue_reconciles</td><td>gold</td><td>PASS</td><td>store_channel=4963888.5, silver=4963888.5</td><td>difference < 0.01</td><td>Store/channel revenue reconciles to silver</td><td>2026-08-20T23:37:05.904Z</td></tr><tr><td>forecast_has_requested_horizon</td><td>forecast</td><td>PASS</td><td>14</td><td>14</td><td>One forecast row exists per future day</td><td>2026-08-20T23:37:05.904Z</td></tr><tr><td>forecast_is_non_negative</td><td>forecast</td><td>PASS</td><td>0</td><td>0</td><td>Revenue forecasts are bounded at zero</td><td>2026-08-20T23:37:05.904Z</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "bronze_has_expected_volume",
         "bronze",
         "PASS",
         "30150",
         ">= 30000",
         "Bronze retained the generated event volume",
         "2026-08-20T23:37:05.904Z"
        ],
        [
         "silver_is_not_empty",
         "silver",
         "PASS",
         "29871",
         "> 0",
         "Cleaned data is available for analytics",
         "2026-08-20T23:37:05.904Z"
        ],
        [
         "silver_event_ids_are_unique",
         "silver",
         "PASS",
         "rows=29871, distinct=29871",
         "equal",
         "Duplicate business keys were removed",
         "2026-08-20T23:37:05.904Z"
        ],
        [
         "silver_business_rules_hold",
         "silver",
         "PASS",
         "0",
         "0",
         "Quantities/prices are positive and statuses are recognized",
         "2026-08-20T23:37:05.904Z"
        ],
        [
         "gold_daily_revenue_reconciles",
         "gold",
         "PASS",
         "gold=4963888.5, silver=4963888.5",
         "difference < 0.01",
         "Gold total reconciles to silver",
         "2026-08-20T23:37:05.904Z"
        ],
        [
         "gold_category_metrics_not_empty",
         "gold",
         "PASS",
         "720",
         "> 0",
         "Category metrics are available for reporting",
         "2026-08-20T23:37:05.904Z"
        ],
        [
         "gold_category_revenue_reconciles",
         "gold",
         "PASS",
         "category=4963888.5, silver=4963888.5",
         "difference < 0.01",
         "Category revenue reconciles to silver",
         "2026-08-20T23:37:05.904Z"
        ],
        [
         "gold_store_channel_metrics_not_empty",
         "gold",
         "PASS",
         "1440",
         "> 0",
         "Store and channel metrics are available for reporting",
         "2026-08-20T23:37:05.904Z"
        ],
        [
         "gold_store_channel_revenue_reconciles",
         "gold",
         "PASS",
         "store_channel=4963888.5, silver=4963888.5",
         "difference < 0.01",
         "Store/channel revenue reconciles to silver",
         "2026-08-20T23:37:05.904Z"
        ],
        [
         "forecast_has_requested_horizon",
         "forecast",
         "PASS",
         "14",
         "14",
         "One forecast row exists per future day",
         "2026-08-20T23:37:05.904Z"
        ],
        [
         "forecast_is_non_negative",
         "forecast",
         "PASS",
         "0",
         "0",
         "Revenue forecasts are bounded at zero",
         "2026-08-20T23:37:05.904Z"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "check_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "layer",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "actual",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "expected",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "description",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "checked_at",
         "type": "\"timestamp\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "bronze_count = spark.table(table(\"bronze_retail_events\")).count()\n",
    "silver_count = silver_table.count()\n",
    "silver_distinct_ids = silver_table.select(\"event_id\").distinct().count()\n",
    "invalid_silver_count = silver_table.where(\n",
    "    (F.col(\"quantity\") <= 0)\n",
    "    | (F.col(\"unit_price\") <= 0)\n",
    "    | (~F.col(\"order_status\").isin(\"COMPLETED\", \"RETURNED\"))\n",
    ").count()\n",
    "daily_revenue_total = daily_metrics.agg(F.round(F.sum(\"net_revenue\"), 2)).first()[0]\n",
    "category_count = category_metrics.count()\n",
    "category_revenue_total = category_metrics.agg(F.round(F.sum(\"net_revenue\"), 2)).first()[0]\n",
    "store_channel_count = store_channel_metrics.count()\n",
    "store_channel_revenue_total = store_channel_metrics.agg(F.round(F.sum(\"net_revenue\"), 2)).first()[0]\n",
    "silver_revenue_total = silver_table.agg(F.round(F.sum(\"net_revenue\"), 2)).first()[0]\n",
    "forecast_count = forecast.count()\n",
    "negative_forecasts = forecast.where(F.col(\"forecast_net_revenue\") < 0).count()\n",
    "\n",
    "checks = [\n",
    "    (\"bronze_has_expected_volume\", \"bronze\", \"PASS\" if bronze_count >= event_count else \"FAIL\", str(bronze_count), f\">= {event_count}\", \"Bronze retained the generated event volume\"),\n",
    "    (\"silver_is_not_empty\", \"silver\", \"PASS\" if silver_count > 0 else \"FAIL\", str(silver_count), \"> 0\", \"Cleaned data is available for analytics\"),\n",
    "    (\"silver_event_ids_are_unique\", \"silver\", \"PASS\" if silver_count == silver_distinct_ids else \"FAIL\", f\"rows={silver_count}, distinct={silver_distinct_ids}\", \"equal\", \"Duplicate business keys were removed\"),\n",
    "    (\"silver_business_rules_hold\", \"silver\", \"PASS\" if invalid_silver_count == 0 else \"FAIL\", str(invalid_silver_count), \"0\", \"Quantities/prices are positive and statuses are recognized\"),\n",
    "    (\"gold_daily_revenue_reconciles\", \"gold\", \"PASS\" if abs(float(daily_revenue_total) - float(silver_revenue_total)) < 0.01 else \"FAIL\", f\"gold={daily_revenue_total}, silver={silver_revenue_total}\", \"difference < 0.01\", \"Gold total reconciles to silver\"),\n",
    "    (\"gold_category_metrics_not_empty\", \"gold\", \"PASS\" if category_count > 0 else \"FAIL\", str(category_count), \"> 0\", \"Category metrics are available for reporting\"),\n",
    "    (\"gold_category_revenue_reconciles\", \"gold\", \"PASS\" if abs(float(category_revenue_total) - float(silver_revenue_total)) < 0.01 else \"FAIL\", f\"category={category_revenue_total}, silver={silver_revenue_total}\", \"difference < 0.01\", \"Category revenue reconciles to silver\"),\n",
    "    (\"gold_store_channel_metrics_not_empty\", \"gold\", \"PASS\" if store_channel_count > 0 else \"FAIL\", str(store_channel_count), \"> 0\", \"Store and channel metrics are available for reporting\"),\n",
    "    (\"gold_store_channel_revenue_reconciles\", \"gold\", \"PASS\" if abs(float(store_channel_revenue_total) - float(silver_revenue_total)) < 0.01 else \"FAIL\", f\"store_channel={store_channel_revenue_total}, silver={silver_revenue_total}\", \"difference < 0.01\", \"Store/channel revenue reconciles to silver\"),\n",
    "    (\"forecast_has_requested_horizon\", \"forecast\", \"PASS\" if forecast_count == forecast_days else \"FAIL\", str(forecast_count), str(forecast_days), \"One forecast row exists per future day\"),\n",
    "    (\"forecast_is_non_negative\", \"forecast\", \"PASS\" if negative_forecasts == 0 else \"FAIL\", str(negative_forecasts), \"0\", \"Revenue forecasts are bounded at zero\"),\n",
    "]\n",
    "\n",
    "validation_schema = T.StructType([\n",
    "    T.StructField(\"check_name\", T.StringType(), False),\n",
    "    T.StructField(\"layer\", T.StringType(), False),\n",
    "    T.StructField(\"status\", T.StringType(), False),\n",
    "    T.StructField(\"actual\", T.StringType(), False),\n",
    "    T.StructField(\"expected\", T.StringType(), False),\n",
    "    T.StructField(\"description\", T.StringType(), False),\n",
    "])\n",
    "validation_results = spark.createDataFrame(checks, validation_schema).withColumn(\"checked_at\", F.current_timestamp())\n",
    "(validation_results.write.format(\"delta\").mode(\"overwrite\").option(\"overwriteSchema\", \"true\").saveAsTable(table(\"gold_validation_results\")))\n",
    "spark.sql(f\"COMMENT ON TABLE {namespace}.`gold_validation_results` IS 'Latest quality and reconciliation results for the retail demo pipeline'\")\n",
    "display(validation_results)\n",
    "\n",
    "failed_checks = validation_results.where(F.col(\"status\") == \"FAIL\").count()\n",
    "if failed_checks:\n",
    "    raise RuntimeError(f\"Retail pipeline failed {failed_checks} validation check(s). Review {table('gold_validation_results')}.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "239d5b10-eb0b-4035-97bf-ecdc3111d97b",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## 7. Final implementation summary\n",
    "\n",
    "The summary inventory is persisted for operational discovery. Row counts are intentionally\n",
    "collected only at the end because this demo's datasets are small."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1787269036008,
     "inputWidgets": {},
     "nuid": "0feda797-ba64-4c96-9a9e-af20a8df0ed2",
     "showTitle": false,
     "startTime": 1787269026810,
     "submitTime": 1787269000943,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>phase</th><th>full_table_name</th><th>row_count</th><th>purpose</th><th>completed_at</th></tr></thead><tbody><tr><td>Bronze</td><td>workspace.retail_meetup_demo.bronze_retail_events</td><td>30150</td><td>Raw point-of-sale events plus ingestion metadata</td><td>2026-08-20T23:37:15.081Z</td></tr><tr><td>Gold</td><td>workspace.retail_meetup_demo.gold_daily_category_metrics</td><td>720</td><td>Daily category trend and product-mix metrics</td><td>2026-08-20T23:37:15.081Z</td></tr><tr><td>Gold</td><td>workspace.retail_meetup_demo.gold_daily_metrics</td><td>120</td><td>Daily executive retail KPIs and forecast training target</td><td>2026-08-20T23:37:15.081Z</td></tr><tr><td>Gold</td><td>workspace.retail_meetup_demo.gold_daily_revenue_forecast</td><td>14</td><td>Future total daily net-revenue predictions</td><td>2026-08-20T23:37:15.081Z</td></tr><tr><td>Gold</td><td>workspace.retail_meetup_demo.gold_daily_store_channel_metrics</td><td>1440</td><td>Daily region, store, and channel drilldown metrics</td><td>2026-08-20T23:37:15.081Z</td></tr><tr><td>Gold</td><td>workspace.retail_meetup_demo.gold_validation_results</td><td>11</td><td>Persisted data-quality and reconciliation checks</td><td>2026-08-20T23:37:15.081Z</td></tr><tr><td>Silver</td><td>workspace.retail_meetup_demo.silver_retail_events</td><td>29871</td><td>Validated, deduplicated, revenue-enriched events</td><td>2026-08-20T23:37:15.081Z</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "Bronze",
         "workspace.retail_meetup_demo.bronze_retail_events",
         30150,
         "Raw point-of-sale events plus ingestion metadata",
         "2026-08-20T23:37:15.081Z"
        ],
        [
         "Gold",
         "workspace.retail_meetup_demo.gold_daily_category_metrics",
         720,
         "Daily category trend and product-mix metrics",
         "2026-08-20T23:37:15.081Z"
        ],
        [
         "Gold",
         "workspace.retail_meetup_demo.gold_daily_metrics",
         120,
         "Daily executive retail KPIs and forecast training target",
         "2026-08-20T23:37:15.081Z"
        ],
        [
         "Gold",
         "workspace.retail_meetup_demo.gold_daily_revenue_forecast",
         14,
         "Future total daily net-revenue predictions",
         "2026-08-20T23:37:15.081Z"
        ],
        [
         "Gold",
         "workspace.retail_meetup_demo.gold_daily_store_channel_metrics",
         1440,
         "Daily region, store, and channel drilldown metrics",
         "2026-08-20T23:37:15.081Z"
        ],
        [
         "Gold",
         "workspace.retail_meetup_demo.gold_validation_results",
         11,
         "Persisted data-quality and reconciliation checks",
         "2026-08-20T23:37:15.081Z"
        ],
        [
         "Silver",
         "workspace.retail_meetup_demo.silver_retail_events",
         29871,
         "Validated, deduplicated, revenue-enriched events",
         "2026-08-20T23:37:15.081Z"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "phase",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "full_table_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "row_count",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "purpose",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "completed_at",
         "type": "\"timestamp\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
    {
     "output_type": "display_data",
     "data": {
      "text/plain": [
       "\u001B[0;31m---------------------------------------------------------------------------\u001B[0m\n",
       "\u001B[0;31mAnalysisException\u001B[0m                         Traceback (most recent call last)\n",
       "File \u001B[0;32m<command-8820324460378679>, line 26\u001B[0m\n",
       "\u001B[1;32m     23\u001B[0m spark\u001B[38;5;241m.\u001B[39msql(\u001B[38;5;124mf\u001B[39m\u001B[38;5;124m\"\u001B[39m\u001B[38;5;124mCOMMENT ON TABLE \u001B[39m\u001B[38;5;132;01m{\u001B[39;00mnamespace\u001B[38;5;132;01m}\u001B[39;00m\u001B[38;5;124m.`gold_implementation_summary` IS \u001B[39m\u001B[38;5;124m'\u001B[39m\u001B[38;5;124mInventory and row counts for all retail demo outputs\u001B[39m\u001B[38;5;124m'\u001B[39m\u001B[38;5;124m\"\u001B[39m)\n",
       "\u001B[1;32m     25\u001B[0m display(implementation_summary\u001B[38;5;241m.\u001B[39morderBy(\u001B[38;5;124m\"\u001B[39m\u001B[38;5;124mphase\u001B[39m\u001B[38;5;124m\"\u001B[39m, \u001B[38;5;124m\"\u001B[39m\u001B[38;5;124mfull_table_name\u001B[39m\u001B[38;5;124m\"\u001B[39m))\n",
       "\u001B[0;32m---> 26\u001B[0m \u001B[43msilver_table\u001B[49m\u001B[38;5;241;43m.\u001B[39;49m\u001B[43munpersist\u001B[49m\u001B[43m(\u001B[49m\u001B[43m)\u001B[49m\n",
       "\u001B[1;32m     27\u001B[0m \u001B[38;5;28mprint\u001B[39m(\u001B[38;5;124m\"\u001B[39m\u001B[38;5;124mAll required validation checks passed. The retail lakehouse demo is ready for Genie.\u001B[39m\u001B[38;5;124m\"\u001B[39m)\n",
       "\n",
       "File \u001B[0;32m/databricks/python/lib/python3.12/site-packages/pyspark/sql/connect/dataframe.py:2156\u001B[0m, in \u001B[0;36mDataFrame.unpersist\u001B[0;34m(self, blocking)\u001B[0m\n",
       "\u001B[1;32m   2154\u001B[0m \u001B[38;5;28;01mdef\u001B[39;00m\u001B[38;5;250m \u001B[39m\u001B[38;5;21munpersist\u001B[39m(\u001B[38;5;28mself\u001B[39m, blocking: \u001B[38;5;28mbool\u001B[39m \u001B[38;5;241m=\u001B[39m \u001B[38;5;28;01mFalse\u001B[39;00m) \u001B[38;5;241m-\u001B[39m\u001B[38;5;241m>\u001B[39m ParentDataFrame:\n",
       "\u001B[1;32m   2155\u001B[0m     relation \u001B[38;5;241m=\u001B[39m \u001B[38;5;28mself\u001B[39m\u001B[38;5;241m.\u001B[39m_plan\u001B[38;5;241m.\u001B[39mplan(\u001B[38;5;28mself\u001B[39m\u001B[38;5;241m.\u001B[39m_session\u001B[38;5;241m.\u001B[39mclient)\n",
       "\u001B[0;32m-> 2156\u001B[0m     \u001B[38;5;28;43mself\u001B[39;49m\u001B[38;5;241;43m.\u001B[39;49m\u001B[43m_session\u001B[49m\u001B[38;5;241;43m.\u001B[39;49m\u001B[43mclient\u001B[49m\u001B[38;5;241;43m.\u001B[39;49m\u001B[43m_analyze\u001B[49m\u001B[43m(\u001B[49m\u001B[43mmethod\u001B[49m\u001B[38;5;241;43m=\u001B[39;49m\u001B[38;5;124;43m\"\u001B[39;49m\u001B[38;5;124;43munpersist\u001B[39;49m\u001B[38;5;124;43m\"\u001B[39;49m\u001B[43m,\u001B[49m\u001B[43m \u001B[49m\u001B[43mrelation\u001B[49m\u001B[38;5;241;43m=\u001B[39;49m\u001B[43mrelation\u001B[49m\u001B[43m,\u001B[49m\u001B[43m \u001B[49m\u001B[43mblocking\u001B[49m\u001B[38;5;241;43m=\u001B[39;49m\u001B[43mblocking\u001B[49m\u001B[43m)\u001B[49m\n",
       "\u001B[1;32m   2157\u001B[0m     \u001B[38;5;28;01mreturn\u001B[39;00m \u001B[38;5;28mself\u001B[39m\n",
       "\n",
       "File \u001B[0;32m/databricks/python/lib/python3.12/site-packages/pyspark/sql/connect/client/core.py:1808\u001B[0m, in \u001B[0;36mSparkConnectClient._analyze\u001B[0;34m(self, method, **kwargs)\u001B[0m\n",
       "\u001B[1;32m   1806\u001B[0m     \u001B[38;5;28;01mraise\u001B[39;00m SparkConnectException(\u001B[38;5;124m\"\u001B[39m\u001B[38;5;124mInvalid state during retry exception handling.\u001B[39m\u001B[38;5;124m\"\u001B[39m)\n",
       "\u001B[1;32m   1807\u001B[0m \u001B[38;5;28;01mexcept\u001B[39;00m \u001B[38;5;167;01mException\u001B[39;00m \u001B[38;5;28;01mas\u001B[39;00m error:\n",
       "\u001B[0;32m-> 1808\u001B[0m     \u001B[38;5;28;43mself\u001B[39;49m\u001B[38;5;241;43m.\u001B[39;49m\u001B[43m_handle_error\u001B[49m\u001B[43m(\u001B[49m\u001B[43merror\u001B[49m\u001B[43m)\u001B[49m\n",
       "\n",
       "File \u001B[0;32m/databricks/python/lib/python3.12/site-packages/pyspark/sql/connect/client/core.py:2380\u001B[0m, in \u001B[0;36mSparkConnectClient._handle_error\u001B[0;34m(self, error)\u001B[0m\n",
       "\u001B[1;32m   2378\u001B[0m     \u001B[38;5;28mself\u001B[39m\u001B[38;5;241m.\u001B[39mthread_local\u001B[38;5;241m.\u001B[39minside_error_handling \u001B[38;5;241m=\u001B[39m \u001B[38;5;28;01mTrue\u001B[39;00m\n",
       "\u001B[1;32m   2379\u001B[0m     \u001B[38;5;28;01mif\u001B[39;00m \u001B[38;5;28misinstance\u001B[39m(error, grpc\u001B[38;5;241m.\u001B[39mRpcError):\n",
       "\u001B[0;32m-> 2380\u001B[0m         \u001B[38;5;28;43mself\u001B[39;49m\u001B[38;5;241;43m.\u001B[39;49m\u001B[43m_handle_rpc_error\u001B[49m\u001B[43m(\u001B[49m\u001B[43merror\u001B[49m\u001B[43m)\u001B[49m\n",
       "\u001B[1;32m   2381\u001B[0m     \u001B[38;5;28;01mraise\u001B[39;00m error\n",
       "\u001B[1;32m   2382\u001B[0m \u001B[38;5;28;01mfinally\u001B[39;00m:\n",
       "\n",
       "File \u001B[0;32m/databricks/python/lib/python3.12/site-packages/pyspark/sql/connect/client/core.py:2458\u001B[0m, in \u001B[0;36mSparkConnectClient._handle_rpc_error\u001B[0;34m(self, rpc_error)\u001B[0m\n",
       "\u001B[1;32m   2454\u001B[0m             logger\u001B[38;5;241m.\u001B[39mdebug(\u001B[38;5;124mf\u001B[39m\u001B[38;5;124m\"\u001B[39m\u001B[38;5;124mReceived ErrorInfo: \u001B[39m\u001B[38;5;132;01m{\u001B[39;00minfo\u001B[38;5;132;01m}\u001B[39;00m\u001B[38;5;124m\"\u001B[39m)\n",
       "\u001B[1;32m   2456\u001B[0m             \u001B[38;5;28mself\u001B[39m\u001B[38;5;241m.\u001B[39m_handle_rpc_error_with_error_info(info, status\u001B[38;5;241m.\u001B[39mmessage, status_code)  \u001B[38;5;66;03m# EDGE\u001B[39;00m\n",
       "\u001B[0;32m-> 2458\u001B[0m             \u001B[38;5;28;01mraise\u001B[39;00m convert_exception(\n",
       "\u001B[1;32m   2459\u001B[0m                 info,\n",
       "\u001B[1;32m   2460\u001B[0m                 status\u001B[38;5;241m.\u001B[39mmessage,\n",
       "\u001B[1;32m   2461\u001B[0m                 \u001B[38;5;28mself\u001B[39m\u001B[38;5;241m.\u001B[39m_fetch_enriched_error(info),\n",
       "\u001B[1;32m   2462\u001B[0m                 \u001B[38;5;28mself\u001B[39m\u001B[38;5;241m.\u001B[39m_display_server_stack_trace(),\n",
       "\u001B[1;32m   2463\u001B[0m                 status_code,\n",
       "\u001B[1;32m   2464\u001B[0m             ) \u001B[38;5;28;01mfrom\u001B[39;00m\u001B[38;5;250m \u001B[39m\u001B[38;5;28;01mNone\u001B[39;00m\n",
       "\u001B[1;32m   2466\u001B[0m     \u001B[38;5;28;01mraise\u001B[39;00m SparkConnectGrpcException(\n",
       "\u001B[1;32m   2467\u001B[0m         message\u001B[38;5;241m=\u001B[39mstatus\u001B[38;5;241m.\u001B[39mmessage,\n",
       "\u001B[1;32m   2468\u001B[0m         sql_state\u001B[38;5;241m=\u001B[39mErrorCode\u001B[38;5;241m.\u001B[39mCLIENT_UNEXPECTED_MISSING_SQL_STATE,  \u001B[38;5;66;03m# EDGE\u001B[39;00m\n",
       "\u001B[1;32m   2469\u001B[0m         grpc_status_code\u001B[38;5;241m=\u001B[39mstatus_code,\n",
       "\u001B[1;32m   2470\u001B[0m     ) \u001B[38;5;28;01mfrom\u001B[39;00m\u001B[38;5;250m \u001B[39m\u001B[38;5;28;01mNone\u001B[39;00m\n",
       "\u001B[1;32m   2471\u001B[0m \u001B[38;5;28;01melse\u001B[39;00m:\n",
       "\n",
       "\u001B[0;31mAnalysisException\u001B[0m: [NOT_SUPPORTED_WITH_SERVERLESS] UNPERSIST TABLE is not supported on serverless compute. SQLSTATE: 0A000\n",
       "\n",
       "JVM stacktrace:\n",
       "org.apache.spark.sql.AnalysisException\n",
       "\tat com.databricks.serverless.ServerlessGCEdgeCheck$.throwError(ServerlessGCEdgeCheck.scala:72)\n",
       "\tat com.databricks.serverless.ServerlessGCEdgeCheck$.checkBlockCacheCommand(ServerlessGCEdgeCheck.scala:50)\n",
       "\tat org.apache.spark.sql.connect.service.SparkConnectAnalyzeHandler.process(SparkConnectAnalyzeHandler.scala:492)\n",
       "\tat org.apache.spark.sql.connect.service.SparkConnectAnalyzeHandler.$anonfun$handle$5(SparkConnectAnalyzeHandler.scala:96)\n",
       "\tat org.apache.spark.sql.connect.service.SparkConnectAnalyzeHandler.$anonfun$handle$5$adapted(SparkConnectAnalyzeHandler.scala:88)\n",
       "\tat org.apache.spark.sql.connect.service.SessionHolder.$anonfun$withSession$2(SessionHolder.scala:844)\n",
       "\tat org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:866)\n",
       "\tat org.apache.spark.sql.connect.service.SessionHolder.$anonfun$withSession$1(SessionHolder.scala:844)\n",
       "\tat org.apache.spark.JobArtifactSet$.withActiveJobArtifactState(JobArtifactSet.scala:97)\n",
       "\tat org.apache.spark.sql.artifact.ArtifactManager.$anonfun$withResources$1(ArtifactManager.scala:124)\n",
       "\tat org.apache.spark.sql.artifact.ArtifactManager.withClassLoaderIfNeeded(ArtifactManager.scala:118)\n",
       "\tat org.apache.spark.sql.artifact.ArtifactManager.withResources(ArtifactManager.scala:123)\n",
       "\tat org.apache.spark.sql.connect.service.SessionHolder.withSession(SessionHolder.scala:843)\n",
       "\tat org.apache.spark.sql.connect.service.SparkConnectAnalyzeHandler.$anonfun$handle$1(SparkConnectAnalyzeHandler.scala:88)\n",
       "\tat org.apache.spark.sql.connect.service.SparkConnectAnalyzeHandler.$anonfun$handle$1$adapted(SparkConnectAnalyzeHandler.scala:55)\n",
       "\tat com.databricks.spark.connect.logging.rpc.SparkConnectRpcMetricsCollectorUtils$.collectMetrics(SparkConnectRpcMetricsCollector.scala:294)\n",
       "\tat org.apache.spark.sql.connect.service.SparkConnectAnalyzeHandler.handle(SparkConnectAnalyzeHandler.scala:54)\n",
       "\tat org.apache.spark.sql.connect.service.SparkConnectService.analyzePlan(SparkConnectService.scala:117)\n",
       "\tat org.apache.spark.connect.proto.SparkConnectServiceGrpc$MethodHandlers.invoke(SparkConnectServiceGrpc.java:1008)\n",
       "\tat org.sparkproject.connect.io.grpc.stub.ServerCalls$UnaryServerCallHandler$UnaryServerCallListener.onHalfClose(ServerCalls.java:182)\n",
       "\tat org.sparkproject.connect.io.grpc.PartialForwardingServerCallListener.onHalfClose(PartialForwardingServerCallListener.java:35)\n",
       "\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:23)\n",
       "\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener$SimpleForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:40)\n",
       "\tat org.sparkproject.connect.io.grpc.PartialForwardingServerCallListener.onHalfClose(PartialForwardingServerCallListener.java:35)\n",
       "\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:23)\n",
       "\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener$SimpleForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:40)\n",
       "\tat org.sparkproject.connect.io.grpc.PartialForwardingServerCallListener.onHalfClose(PartialForwardingServerCallListener.java:35)\n",
       "\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:23)\n",
       "\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener$SimpleForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:40)\n",
       "\tat org.sparkproject.connect.io.grpc.Contexts$ContextualizedServerCallListener.onHalfClose(Contexts.java:86)\n",
       "\tat org.sparkproject.connect.io.grpc.PartialForwardingServerCallListener.onHalfClose(PartialForwardingServerCallListener.java:35)\n",
       "\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:23)\n",
       "\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener$SimpleForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:40)\n",
       "\tat com.databricks.spark.connect.service.AuthenticationInterceptor$AuthenticatedServerCallListener.$anonfun$onHalfClose$1(AuthenticationInterceptor.scala:528)\n",
       "\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n",
       "\tat com.databricks.unity.UCSEphemeralState$Handle.runWith(UCSEphemeralState.scala:51)\n",
       "\tat com.databricks.unity.HandleImpl.runWith(UCSHandle.scala:128)\n",
       "\tat com.databricks.spark.connect.service.RequestContext.$anonfun$runWith$4(RequestContext.scala:494)\n",
       "\tat com.databricks.util.TracingSpanUtils$.withSyncTracingAndParentFromHeaders(TracingSpanUtils.scala:456)\n",
       "\tat com.databricks.spark.util.DatabricksTracingHelper.withSpanFromRequest(DatabricksSparkTracingHelper.scala:136)\n",
       "\tat com.databricks.spark.util.DBRTracing$.withSpanFromRequest(DBRTracing.scala:75)\n",
       "\tat com.databricks.spark.connect.service.RequestContext.runWithSpanFromTags(RequestContext.scala:517)\n",
       "\tat com.databricks.spark.connect.service.RequestContext.$anonfun$runWith$3(RequestContext.scala:494)\n",
       "\tat com.databricks.spark.connect.service.RequestContext$.com$databricks$spark$connect$service$RequestContext$$withLocalProperties(RequestContext.scala:729)\n",
       "\tat com.databricks.spark.connect.service.RequestContext.$anonfun$runWith$2(RequestContext.scala:493)\n",
       "\tat com.databricks.logging.AttributionContextTracing.$anonfun$withAttributionContext$1(AttributionContextTracing.scala:146)\n",
       "\tat com.databricks.logging.AttributionContext$.$anonfun$withValue$1(AttributionContext.scala:349)\n",
       "\tat scala.util.DynamicVariable.withValue(DynamicVariable.scala:59)\n",
       "\tat com.databricks.logging.AttributionContext$.withValue(AttributionContext.scala:345)\n",
       "\tat com.databricks.logging.AttributionContextTracing.withAttributionContext(AttributionContextTracing.scala:144)\n",
       "\tat com.databricks.logging.AttributionContextTracing.withAttributionContext$(AttributionContextTracing.scala:141)\n",
       "\tat com.databricks.spark.util.PublicDBLogging.withAttributionContext(DatabricksSparkUsageLogger.scala:29)\n",
       "\tat com.databricks.spark.util.UniverseAttributionContextWrapper.withValue(AttributionContextUtils.scala:242)\n",
       "\tat com.databricks.spark.connect.service.RequestContext.$anonfun$runWith$1(RequestContext.scala:492)\n",
       "\tat com.databricks.spark.connect.service.RequestContext.withContext(RequestContext.scala:525)\n",
       "\tat com.databricks.spark.connect.service.RequestContext.runWith(RequestContext.scala:485)\n",
       "\tat com.databricks.spark.connect.service.AuthenticationInterceptor$AuthenticatedServerCallListener.onHalfClose(AuthenticationInterceptor.scala:528)\n",
       "\tat org.sparkproject.connect.io.grpc.PartialForwardingServerCallListener.onHalfClose(PartialForwardingServerCallListener.java:35)\n",
       "\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:23)\n",
       "\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener$SimpleForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:40)\n",
       "\tat org.sparkproject.connect.io.grpc.internal.ServerCallImpl$ServerStreamListenerImpl.halfClosed(ServerCallImpl.java:356)\n",
       "\tat org.sparkproject.connect.io.grpc.internal.ServerImpl$JumpToApplicationThreadServerStreamListener$1HalfClosed.runInContext(ServerImpl.java:861)\n",
       "\tat org.sparkproject.connect.io.grpc.internal.ContextRunnable.run(ContextRunnable.java:37)\n",
       "\tat org.sparkproject.connect.io.grpc.internal.SerializingExecutor.run(SerializingExecutor.java:133)\n",
       "\tat org.apache.spark.util.threads.SparkThreadLocalCapturingRunnable.$anonfun$run$1(SparkThreadLocalForwardingThreadPoolExecutor.scala:171)\n",
       "\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n",
       "\tat com.databricks.spark.util.DBRTracing$.withSpanFromParent(DBRTracing.scala:70)\n",
       "\tat org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.$anonfun$runWithCaptured$7(SparkThreadLocalForwardingThreadPoolExecutor.scala:124)\n",
       "\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n",
       "\tat org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.$anonfun$runWithCaptured$6(SparkThreadLocalForwardingThreadPoolExecutor.scala:123)\n",
       "\tat com.databricks.sql.transaction.tahoe.mst.MSTThreadHelper$.runWithMSTContext(MSTThreadHelper.scala:77)\n",
       "\tat org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.$anonfun$runWithCaptured$5(SparkThreadLocalForwardingThreadPoolExecutor.scala:120)\n",
       "\tat com.databricks.spark.util.IdentityClaim$.withClaim(IdentityClaim.scala:48)\n",
       "\tat org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.$anonfun$runWithCaptured$4(SparkThreadLocalForwardingThreadPoolExecutor.scala:119)\n",
       "\tat com.databricks.unity.UCSEphemeralState$Handle.runWith(UCSEphemeralState.scala:51)\n",
       "\tat org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.runWithCaptured(SparkThreadLocalForwardingThreadPoolExecutor.scala:118)\n",
       "\tat org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.runWithCaptured$(SparkThreadLocalForwardingThreadPoolExecutor.scala:95)\n",
       "\tat org.apache.spark.util.threads.SparkThreadLocalCapturingRunnable.runWithCaptured(SparkThreadLocalForwardingThreadPoolExecutor.scala:168)\n",
       "\tat org.apache.spark.util.threads.SparkThreadLocalCapturingRunnable.run(SparkThreadLocalForwardingThreadPoolExecutor.scala:171)\n",
       "\tat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)\n",
       "\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)\n",
       "\tat java.lang.Thread.run(Thread.java:840)"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "arguments": {},
       "datasetInfos": [],
       "jupyterProps": {
        "ename": "AnalysisException",
        "evalue": "[NOT_SUPPORTED_WITH_SERVERLESS] UNPERSIST TABLE is not supported on serverless compute. SQLSTATE: 0A000\n\nJVM stacktrace:\norg.apache.spark.sql.AnalysisException\n\tat com.databricks.serverless.ServerlessGCEdgeCheck$.throwError(ServerlessGCEdgeCheck.scala:72)\n\tat com.databricks.serverless.ServerlessGCEdgeCheck$.checkBlockCacheCommand(ServerlessGCEdgeCheck.scala:50)\n\tat org.apache.spark.sql.connect.service.SparkConnectAnalyzeHandler.process(SparkConnectAnalyzeHandler.scala:492)\n\tat org.apache.spark.sql.connect.service.SparkConnectAnalyzeHandler.$anonfun$handle$5(SparkConnectAnalyzeHandler.scala:96)\n\tat org.apache.spark.sql.connect.service.SparkConnectAnalyzeHandler.$anonfun$handle$5$adapted(SparkConnectAnalyzeHandler.scala:88)\n\tat org.apache.spark.sql.connect.service.SessionHolder.$anonfun$withSession$2(SessionHolder.scala:844)\n\tat org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:866)\n\tat org.apache.spark.sql.connect.service.SessionHolder.$anonfun$withSession$1(SessionHolder.scala:844)\n\tat org.apache.spark.JobArtifactSet$.withActiveJobArtifactState(JobArtifactSet.scala:97)\n\tat org.apache.spark.sql.artifact.ArtifactManager.$anonfun$withResources$1(ArtifactManager.scala:124)\n\tat org.apache.spark.sql.artifact.ArtifactManager.withClassLoaderIfNeeded(ArtifactManager.scala:118)\n\tat org.apache.spark.sql.artifact.ArtifactManager.withResources(ArtifactManager.scala:123)\n\tat org.apache.spark.sql.connect.service.SessionHolder.withSession(SessionHolder.scala:843)\n\tat org.apache.spark.sql.connect.service.SparkConnectAnalyzeHandler.$anonfun$handle$1(SparkConnectAnalyzeHandler.scala:88)\n\tat org.apache.spark.sql.connect.service.SparkConnectAnalyzeHandler.$anonfun$handle$1$adapted(SparkConnectAnalyzeHandler.scala:55)\n\tat com.databricks.spark.connect.logging.rpc.SparkConnectRpcMetricsCollectorUtils$.collectMetrics(SparkConnectRpcMetricsCollector.scala:294)\n\tat org.apache.spark.sql.connect.service.SparkConnectAnalyzeHandler.handle(SparkConnectAnalyzeHandler.scala:54)\n\tat org.apache.spark.sql.connect.service.SparkConnectService.analyzePlan(SparkConnectService.scala:117)\n\tat org.apache.spark.connect.proto.SparkConnectServiceGrpc$MethodHandlers.invoke(SparkConnectServiceGrpc.java:1008)\n\tat org.sparkproject.connect.io.grpc.stub.ServerCalls$UnaryServerCallHandler$UnaryServerCallListener.onHalfClose(ServerCalls.java:182)\n\tat org.sparkproject.connect.io.grpc.PartialForwardingServerCallListener.onHalfClose(PartialForwardingServerCallListener.java:35)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:23)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener$SimpleForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:40)\n\tat org.sparkproject.connect.io.grpc.PartialForwardingServerCallListener.onHalfClose(PartialForwardingServerCallListener.java:35)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:23)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener$SimpleForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:40)\n\tat org.sparkproject.connect.io.grpc.PartialForwardingServerCallListener.onHalfClose(PartialForwardingServerCallListener.java:35)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:23)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener$SimpleForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:40)\n\tat org.sparkproject.connect.io.grpc.Contexts$ContextualizedServerCallListener.onHalfClose(Contexts.java:86)\n\tat org.sparkproject.connect.io.grpc.PartialForwardingServerCallListener.onHalfClose(PartialForwardingServerCallListener.java:35)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:23)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener$SimpleForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:40)\n\tat com.databricks.spark.connect.service.AuthenticationInterceptor$AuthenticatedServerCallListener.$anonfun$onHalfClose$1(AuthenticationInterceptor.scala:528)\n\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n\tat com.databricks.unity.UCSEphemeralState$Handle.runWith(UCSEphemeralState.scala:51)\n\tat com.databricks.unity.HandleImpl.runWith(UCSHandle.scala:128)\n\tat com.databricks.spark.connect.service.RequestContext.$anonfun$runWith$4(RequestContext.scala:494)\n\tat com.databricks.util.TracingSpanUtils$.withSyncTracingAndParentFromHeaders(TracingSpanUtils.scala:456)\n\tat com.databricks.spark.util.DatabricksTracingHelper.withSpanFromRequest(DatabricksSparkTracingHelper.scala:136)\n\tat com.databricks.spark.util.DBRTracing$.withSpanFromRequest(DBRTracing.scala:75)\n\tat com.databricks.spark.connect.service.RequestContext.runWithSpanFromTags(RequestContext.scala:517)\n\tat com.databricks.spark.connect.service.RequestContext.$anonfun$runWith$3(RequestContext.scala:494)\n\tat com.databricks.spark.connect.service.RequestContext$.com$databricks$spark$connect$service$RequestContext$$withLocalProperties(RequestContext.scala:729)\n\tat com.databricks.spark.connect.service.RequestContext.$anonfun$runWith$2(RequestContext.scala:493)\n\tat com.databricks.logging.AttributionContextTracing.$anonfun$withAttributionContext$1(AttributionContextTracing.scala:146)\n\tat com.databricks.logging.AttributionContext$.$anonfun$withValue$1(AttributionContext.scala:349)\n\tat scala.util.DynamicVariable.withValue(DynamicVariable.scala:59)\n\tat com.databricks.logging.AttributionContext$.withValue(AttributionContext.scala:345)\n\tat com.databricks.logging.AttributionContextTracing.withAttributionContext(AttributionContextTracing.scala:144)\n\tat com.databricks.logging.AttributionContextTracing.withAttributionContext$(AttributionContextTracing.scala:141)\n\tat com.databricks.spark.util.PublicDBLogging.withAttributionContext(DatabricksSparkUsageLogger.scala:29)\n\tat com.databricks.spark.util.UniverseAttributionContextWrapper.withValue(AttributionContextUtils.scala:242)\n\tat com.databricks.spark.connect.service.RequestContext.$anonfun$runWith$1(RequestContext.scala:492)\n\tat com.databricks.spark.connect.service.RequestContext.withContext(RequestContext.scala:525)\n\tat com.databricks.spark.connect.service.RequestContext.runWith(RequestContext.scala:485)\n\tat com.databricks.spark.connect.service.AuthenticationInterceptor$AuthenticatedServerCallListener.onHalfClose(AuthenticationInterceptor.scala:528)\n\tat org.sparkproject.connect.io.grpc.PartialForwardingServerCallListener.onHalfClose(PartialForwardingServerCallListener.java:35)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:23)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener$SimpleForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:40)\n\tat org.sparkproject.connect.io.grpc.internal.ServerCallImpl$ServerStreamListenerImpl.halfClosed(ServerCallImpl.java:356)\n\tat org.sparkproject.connect.io.grpc.internal.ServerImpl$JumpToApplicationThreadServerStreamListener$1HalfClosed.runInContext(ServerImpl.java:861)\n\tat org.sparkproject.connect.io.grpc.internal.ContextRunnable.run(ContextRunnable.java:37)\n\tat org.sparkproject.connect.io.grpc.internal.SerializingExecutor.run(SerializingExecutor.java:133)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingRunnable.$anonfun$run$1(SparkThreadLocalForwardingThreadPoolExecutor.scala:171)\n\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n\tat com.databricks.spark.util.DBRTracing$.withSpanFromParent(DBRTracing.scala:70)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.$anonfun$runWithCaptured$7(SparkThreadLocalForwardingThreadPoolExecutor.scala:124)\n\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.$anonfun$runWithCaptured$6(SparkThreadLocalForwardingThreadPoolExecutor.scala:123)\n\tat com.databricks.sql.transaction.tahoe.mst.MSTThreadHelper$.runWithMSTContext(MSTThreadHelper.scala:77)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.$anonfun$runWithCaptured$5(SparkThreadLocalForwardingThreadPoolExecutor.scala:120)\n\tat com.databricks.spark.util.IdentityClaim$.withClaim(IdentityClaim.scala:48)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.$anonfun$runWithCaptured$4(SparkThreadLocalForwardingThreadPoolExecutor.scala:119)\n\tat com.databricks.unity.UCSEphemeralState$Handle.runWith(UCSEphemeralState.scala:51)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.runWithCaptured(SparkThreadLocalForwardingThreadPoolExecutor.scala:118)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.runWithCaptured$(SparkThreadLocalForwardingThreadPoolExecutor.scala:95)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingRunnable.runWithCaptured(SparkThreadLocalForwardingThreadPoolExecutor.scala:168)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingRunnable.run(SparkThreadLocalForwardingThreadPoolExecutor.scala:171)\n\tat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)\n\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)\n\tat java.lang.Thread.run(Thread.java:840)"
       },
       "metadata": {
        "errorSummary": "[NOT_SUPPORTED_WITH_SERVERLESS] UNPERSIST TABLE is not supported on serverless compute. SQLSTATE: 0A000"
       },
       "removedWidgets": [],
       "sqlProps": {
        "breakingChangeInfo": null,
        "errorClass": "NOT_SUPPORTED_WITH_SERVERLESS",
        "pysparkCallSite": "",
        "pysparkFragment": "",
        "pysparkSummary": "",
        "sqlState": "0A000",
        "stackTrace": "org.apache.spark.sql.AnalysisException\n\tat com.databricks.serverless.ServerlessGCEdgeCheck$.throwError(ServerlessGCEdgeCheck.scala:72)\n\tat com.databricks.serverless.ServerlessGCEdgeCheck$.checkBlockCacheCommand(ServerlessGCEdgeCheck.scala:50)\n\tat org.apache.spark.sql.connect.service.SparkConnectAnalyzeHandler.process(SparkConnectAnalyzeHandler.scala:492)\n\tat org.apache.spark.sql.connect.service.SparkConnectAnalyzeHandler.$anonfun$handle$5(SparkConnectAnalyzeHandler.scala:96)\n\tat org.apache.spark.sql.connect.service.SparkConnectAnalyzeHandler.$anonfun$handle$5$adapted(SparkConnectAnalyzeHandler.scala:88)\n\tat org.apache.spark.sql.connect.service.SessionHolder.$anonfun$withSession$2(SessionHolder.scala:844)\n\tat org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:866)\n\tat org.apache.spark.sql.connect.service.SessionHolder.$anonfun$withSession$1(SessionHolder.scala:844)\n\tat org.apache.spark.JobArtifactSet$.withActiveJobArtifactState(JobArtifactSet.scala:97)\n\tat org.apache.spark.sql.artifact.ArtifactManager.$anonfun$withResources$1(ArtifactManager.scala:124)\n\tat org.apache.spark.sql.artifact.ArtifactManager.withClassLoaderIfNeeded(ArtifactManager.scala:118)\n\tat org.apache.spark.sql.artifact.ArtifactManager.withResources(ArtifactManager.scala:123)\n\tat org.apache.spark.sql.connect.service.SessionHolder.withSession(SessionHolder.scala:843)\n\tat org.apache.spark.sql.connect.service.SparkConnectAnalyzeHandler.$anonfun$handle$1(SparkConnectAnalyzeHandler.scala:88)\n\tat org.apache.spark.sql.connect.service.SparkConnectAnalyzeHandler.$anonfun$handle$1$adapted(SparkConnectAnalyzeHandler.scala:55)\n\tat com.databricks.spark.connect.logging.rpc.SparkConnectRpcMetricsCollectorUtils$.collectMetrics(SparkConnectRpcMetricsCollector.scala:294)\n\tat org.apache.spark.sql.connect.service.SparkConnectAnalyzeHandler.handle(SparkConnectAnalyzeHandler.scala:54)\n\tat org.apache.spark.sql.connect.service.SparkConnectService.analyzePlan(SparkConnectService.scala:117)\n\tat org.apache.spark.connect.proto.SparkConnectServiceGrpc$MethodHandlers.invoke(SparkConnectServiceGrpc.java:1008)\n\tat org.sparkproject.connect.io.grpc.stub.ServerCalls$UnaryServerCallHandler$UnaryServerCallListener.onHalfClose(ServerCalls.java:182)\n\tat org.sparkproject.connect.io.grpc.PartialForwardingServerCallListener.onHalfClose(PartialForwardingServerCallListener.java:35)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:23)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener$SimpleForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:40)\n\tat org.sparkproject.connect.io.grpc.PartialForwardingServerCallListener.onHalfClose(PartialForwardingServerCallListener.java:35)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:23)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener$SimpleForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:40)\n\tat org.sparkproject.connect.io.grpc.PartialForwardingServerCallListener.onHalfClose(PartialForwardingServerCallListener.java:35)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:23)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener$SimpleForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:40)\n\tat org.sparkproject.connect.io.grpc.Contexts$ContextualizedServerCallListener.onHalfClose(Contexts.java:86)\n\tat org.sparkproject.connect.io.grpc.PartialForwardingServerCallListener.onHalfClose(PartialForwardingServerCallListener.java:35)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:23)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener$SimpleForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:40)\n\tat com.databricks.spark.connect.service.AuthenticationInterceptor$AuthenticatedServerCallListener.$anonfun$onHalfClose$1(AuthenticationInterceptor.scala:528)\n\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n\tat com.databricks.unity.UCSEphemeralState$Handle.runWith(UCSEphemeralState.scala:51)\n\tat com.databricks.unity.HandleImpl.runWith(UCSHandle.scala:128)\n\tat com.databricks.spark.connect.service.RequestContext.$anonfun$runWith$4(RequestContext.scala:494)\n\tat com.databricks.util.TracingSpanUtils$.withSyncTracingAndParentFromHeaders(TracingSpanUtils.scala:456)\n\tat com.databricks.spark.util.DatabricksTracingHelper.withSpanFromRequest(DatabricksSparkTracingHelper.scala:136)\n\tat com.databricks.spark.util.DBRTracing$.withSpanFromRequest(DBRTracing.scala:75)\n\tat com.databricks.spark.connect.service.RequestContext.runWithSpanFromTags(RequestContext.scala:517)\n\tat com.databricks.spark.connect.service.RequestContext.$anonfun$runWith$3(RequestContext.scala:494)\n\tat com.databricks.spark.connect.service.RequestContext$.com$databricks$spark$connect$service$RequestContext$$withLocalProperties(RequestContext.scala:729)\n\tat com.databricks.spark.connect.service.RequestContext.$anonfun$runWith$2(RequestContext.scala:493)\n\tat com.databricks.logging.AttributionContextTracing.$anonfun$withAttributionContext$1(AttributionContextTracing.scala:146)\n\tat com.databricks.logging.AttributionContext$.$anonfun$withValue$1(AttributionContext.scala:349)\n\tat scala.util.DynamicVariable.withValue(DynamicVariable.scala:59)\n\tat com.databricks.logging.AttributionContext$.withValue(AttributionContext.scala:345)\n\tat com.databricks.logging.AttributionContextTracing.withAttributionContext(AttributionContextTracing.scala:144)\n\tat com.databricks.logging.AttributionContextTracing.withAttributionContext$(AttributionContextTracing.scala:141)\n\tat com.databricks.spark.util.PublicDBLogging.withAttributionContext(DatabricksSparkUsageLogger.scala:29)\n\tat com.databricks.spark.util.UniverseAttributionContextWrapper.withValue(AttributionContextUtils.scala:242)\n\tat com.databricks.spark.connect.service.RequestContext.$anonfun$runWith$1(RequestContext.scala:492)\n\tat com.databricks.spark.connect.service.RequestContext.withContext(RequestContext.scala:525)\n\tat com.databricks.spark.connect.service.RequestContext.runWith(RequestContext.scala:485)\n\tat com.databricks.spark.connect.service.AuthenticationInterceptor$AuthenticatedServerCallListener.onHalfClose(AuthenticationInterceptor.scala:528)\n\tat org.sparkproject.connect.io.grpc.PartialForwardingServerCallListener.onHalfClose(PartialForwardingServerCallListener.java:35)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:23)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener$SimpleForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:40)\n\tat org.sparkproject.connect.io.grpc.internal.ServerCallImpl$ServerStreamListenerImpl.halfClosed(ServerCallImpl.java:356)\n\tat org.sparkproject.connect.io.grpc.internal.ServerImpl$JumpToApplicationThreadServerStreamListener$1HalfClosed.runInContext(ServerImpl.java:861)\n\tat org.sparkproject.connect.io.grpc.internal.ContextRunnable.run(ContextRunnable.java:37)\n\tat org.sparkproject.connect.io.grpc.internal.SerializingExecutor.run(SerializingExecutor.java:133)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingRunnable.$anonfun$run$1(SparkThreadLocalForwardingThreadPoolExecutor.scala:171)\n\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n\tat com.databricks.spark.util.DBRTracing$.withSpanFromParent(DBRTracing.scala:70)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.$anonfun$runWithCaptured$7(SparkThreadLocalForwardingThreadPoolExecutor.scala:124)\n\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.$anonfun$runWithCaptured$6(SparkThreadLocalForwardingThreadPoolExecutor.scala:123)\n\tat com.databricks.sql.transaction.tahoe.mst.MSTThreadHelper$.runWithMSTContext(MSTThreadHelper.scala:77)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.$anonfun$runWithCaptured$5(SparkThreadLocalForwardingThreadPoolExecutor.scala:120)\n\tat com.databricks.spark.util.IdentityClaim$.withClaim(IdentityClaim.scala:48)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.$anonfun$runWithCaptured$4(SparkThreadLocalForwardingThreadPoolExecutor.scala:119)\n\tat com.databricks.unity.UCSEphemeralState$Handle.runWith(UCSEphemeralState.scala:51)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.runWithCaptured(SparkThreadLocalForwardingThreadPoolExecutor.scala:118)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.runWithCaptured$(SparkThreadLocalForwardingThreadPoolExecutor.scala:95)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingRunnable.runWithCaptured(SparkThreadLocalForwardingThreadPoolExecutor.scala:168)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingRunnable.run(SparkThreadLocalForwardingThreadPoolExecutor.scala:171)\n\tat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)\n\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)\n\tat java.lang.Thread.run(Thread.java:840)",
        "startIndex": null,
        "stopIndex": null
       },
       "stackFrames": [
        "\u001B[0;31m---------------------------------------------------------------------------\u001B[0m",
        "\u001B[0;31mAnalysisException\u001B[0m                         Traceback (most recent call last)",
        "File \u001B[0;32m<command-8820324460378679>, line 26\u001B[0m\n\u001B[1;32m     23\u001B[0m spark\u001B[38;5;241m.\u001B[39msql(\u001B[38;5;124mf\u001B[39m\u001B[38;5;124m\"\u001B[39m\u001B[38;5;124mCOMMENT ON TABLE \u001B[39m\u001B[38;5;132;01m{\u001B[39;00mnamespace\u001B[38;5;132;01m}\u001B[39;00m\u001B[38;5;124m.`gold_implementation_summary` IS \u001B[39m\u001B[38;5;124m'\u001B[39m\u001B[38;5;124mInventory and row counts for all retail demo outputs\u001B[39m\u001B[38;5;124m'\u001B[39m\u001B[38;5;124m\"\u001B[39m)\n\u001B[1;32m     25\u001B[0m display(implementation_summary\u001B[38;5;241m.\u001B[39morderBy(\u001B[38;5;124m\"\u001B[39m\u001B[38;5;124mphase\u001B[39m\u001B[38;5;124m\"\u001B[39m, \u001B[38;5;124m\"\u001B[39m\u001B[38;5;124mfull_table_name\u001B[39m\u001B[38;5;124m\"\u001B[39m))\n\u001B[0;32m---> 26\u001B[0m \u001B[43msilver_table\u001B[49m\u001B[38;5;241;43m.\u001B[39;49m\u001B[43munpersist\u001B[49m\u001B[43m(\u001B[49m\u001B[43m)\u001B[49m\n\u001B[1;32m     27\u001B[0m \u001B[38;5;28mprint\u001B[39m(\u001B[38;5;124m\"\u001B[39m\u001B[38;5;124mAll required validation checks passed. The retail lakehouse demo is ready for Genie.\u001B[39m\u001B[38;5;124m\"\u001B[39m)\n",
        "File \u001B[0;32m/databricks/python/lib/python3.12/site-packages/pyspark/sql/connect/dataframe.py:2156\u001B[0m, in \u001B[0;36mDataFrame.unpersist\u001B[0;34m(self, blocking)\u001B[0m\n\u001B[1;32m   2154\u001B[0m \u001B[38;5;28;01mdef\u001B[39;00m\u001B[38;5;250m \u001B[39m\u001B[38;5;21munpersist\u001B[39m(\u001B[38;5;28mself\u001B[39m, blocking: \u001B[38;5;28mbool\u001B[39m \u001B[38;5;241m=\u001B[39m \u001B[38;5;28;01mFalse\u001B[39;00m) \u001B[38;5;241m-\u001B[39m\u001B[38;5;241m>\u001B[39m ParentDataFrame:\n\u001B[1;32m   2155\u001B[0m     relation \u001B[38;5;241m=\u001B[39m \u001B[38;5;28mself\u001B[39m\u001B[38;5;241m.\u001B[39m_plan\u001B[38;5;241m.\u001B[39mplan(\u001B[38;5;28mself\u001B[39m\u001B[38;5;241m.\u001B[39m_session\u001B[38;5;241m.\u001B[39mclient)\n\u001B[0;32m-> 2156\u001B[0m     \u001B[38;5;28;43mself\u001B[39;49m\u001B[38;5;241;43m.\u001B[39;49m\u001B[43m_session\u001B[49m\u001B[38;5;241;43m.\u001B[39;49m\u001B[43mclient\u001B[49m\u001B[38;5;241;43m.\u001B[39;49m\u001B[43m_analyze\u001B[49m\u001B[43m(\u001B[49m\u001B[43mmethod\u001B[49m\u001B[38;5;241;43m=\u001B[39;49m\u001B[38;5;124;43m\"\u001B[39;49m\u001B[38;5;124;43munpersist\u001B[39;49m\u001B[38;5;124;43m\"\u001B[39;49m\u001B[43m,\u001B[49m\u001B[43m \u001B[49m\u001B[43mrelation\u001B[49m\u001B[38;5;241;43m=\u001B[39;49m\u001B[43mrelation\u001B[49m\u001B[43m,\u001B[49m\u001B[43m \u001B[49m\u001B[43mblocking\u001B[49m\u001B[38;5;241;43m=\u001B[39;49m\u001B[43mblocking\u001B[49m\u001B[43m)\u001B[49m\n\u001B[1;32m   2157\u001B[0m     \u001B[38;5;28;01mreturn\u001B[39;00m \u001B[38;5;28mself\u001B[39m\n",
        "File \u001B[0;32m/databricks/python/lib/python3.12/site-packages/pyspark/sql/connect/client/core.py:1808\u001B[0m, in \u001B[0;36mSparkConnectClient._analyze\u001B[0;34m(self, method, **kwargs)\u001B[0m\n\u001B[1;32m   1806\u001B[0m     \u001B[38;5;28;01mraise\u001B[39;00m SparkConnectException(\u001B[38;5;124m\"\u001B[39m\u001B[38;5;124mInvalid state during retry exception handling.\u001B[39m\u001B[38;5;124m\"\u001B[39m)\n\u001B[1;32m   1807\u001B[0m \u001B[38;5;28;01mexcept\u001B[39;00m \u001B[38;5;167;01mException\u001B[39;00m \u001B[38;5;28;01mas\u001B[39;00m error:\n\u001B[0;32m-> 1808\u001B[0m     \u001B[38;5;28;43mself\u001B[39;49m\u001B[38;5;241;43m.\u001B[39;49m\u001B[43m_handle_error\u001B[49m\u001B[43m(\u001B[49m\u001B[43merror\u001B[49m\u001B[43m)\u001B[49m\n",
        "File \u001B[0;32m/databricks/python/lib/python3.12/site-packages/pyspark/sql/connect/client/core.py:2380\u001B[0m, in \u001B[0;36mSparkConnectClient._handle_error\u001B[0;34m(self, error)\u001B[0m\n\u001B[1;32m   2378\u001B[0m     \u001B[38;5;28mself\u001B[39m\u001B[38;5;241m.\u001B[39mthread_local\u001B[38;5;241m.\u001B[39minside_error_handling \u001B[38;5;241m=\u001B[39m \u001B[38;5;28;01mTrue\u001B[39;00m\n\u001B[1;32m   2379\u001B[0m     \u001B[38;5;28;01mif\u001B[39;00m \u001B[38;5;28misinstance\u001B[39m(error, grpc\u001B[38;5;241m.\u001B[39mRpcError):\n\u001B[0;32m-> 2380\u001B[0m         \u001B[38;5;28;43mself\u001B[39;49m\u001B[38;5;241;43m.\u001B[39;49m\u001B[43m_handle_rpc_error\u001B[49m\u001B[43m(\u001B[49m\u001B[43merror\u001B[49m\u001B[43m)\u001B[49m\n\u001B[1;32m   2381\u001B[0m     \u001B[38;5;28;01mraise\u001B[39;00m error\n\u001B[1;32m   2382\u001B[0m \u001B[38;5;28;01mfinally\u001B[39;00m:\n",
        "File \u001B[0;32m/databricks/python/lib/python3.12/site-packages/pyspark/sql/connect/client/core.py:2458\u001B[0m, in \u001B[0;36mSparkConnectClient._handle_rpc_error\u001B[0;34m(self, rpc_error)\u001B[0m\n\u001B[1;32m   2454\u001B[0m             logger\u001B[38;5;241m.\u001B[39mdebug(\u001B[38;5;124mf\u001B[39m\u001B[38;5;124m\"\u001B[39m\u001B[38;5;124mReceived ErrorInfo: \u001B[39m\u001B[38;5;132;01m{\u001B[39;00minfo\u001B[38;5;132;01m}\u001B[39;00m\u001B[38;5;124m\"\u001B[39m)\n\u001B[1;32m   2456\u001B[0m             \u001B[38;5;28mself\u001B[39m\u001B[38;5;241m.\u001B[39m_handle_rpc_error_with_error_info(info, status\u001B[38;5;241m.\u001B[39mmessage, status_code)  \u001B[38;5;66;03m# EDGE\u001B[39;00m\n\u001B[0;32m-> 2458\u001B[0m             \u001B[38;5;28;01mraise\u001B[39;00m convert_exception(\n\u001B[1;32m   2459\u001B[0m                 info,\n\u001B[1;32m   2460\u001B[0m                 status\u001B[38;5;241m.\u001B[39mmessage,\n\u001B[1;32m   2461\u001B[0m                 \u001B[38;5;28mself\u001B[39m\u001B[38;5;241m.\u001B[39m_fetch_enriched_error(info),\n\u001B[1;32m   2462\u001B[0m                 \u001B[38;5;28mself\u001B[39m\u001B[38;5;241m.\u001B[39m_display_server_stack_trace(),\n\u001B[1;32m   2463\u001B[0m                 status_code,\n\u001B[1;32m   2464\u001B[0m             ) \u001B[38;5;28;01mfrom\u001B[39;00m\u001B[38;5;250m \u001B[39m\u001B[38;5;28;01mNone\u001B[39;00m\n\u001B[1;32m   2466\u001B[0m     \u001B[38;5;28;01mraise\u001B[39;00m SparkConnectGrpcException(\n\u001B[1;32m   2467\u001B[0m         message\u001B[38;5;241m=\u001B[39mstatus\u001B[38;5;241m.\u001B[39mmessage,\n\u001B[1;32m   2468\u001B[0m         sql_state\u001B[38;5;241m=\u001B[39mErrorCode\u001B[38;5;241m.\u001B[39mCLIENT_UNEXPECTED_MISSING_SQL_STATE,  \u001B[38;5;66;03m# EDGE\u001B[39;00m\n\u001B[1;32m   2469\u001B[0m         grpc_status_code\u001B[38;5;241m=\u001B[39mstatus_code,\n\u001B[1;32m   2470\u001B[0m     ) \u001B[38;5;28;01mfrom\u001B[39;00m\u001B[38;5;250m \u001B[39m\u001B[38;5;28;01mNone\u001B[39;00m\n\u001B[1;32m   2471\u001B[0m \u001B[38;5;28;01melse\u001B[39;00m:\n",
        "\u001B[0;31mAnalysisException\u001B[0m: [NOT_SUPPORTED_WITH_SERVERLESS] UNPERSIST TABLE is not supported on serverless compute. SQLSTATE: 0A000\n\nJVM stacktrace:\norg.apache.spark.sql.AnalysisException\n\tat com.databricks.serverless.ServerlessGCEdgeCheck$.throwError(ServerlessGCEdgeCheck.scala:72)\n\tat com.databricks.serverless.ServerlessGCEdgeCheck$.checkBlockCacheCommand(ServerlessGCEdgeCheck.scala:50)\n\tat org.apache.spark.sql.connect.service.SparkConnectAnalyzeHandler.process(SparkConnectAnalyzeHandler.scala:492)\n\tat org.apache.spark.sql.connect.service.SparkConnectAnalyzeHandler.$anonfun$handle$5(SparkConnectAnalyzeHandler.scala:96)\n\tat org.apache.spark.sql.connect.service.SparkConnectAnalyzeHandler.$anonfun$handle$5$adapted(SparkConnectAnalyzeHandler.scala:88)\n\tat org.apache.spark.sql.connect.service.SessionHolder.$anonfun$withSession$2(SessionHolder.scala:844)\n\tat org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:866)\n\tat org.apache.spark.sql.connect.service.SessionHolder.$anonfun$withSession$1(SessionHolder.scala:844)\n\tat org.apache.spark.JobArtifactSet$.withActiveJobArtifactState(JobArtifactSet.scala:97)\n\tat org.apache.spark.sql.artifact.ArtifactManager.$anonfun$withResources$1(ArtifactManager.scala:124)\n\tat org.apache.spark.sql.artifact.ArtifactManager.withClassLoaderIfNeeded(ArtifactManager.scala:118)\n\tat org.apache.spark.sql.artifact.ArtifactManager.withResources(ArtifactManager.scala:123)\n\tat org.apache.spark.sql.connect.service.SessionHolder.withSession(SessionHolder.scala:843)\n\tat org.apache.spark.sql.connect.service.SparkConnectAnalyzeHandler.$anonfun$handle$1(SparkConnectAnalyzeHandler.scala:88)\n\tat org.apache.spark.sql.connect.service.SparkConnectAnalyzeHandler.$anonfun$handle$1$adapted(SparkConnectAnalyzeHandler.scala:55)\n\tat com.databricks.spark.connect.logging.rpc.SparkConnectRpcMetricsCollectorUtils$.collectMetrics(SparkConnectRpcMetricsCollector.scala:294)\n\tat org.apache.spark.sql.connect.service.SparkConnectAnalyzeHandler.handle(SparkConnectAnalyzeHandler.scala:54)\n\tat org.apache.spark.sql.connect.service.SparkConnectService.analyzePlan(SparkConnectService.scala:117)\n\tat org.apache.spark.connect.proto.SparkConnectServiceGrpc$MethodHandlers.invoke(SparkConnectServiceGrpc.java:1008)\n\tat org.sparkproject.connect.io.grpc.stub.ServerCalls$UnaryServerCallHandler$UnaryServerCallListener.onHalfClose(ServerCalls.java:182)\n\tat org.sparkproject.connect.io.grpc.PartialForwardingServerCallListener.onHalfClose(PartialForwardingServerCallListener.java:35)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:23)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener$SimpleForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:40)\n\tat org.sparkproject.connect.io.grpc.PartialForwardingServerCallListener.onHalfClose(PartialForwardingServerCallListener.java:35)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:23)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener$SimpleForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:40)\n\tat org.sparkproject.connect.io.grpc.PartialForwardingServerCallListener.onHalfClose(PartialForwardingServerCallListener.java:35)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:23)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener$SimpleForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:40)\n\tat org.sparkproject.connect.io.grpc.Contexts$ContextualizedServerCallListener.onHalfClose(Contexts.java:86)\n\tat org.sparkproject.connect.io.grpc.PartialForwardingServerCallListener.onHalfClose(PartialForwardingServerCallListener.java:35)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:23)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener$SimpleForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:40)\n\tat com.databricks.spark.connect.service.AuthenticationInterceptor$AuthenticatedServerCallListener.$anonfun$onHalfClose$1(AuthenticationInterceptor.scala:528)\n\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n\tat com.databricks.unity.UCSEphemeralState$Handle.runWith(UCSEphemeralState.scala:51)\n\tat com.databricks.unity.HandleImpl.runWith(UCSHandle.scala:128)\n\tat com.databricks.spark.connect.service.RequestContext.$anonfun$runWith$4(RequestContext.scala:494)\n\tat com.databricks.util.TracingSpanUtils$.withSyncTracingAndParentFromHeaders(TracingSpanUtils.scala:456)\n\tat com.databricks.spark.util.DatabricksTracingHelper.withSpanFromRequest(DatabricksSparkTracingHelper.scala:136)\n\tat com.databricks.spark.util.DBRTracing$.withSpanFromRequest(DBRTracing.scala:75)\n\tat com.databricks.spark.connect.service.RequestContext.runWithSpanFromTags(RequestContext.scala:517)\n\tat com.databricks.spark.connect.service.RequestContext.$anonfun$runWith$3(RequestContext.scala:494)\n\tat com.databricks.spark.connect.service.RequestContext$.com$databricks$spark$connect$service$RequestContext$$withLocalProperties(RequestContext.scala:729)\n\tat com.databricks.spark.connect.service.RequestContext.$anonfun$runWith$2(RequestContext.scala:493)\n\tat com.databricks.logging.AttributionContextTracing.$anonfun$withAttributionContext$1(AttributionContextTracing.scala:146)\n\tat com.databricks.logging.AttributionContext$.$anonfun$withValue$1(AttributionContext.scala:349)\n\tat scala.util.DynamicVariable.withValue(DynamicVariable.scala:59)\n\tat com.databricks.logging.AttributionContext$.withValue(AttributionContext.scala:345)\n\tat com.databricks.logging.AttributionContextTracing.withAttributionContext(AttributionContextTracing.scala:144)\n\tat com.databricks.logging.AttributionContextTracing.withAttributionContext$(AttributionContextTracing.scala:141)\n\tat com.databricks.spark.util.PublicDBLogging.withAttributionContext(DatabricksSparkUsageLogger.scala:29)\n\tat com.databricks.spark.util.UniverseAttributionContextWrapper.withValue(AttributionContextUtils.scala:242)\n\tat com.databricks.spark.connect.service.RequestContext.$anonfun$runWith$1(RequestContext.scala:492)\n\tat com.databricks.spark.connect.service.RequestContext.withContext(RequestContext.scala:525)\n\tat com.databricks.spark.connect.service.RequestContext.runWith(RequestContext.scala:485)\n\tat com.databricks.spark.connect.service.AuthenticationInterceptor$AuthenticatedServerCallListener.onHalfClose(AuthenticationInterceptor.scala:528)\n\tat org.sparkproject.connect.io.grpc.PartialForwardingServerCallListener.onHalfClose(PartialForwardingServerCallListener.java:35)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:23)\n\tat org.sparkproject.connect.io.grpc.ForwardingServerCallListener$SimpleForwardingServerCallListener.onHalfClose(ForwardingServerCallListener.java:40)\n\tat org.sparkproject.connect.io.grpc.internal.ServerCallImpl$ServerStreamListenerImpl.halfClosed(ServerCallImpl.java:356)\n\tat org.sparkproject.connect.io.grpc.internal.ServerImpl$JumpToApplicationThreadServerStreamListener$1HalfClosed.runInContext(ServerImpl.java:861)\n\tat org.sparkproject.connect.io.grpc.internal.ContextRunnable.run(ContextRunnable.java:37)\n\tat org.sparkproject.connect.io.grpc.internal.SerializingExecutor.run(SerializingExecutor.java:133)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingRunnable.$anonfun$run$1(SparkThreadLocalForwardingThreadPoolExecutor.scala:171)\n\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)\n\tat com.databricks.spark.util.DBRTracing$.withSpanFromParent(DBRTracing.scala:70)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.$anonfun$runWithCaptured$7(SparkThreadLocalForwardingThreadPoolExecutor.scala:124)\n\tat com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.$anonfun$runWithCaptured$6(SparkThreadLocalForwardingThreadPoolExecutor.scala:123)\n\tat com.databricks.sql.transaction.tahoe.mst.MSTThreadHelper$.runWithMSTContext(MSTThreadHelper.scala:77)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.$anonfun$runWithCaptured$5(SparkThreadLocalForwardingThreadPoolExecutor.scala:120)\n\tat com.databricks.spark.util.IdentityClaim$.withClaim(IdentityClaim.scala:48)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.$anonfun$runWithCaptured$4(SparkThreadLocalForwardingThreadPoolExecutor.scala:119)\n\tat com.databricks.unity.UCSEphemeralState$Handle.runWith(UCSEphemeralState.scala:51)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.runWithCaptured(SparkThreadLocalForwardingThreadPoolExecutor.scala:118)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.runWithCaptured$(SparkThreadLocalForwardingThreadPoolExecutor.scala:95)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingRunnable.runWithCaptured(SparkThreadLocalForwardingThreadPoolExecutor.scala:168)\n\tat org.apache.spark.util.threads.SparkThreadLocalCapturingRunnable.run(SparkThreadLocalForwardingThreadPoolExecutor.scala:171)\n\tat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)\n\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)\n\tat java.lang.Thread.run(Thread.java:840)"
       ],
       "type": "baseError"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "summary_objects = [\n",
    "    (\"Bronze\", \"bronze_retail_events\", \"Raw point-of-sale events plus ingestion metadata\"),\n",
    "    (\"Silver\", \"silver_retail_events\", \"Validated, deduplicated, revenue-enriched events\"),\n",
    "    (\"Gold\", \"gold_daily_metrics\", \"Daily executive retail KPIs and forecast training target\"),\n",
    "    (\"Gold\", \"gold_daily_category_metrics\", \"Daily category trend and product-mix metrics\"),\n",
    "    (\"Gold\", \"gold_daily_store_channel_metrics\", \"Daily region, store, and channel drilldown metrics\"),\n",
    "    (\"Gold\", \"gold_daily_revenue_forecast\", \"Future total daily net-revenue predictions\"),\n",
    "    (\"Gold\", \"gold_validation_results\", \"Persisted data-quality and reconciliation checks\"),\n",
    "]\n",
    "\n",
    "summary_rows = [\n",
    "    (phase, table_name, spark.table(table(table_name)).count(), description)\n",
    "    for phase, table_name, description in summary_objects\n",
    "]\n",
    "summary_schema = \"phase string, table_name string, row_count long, purpose string\"\n",
    "implementation_summary = (\n",
    "    spark.createDataFrame(summary_rows, summary_schema)\n",
    "    .withColumn(\"full_table_name\", F.concat_ws(\".\", F.lit(catalog), F.lit(schema), F.col(\"table_name\")))\n",
    "    .withColumn(\"completed_at\", F.current_timestamp())\n",
    "    .select(\"phase\", \"full_table_name\", \"row_count\", \"purpose\", \"completed_at\")\n",
    ")\n",
    "(implementation_summary.write.format(\"delta\").mode(\"overwrite\").option(\"overwriteSchema\", \"true\").saveAsTable(table(\"gold_implementation_summary\")))\n",
    "spark.sql(f\"COMMENT ON TABLE {namespace}.`gold_implementation_summary` IS 'Inventory and row counts for all retail demo outputs'\")\n",
    "\n",
    "display(implementation_summary.orderBy(\"phase\", \"full_table_name\"))\n",
    "silver_table.unpersist()\n",
    "print(\"All required validation checks passed. The retail lakehouse demo is ready for Genie.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "6b13bfdf-a4b1-4755-884c-894e04a40f93",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## 8. Genie prompt for dashboards and reports\n",
    "\n",
    "Create a Genie space with the gold tables listed below, then paste the generated prompt. The prompt\n",
    "is fully qualified so it remains unambiguous if the workspace has similarly named datasets."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "d2cff216-f3f1-4485-b640-c301b7d88fa6",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "%skip\n",
    "genie_prompt = f\"\"\"\n",
    "You are a retail analytics expert. Build an executive dashboard and supporting report using only these\n",
    "Unity Catalog tables:\n",
    "- {table('gold_daily_metrics')}: daily KPIs including transactions, customers, units, net revenue,\n",
    "  average completed order value, and return rate.\n",
    "- {table('gold_daily_category_metrics')}: daily category revenue, units, discounts, and returns.\n",
    "- {table('gold_daily_store_channel_metrics')}: daily region/store/channel performance.\n",
    "- {table('gold_daily_revenue_forecast')}: the next {forecast_days} days of forecast total net revenue.\n",
    "- {table('gold_validation_results')}: current pipeline quality checks.\n",
    "\n",
    "Create:\n",
    "1. KPI cards for latest-day net revenue, transactions, unique customers, average completed order value,\n",
    "   and return rate, with comparisons to the prior day and trailing 7-day average.\n",
    "2. A time-series chart of actual daily net revenue followed by forecast net revenue; clearly distinguish\n",
    "   actuals from predictions and mark the forecast boundary after {max_date}.\n",
    "3. Category revenue and net-unit trends, plus each category's share of total revenue.\n",
    "4. Region, store, and channel leaderboards with date, category, region, store, and channel filters.\n",
    "5. A data-quality status section showing every validation result and prominently flagging failures.\n",
    "6. A concise narrative explaining the strongest trend, best and weakest segments, return-rate risks,\n",
    "   and the forecast outlook. State that the forecast is a simple linear trend plus weekly-seasonality baseline.\n",
    "\n",
    "Use net_revenue as the authoritative sales measure because it includes discounts and subtracts returns.\n",
    "Do not sum distinct customer counts across dates. Format revenue as currency and rates as percentages.\n",
    "\"\"\".strip()\n",
    "\n",
    "print(genie_prompt)"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "5"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 4
   },
   "notebookName": "retail_analytics_demo2",
   "widgets": {
    "catalog": {
     "currentValue": "workspace",
     "nuid": "59d89af6-ab45-448f-b60b-f0de40c4cfd9",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "workspace",
      "label": "01 Unity Catalog catalog",
      "name": "catalog",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "workspace",
      "label": "01 Unity Catalog catalog",
      "name": "catalog",
      "options": {
       "widgetType": "text",
       "autoCreated": false,
       "validationRegex": null
      }
     }
    },
    "event_count": {
     "currentValue": "30000",
     "nuid": "1db81db3-49bb-40ab-beae-5d06cbf25a04",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "30000",
      "label": "03 Base event count",
      "name": "event_count",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "30000",
      "label": "03 Base event count",
      "name": "event_count",
      "options": {
       "widgetType": "text",
       "autoCreated": false,
       "validationRegex": null
      }
     }
    },
    "forecast_days": {
     "currentValue": "14",
     "nuid": "01072432-5924-4429-8f8a-813a8c18a077",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "14",
      "label": "04 Forecast horizon (days)",
      "name": "forecast_days",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "14",
      "label": "04 Forecast horizon (days)",
      "name": "forecast_days",
      "options": {
       "widgetType": "text",
       "autoCreated": false,
       "validationRegex": null
      }
     }
    },
    "schema": {
     "currentValue": "retail_meetup_demo",
     "nuid": "d1ee2f09-d111-4c56-82e7-c8fcf3deb15e",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "retail_meetup_demo",
      "label": "02 Demo schema",
      "name": "schema",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "retail_meetup_demo",
      "label": "02 Demo schema",
      "name": "schema",
      "options": {
       "widgetType": "text",
       "autoCreated": false,
       "validationRegex": null
      }
     }
    }
   }
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}