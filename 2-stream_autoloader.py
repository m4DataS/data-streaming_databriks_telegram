# Databricks notebook source
dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("volume_name", "")
dbutils.widgets.text("bronze_table", "")
CATALOG_NAME = dbutils.widgets.get("catalog_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
VOLUME_NAME = dbutils.widgets.get("volume_name")
BRONZE_TABLE = dbutils.widgets.get("bronze_table")

# COMMAND ----------

# Databricks notebook source
files = dbutils.fs.ls(f"dbfs:/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}/data")
if len(files) == 0:
    print("No new files, exiting job.")
    dbutils.notebook.exit("No new files")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")


# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.{BRONZE_TABLE} (
    update_id BIGINT,
    message_id BIGINT,
    author_signature STRING,
    conversation_type STRING,
    channel_id BIGINT,
    channel_title STRING,
    text STRING,
    date BIGINT
)
USING DELTA
""")


# COMMAND ----------

# MAGIC %md
# MAGIC # Auto Loader

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

# MAGIC %md
# MAGIC ## Var definition

# COMMAND ----------

# ----------------------------
# Paths
# ----------------------------
# RAW_PATH: location where JSON files arrive (from Telegram polling job)
# CHECKPOINT_PATH: required for Auto Loader to maintain state and ensure exactly-once ingestion
RAW_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}/data/"
CHECKPOINT_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}/checkpoint/autoloader"

# COMMAND ----------

# ----------------------------
# Define the schema for Bronze ingestion
# ----------------------------
# Note: All fields are marked as nullable (True) intentionally.
# This is a common pattern for Bronze/raw tables because:
# 1. Telegram messages may sometimes miss fields (e.g., text could be None for media-only messages).
# 2. Bronze layer aims for maximum ingestion reliability, not strict validation.
# 3. Data validation and enforcement will be done in downstream Silver/Gold tables.
telegram_schema = StructType([
    StructField("update_id", LongType(), True),
    StructField("message_id", LongType(), True),
    StructField("channel_id", LongType(), True),
    StructField("conversation_type", StringType(), True),
    StructField("channel_title", StringType(), True),
    StructField("author_signature", StringType(), True),
    StructField("text", StringType(), True),
    StructField("date", LongType(), True)
])


# COMMAND ----------

# MAGIC %md
# MAGIC ## Read stream

# COMMAND ----------

# ----------------------------
# Read streaming data using Auto Loader
# ----------------------------
# Using schema enforcement ensures the table stays consistent even if new files have slightly different structure.
# We use 'cloudFiles.format' = 'json' since incoming files are JSON from the polling job.
bronze_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true") # Add this if your files are JSON arrays
        .schema(telegram_schema)
        .load(RAW_PATH)
)

# ----------------------------
# Enrich Bronze data with ingestion metadata
# ----------------------------
# _ingested_at: timestamp when the row is ingested
# _source_file: which JSON file produced this row (useful for auditing and debugging)
bronze_df_enriched = (
    bronze_df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
)

# ----------------------------
# Write streaming data to Bronze Delta table
# ----------------------------
# - trigger(availableNow=True) ensures the job processes all currently available files and then stops.
#   This is perfect for a scheduled job every 5 minutes (cost-efficient, Free-tier compatible).
# - checkpointLocation maintains exactly-once processing across job runs.
# - outputMode('append') appends new rows to the Delta table.
# - fully qualified table name ensures the table is created/used in the correct schema.
(
    bronze_df_enriched.writeStream
        # .trigger(processingTime="5 seconds")  # micro-batch every 5 seconds 
        .trigger(availableNow=True)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .option("mergeSchema", "true")  # <-- allows new columns to be added automatically
        .table(f"{CATALOG_NAME}.{SCHEMA_NAME}.{BRONZE_TABLE}")
)

# COMMAND ----------

# # Show the last 10 rows of the Bronze table
# spark.sql("""
# SELECT *
# FROM workspace.telegram_qa.bronze_telegram_messages
# ORDER BY _ingested_at DESC
# LIMIT 10
# """).show(truncate=False)