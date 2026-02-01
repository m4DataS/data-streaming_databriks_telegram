# Databricks notebook source
dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("bronze_table", "")
CATALOG_NAME = dbutils.widgets.get("catalog_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
BRONZE_TABLE = dbutils.widgets.get("bronze_table")

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