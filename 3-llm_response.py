# Databricks notebook source
# MAGIC %md
# MAGIC # Prepare archi

# COMMAND ----------

# MAGIC %md
# MAGIC # LLM config

# COMMAND ----------

# MAGIC %pip install openai

# COMMAND ----------

from openai import OpenAI
import os
import pandas as pd
import json
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# COMMAND ----------

import requests


# COMMAND ----------

dbutils.widgets.text("telegram_token", "")
dbutils.widgets.text("telegram_chat_id", "")
dbutils.widgets.text("databricks_token", "")
dbutils.widgets.text("databricks_model_url", "")
dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("volume_name", "")
dbutils.widgets.text("bronze_table", "")
dbutils.widgets.text("silver_table", "")
telegram_token = dbutils.widgets.get("telegram_token")
telegram_chat_id = dbutils.widgets.get("telegram_chat_id")
databricks_token = dbutils.widgets.get("databricks_token")
databricks_model_url = dbutils.widgets.get("databricks_model_url")
CATALOG_NAME = dbutils.widgets.get("catalog_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
VOLUME_NAME = dbutils.widgets.get("volume_name")
BRONZE_TABLE = dbutils.widgets.get("bronze_table")
SILVER_TABLE = dbutils.widgets.get("silver_table")
# To update to use job secret

# COMMAND ----------

# MAGIC %md
# MAGIC # Run 

# COMMAND ----------

def process_batch(df, batch_id):
    # Note: process_batch is called by Spark foreachBatch with exactly 2 arguments: (df, batch_id)
    # Any other parameters (tokens, model URLs, etc.) must be captured from the outer scope (closure)
    """
    Process a micro-batch of new Bronze messages:
    - Call LLM to generate answer
    - Save answer to Silver table
    - Send reply back to Telegram
    """
    if df.count() == 0:
        return

    pdf = df.select(
        "update_id",
        "message_id",
        "channel_id",
        "conversation_type",
        "channel_title",
        "author_signature",
        "text"
    ).toPandas()

    rows = []

    # Create LLM client inside the function
    from openai import OpenAI
    import os

    client = OpenAI(api_key=databricks_token, base_url=databricks_model_url)

    # Telegram helper defined inside the function
    import requests
    def send_telegram_message(text: str):
        url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
        payload = {"chat_id": telegram_chat_id, "text": text}
        response = requests.post(url, data=payload)
        if response.status_code != 200:
            print("Error:", response.text)

    # Define the ai_answering function inside
    def ai_answering(user_message: str):
        """
        Send user_message to the LLM and return a JSON object:
        {
            "answer": "model response text"
        }
        """
        prompt = f"""
    You are a helpful assistant. Respond to the user's message below clearly and concisely.
    Always respond in the SAME LANGUAGE as the user message.
    If you do not understand the message, respond with : "Sorry, I don't know the answer. But I would be glad to hear more"
    If you do not understand the user message, respond with "Sorry, I couldn't generate a response."
    Only return one answer. Do not provide translations or alternatives.

    User message:
    {user_message}

    Return the response in this exact JSON format:
    {{
        "answer": "This is an AI intervention !\\nMessage: {user_message}\\nResponse: <your response here>"
    }}
    """
        try:
            response = client.chat.completions.create(
                model="databricks-gpt-oss-120b",
                messages=[{"role": "user", "content": prompt}]
            )
            # Extract model text
            model_text = response.choices[0].message.content[1]['text']
            # Parse JSON safely
            parsed = json.loads(model_text)
            return parsed.get("answer", "No response")
        except Exception as e:
            print("LLM error:", e)
            return "Internal error"

    def sanitize_row(row):
        """
        Ensure each field matches the schema type.
        """
        try:
            return {
                "update_id": int(row.get("update_id", 0)),
                "message_id": int(row.get("message_id", 0)),
                "channel_id": int(row.get("channel_id", 0)),
                "conversation_type": str(row.get("conversation_type", "")),
                "channel_title": str(row.get("channel_title", "")),
                "author_signature": str(row.get("author_signature", "")),
                "user_message": sanitize_llm_response(row.get("user_message")),
                "llm_response": sanitize_llm_response(row.get("llm_response")),
                "model_name": str(row.get("model_name", "")),
                "batch_id": int(row.get("batch_id", 0)),
                "processed_at": pd.Timestamp.utcnow()
            }
        except Exception as e:
            print("❌ Failed to sanitize row:", row, e)
            return None  # will be filtered out

    for _, row in pdf.iterrows():
        user_text = row["text"]
        # print("User message:", user_text)
        llm_response = ai_answering(user_text)
        # print("LLM response:", llm_response)
        send_telegram_message(llm_response)

        rows.append({
            "update_id": row["update_id"],
            "message_id": row["message_id"],
            "channel_id": row["channel_id"],
            "conversation_type": row["conversation_type"],
            "channel_title": row["channel_title"],
            "author_signature": row["author_signature"],
            "user_message": user_text,
            "llm_response": llm_response,
            "model_name": "databricks-gpt-oss-120b",
            "processed_at": pd.Timestamp.utcnow(),
            "batch_id": batch_id
        })

    if rows:
        from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType
        # Define the schema for Silver table
        silver_schema = StructType([
            StructField("update_id", LongType(), True),
            StructField("message_id", LongType(), True),
            StructField("channel_id", LongType(), True),
            StructField("conversation_type", StringType(), True),
            StructField("channel_title", StringType(), True),
            StructField("author_signature", StringType(), True),
            StructField("user_message", StringType(), True),
            StructField("llm_response", StringType(), True),
            StructField("model_name", StringType(), True),
            StructField("batch_id", LongType(), True),
            StructField("processed_at", TimestampType(), True)
        ])

        # Sanitize rows and skip invalid ones
        rows_sanitized = [r for r in (sanitize_row(r) for r in rows) if r is not None]

        if rows_sanitized:
            df_silver = spark.createDataFrame(rows_sanitized, schema=silver_schema)
            df_silver.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.{SILVER_TABLE}")

# COMMAND ----------

bronze_stream_df = spark.readStream.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.{BRONZE_TABLE}")

query = (
    bronze_stream_df.writeStream
        .trigger(availableNow=True)  # micro-batch, stops after current rows
        .option("checkpointLocation", f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}/checkpoint/llm")
        .foreachBatch(process_batch)
        .start()
)
