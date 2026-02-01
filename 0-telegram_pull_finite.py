# Databricks notebook source
import requests
import json
from datetime import datetime
from pathlib import Path

# COMMAND ----------

dbutils.widgets.text("telegram_token", "")
dbutils.widgets.text("telegram_chat_id", "")
dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("volume_name", "")
TOKEN = dbutils.widgets.get("telegram_token")
CHAT_ID = dbutils.widgets.get("telegram_chat_id")
CATALOG_NAME = dbutils.widgets.get("catalog_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
VOLUME_NAME = dbutils.widgets.get("volume_name")
# To update to use job secret


BASE_URL = f"https://api.telegram.org/bot{TOKEN}"
DATA_DIR = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}/data/"
OFFSET_FILE = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}/checkpoint/offset.txt"

# COMMAND ----------

def load_offset():
    try:
        with open(OFFSET_FILE, "r") as f:
            return int(f.read())
    except:
        return None


def save_offset(offset: int):
    Path(OFFSET_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(OFFSET_FILE, "w") as f:
        f.write(str(offset))


def get_channel_messages(offset=None):
    url = f"{BASE_URL}/getUpdates"
    params = {}
    if offset:
        params["offset"] = offset

    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()


def run_once():
    offset = load_offset()
    data = get_channel_messages(offset)

    messages = []
    new_offset = offset

    for update in data.get("result", []):
        new_offset = update["update_id"] + 1

        if "channel_post" in update:
            post = update["channel_post"]
            messages.append({
                "update_id": update["update_id"],
                "message_id": post["message_id"],
                "conversation_type": post["chat"]["type"],
                "channel_id": post["chat"]["id"],
                "author_signature": post["author_signature"],
                "channel_title": post["chat"]["title"],
                "text": post.get("text"),
                "date": post["date"]
            })

    if messages:
        Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
        ts = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
        file_path = f"{DATA_DIR}/telegram_{ts}.json"

        with open(file_path, "w") as f:
            json.dump(messages, f)
        # # Write each message as a separate line (NDJSON)
        # with open(file_path, "w", encoding="utf-8") as f:
        #     for msg in messages:
        #         f.write(json.dumps(msg, ensure_ascii=False) + "\n")

        print(f"✅ Wrote {len(messages)} messages to {file_path}")

    if new_offset:
        save_offset(new_offset)

    print("🏁 Job finished")


if __name__ == "__main__":
    run_once()
