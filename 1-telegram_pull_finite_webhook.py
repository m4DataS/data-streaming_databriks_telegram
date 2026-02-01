# COMMAND ----------

import json
from datetime import datetime
from pathlib import Path
import base64

# Receive the full JSON string
dbutils.widgets.text("telegram_message", "")
raw_input = dbutils.widgets.get("telegram_message")

DATA_DIR = "/Volumes/workspace/telegram_qa/raw_telegram/data/"

if raw_input:
    try:
        # 1. Decode the Base64 string back into bytes, then to a UTF-8 string
        decoded_bytes = base64.b64decode(raw_input)
        json_string = decoded_bytes.decode("utf-8")
        
        # 2. Parse the resulting string into a dictionary
        full_data = json.loads(json_string)
    
        # Extract info from 'message' or 'channel_post'
        msg_obj = full_data.get("message") or full_data.get("channel_post") or {}
        chat_obj = msg_obj.get("chat") or {}

        # Map the fields to your Bronze Schema
        processed_msg = {
            "update_id": full_data.get("update_id"),
            "message_id": msg_obj.get("message_id"),
            "conversation_type": chat_obj.get("type"),
            "channel_id": chat_obj.get("id"),
            "channel_title": chat_obj.get("title"),
            "text": msg_obj.get("text"),
            "date": msg_obj.get("date"),  # Using Telegram's original timestamp
            "author_signature": msg_obj.get("author_signature"),
        }

        # Save as a list of one object for your loader
        msg_json_list = [processed_msg]

        Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
        ts = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
        file_path = f"{DATA_DIR}/telegram_{ts}.json"

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(msg_json_list, f, ensure_ascii=False)

        print(f"✅ Saved message {processed_msg['message_id']} to {file_path}")
    
    except Exception as e:
        print(f"❌ Error during decoding or parsing: {e}")
        # Log the raw input to see what actually arrived
        print(f"Raw input received: {raw_input[:50]}...")