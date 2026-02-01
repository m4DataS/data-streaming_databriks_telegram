from fastapi import FastAPI, Request
import os
import httpx
import json
import base64

app = FastAPI()

DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_INSTANCE = os.getenv("DATABRICKS_INSTANCE")
JOB_NAME = os.getenv("JOB_NAME")

@app.post("/telegram-webhook")
async def telegram_webhook(req: Request):
    data = await req.json()

    # This will show the pretty-printed JSON in your Railway logs
    print("🔔 Received Telegram JSON:", json.dumps(data, indent=2, ensure_ascii=False))
    
    # 1. Convert dict to JSON string
    json_str = json.dumps(data, ensure_ascii=False)
    
    # 2. Encode to Base64 (This turns "données" into safe ASCII like "ZG9ubsOpZXM=")
    encoded_param = base64.b64encode(json_str.encode("utf-8")).decode("ascii")


    # 1. Stringify the data
    # We must ensure this is a single string for the Databricks parameter

    async with httpx.AsyncClient(timeout=60.0) as client:
        # Get job list (Consider hardcoding JOB_ID to speed this up!)
        jobs_list_resp = await client.get(
            f"{DATABRICKS_INSTANCE}/api/2.1/jobs/list",
            headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
        )
        jobs_list = jobs_list_resp.json()
        job_id = next(job["job_id"] for job in jobs_list.get("jobs", []) if job["settings"]["name"] == JOB_NAME)

        # 2. Trigger Job
        response = await client.post(
            f"{DATABRICKS_INSTANCE}/api/2.1/jobs/run-now",
            headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
            json={
                "job_id": int(job_id),
                "notebook_params": {
                    "telegram_message": encoded_param # Send the encoded string
                }
            }
        )
        
        # This will print WHY it failed (e.g., "Parameter 'telegram_message' is too long")
        if response.status_code != 200:
            print(f"❌ Databricks Error Detail: {response.text}")
        else:
            print(f"🚀 Success! Run ID: {response.json().get('run_id')}")

    return {"ok": True}