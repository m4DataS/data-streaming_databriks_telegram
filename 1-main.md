# 🚀 telegram-webhook-databricks

### Event-driven Telegram → Databricks LLM pipeline using webhooks and Delta Lake

This project demonstrates a cost-efficient, event-driven architecture that connects Telegram messages to Databricks for real-time ingestion, processing, and LLM-powered responses.

Instead of running always-on streaming jobs, the pipeline is triggered only when new Telegram messages arrive, making it suitable for low-cost or free-tier environments.

## 🧠 Architecture Overview

1. Telegram Channel → Webhook → Databricks → LLM → Telegram Reply
 * Telegram Bot Webhook
 * Hosted on Railway
 * Receives channel messages in real time
 * No polling, no long-running jobs
2. Raw Data Storage
 * Incoming messages are stored as JSON files
 * Written to a Databricks Volume (raw_telegram)
3. Bronze Layer (Delta Lake)
 * Databricks Autoloader ingests new files on arrival
 * Schema evolution & checkpointing handled automatically
4. Silver Layer (LLM Processing)
 * Triggered when Bronze table updates
 * Sends messages to a Databricks-hosted LLM
 * Stores AI responses in a Silver Delta table
 * Replies back to Telegram via Bot API
5. Fully Event-Driven
 * No infinite streaming loops
 * Compatible with Databricks Free Edition constraints

## ✨ Key Features
* 🔔 Webhook-based triggering (no polling)
* 🧱 Medallion architecture (Raw → Bronze → Silver)
* 🤖 LLM inference using Databricks models
* 💬 Automatic Telegram replies
* 💾 Delta Lake with exactly-once semantics
* 💸 Cost-optimized for free / low-tier environments

## 🛠️ Tech Stack
* Telegram Bot API
* Railway (Webhook hosting)
* Databricks
   * Delta Lake
   * Autoloader
   * Jobs & Triggers
* Python / PySpark
* LLM inference via Databricks endpoints

## 🎯 Why this project?
This project showcases:
* Real-world event-driven data engineering
* Practical LLM integration in production pipelines
* Cost-aware architecture decisions
* End-to-end ownership from ingestion to AI output
It’s designed as a portfolio project for Data / Platform / MLOps / DevOps roles.

## Code explanations

### The Decorator: @app.post("/telegram-webhook")
In Python, a decorator (the @ symbol) wraps a function to modify its behavior.
 * app: This refers to the instance of FastAPI you created at the top of your script (app = FastAPI()).
 * .post(): This tells FastAPI that this specific function should only trigger when an HTTP POST request is sent to the server. (Telegram sends updates via POST).
 * "/telegram-webhook": This is the path (or route). If your server is at https://my-app.railway.app, this function will only run when a request hits https://my-app.railway.app/telegram-webhook.
In short: It’s a "listener." It maps a specific URL and a specific HTTP method (POST) to your Python code.


### ⚡ Asynchronous Execution & Scalability
The webhook is built using `async def` and `httpx.AsyncClient` to ensure the system is **non-blocking**. 
* **`async def`**: This allows the FastAPI server to handle multiple incoming Telegram messages concurrently. Instead of waiting for one Databricks job to trigger before accepting the next message, the server handles requests on an "event loop."
* **`async with httpx.AsyncClient`**: This creates an asynchronous HTTP session. When the script calls the Databricks API, it uses `await`. This tells the CPU to "pause" this specific task and handle other incoming traffic while waiting for the network response from Databricks, making the bridge extremely lightweight and high-performance.
* **HTTPX**: Chosen over the standard `requests` library because it supports **asynchronous HTTP calls**. This is essential for maintaining the performance of the FastAPI event loop.
* **Await Logic**: The service `awaits` the response from the Databricks `/api/2.1/jobs/run-now` endpoint. This ensures that we can:
    1.  Confirm the job was successfully triggered.
    2.  Capture the `run_id` for logging.
    3.  Catch and log errors (like "Parameter too long" or "Unauthorized") immediately in the Railway logs.


### What is httpx?
httpx as the modern, more powerful sibling of the famous requests library.

In the Python world, requests is the gold standard for sending HTTP calls, but it has one major flaw: it is synchronous (blocking). It cannot be used with async/await.

httpx was created to solve this:
 * Async Support: It is designed specifically to work with async frameworks like FastAPI.
 * Speed: It allows you to send multiple requests at the same time without waiting for each one to finish sequentially.
 * HTTP/2: It supports newer web protocols that make communication faster.




## 🛠️ Future Improvements & Roadmap

While the current Webhook-to-Databricks bridge is functional and cost-efficient, the following enhancements would make the pipeline more robust:

### 1. Performance Optimization: Direct Job ID
* **Current State:** The script fetches the entire list of Databricks jobs and filters by name for every message.
* **Improvement:** Store the `DATABRICKS_JOB_ID` directly as an environment variable in Railway. This eliminates one API call per message, reducing latency by ~500ms and avoiding potential API rate limits.

### 2. Security: Webhook Authentication
* **Current State:** The `/telegram-webhook` endpoint is public. Anyone who knows the URL could potentially trigger the Databricks jobs.
* **Improvement:** * Implement a **Secret Token** check. Telegram allows you to pass an `X-Telegram-Bot-Api-Secret-Token` header.
    * Verify this token in the FastAPI app before processing the request.

### 3. Reliability: Message Queueing (DLQ)
* **Current State:** If Databricks is down or the API fails, the message is lost after the print statement.
* **Improvement:** Integrate a lightweight queue (like Redis or AWS SQS). The FastAPI app would drop the message into the queue, and a worker would ensure the Databricks job starts, retrying if necessary.

### 4. Payload Validation
* **Current State:** The script assumes the JSON structure from Telegram is always valid.
* **Improvement:** Use **Pydantic models** within FastAPI to validate the incoming Telegram schema. This would catch malformed data at the edge before it ever reaches the Databricks compute layer.

### 5. Efficient Compute: Serverless/SQL Warehouse
* **Current State:** Triggering a standard Job Cluster for every single message can lead to "Startup Latency" (3-5 mins).
* **Improvement:** * Use a **Serverless SQL Warehouse** for ingestion if the volume increases.
    * Or, batch messages in the Railway app and trigger the Databricks job only once every 5-10 messages.