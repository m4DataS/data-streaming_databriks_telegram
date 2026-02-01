# data-streaming_databriks_telegram
End-to-end streaming pipeline from Telegram to Databricks: 
* collects channel messages via webhook, 
* processes them with a large language model, 
* and stores raw data in volumes & enriched data in Delta tables. 

Demonstrates PySpark, structured streaming, API integration, and failure-resilient batch processing.

## 🎯 Project Objective
The goal of this project is to showcase a modern, high-resilience data engineering stack. It demonstrates mastery in:
* Real-time Data Streaming: Orchestrating Structured Streaming to handle live API data.
* Third-Party API Integration: Managing Telegram Bot API interactions and robust error handling. + handling cost redution by setting webhook using python endpoint (Railway)
* LLM integration : Pass messages send by telegram's channel's users to a LLM with the role of supporting them by answering their questions.
* Infrastructure as Code (IaC): Using Databricks Asset Bundles (DABs) to ensure reproducible environments and automated deployments.

## 🚀 Deployment Strategy (for the most simple version : non-cost optimized)

### Important information
As the project is a bit big (composed of several part). Each are numeroted in the order you must follow to fully build the project. And each is supported by a .md doc to explain how the code is designed and works.
To simplify the setup, this project uses Databricks Asset Bundles to run a continuous polling version of the pipeline. 

This removes the need for external hosting (Railway) while maintaining a resilient, live connection to Telegram

But if you want the associated codes (due to cost issues are for other reasons) are provided : 
 * railway app is '1-main.py' 
 * the databricks notebook linked to it is '1-telegram_pull_finite_webhook.py'

### How to build the naive (not cost optimized) version
1. Configuration
Insert the names and credentials related to your databricks workspace into variables.yml:
 * bot_token: Your Telegram Bot API Key.
 * channel_id: The ID of the Telegram channel to monitor.
 * catalog_name / schema_name: Your Unity Catalog destinations.
 * ...

2. Infra Deployment (The Foundation)
 * Configure databricks authentication to your workspace.
 * Deploy the storage layer first (Volumes and Tables).
 ```bash
    cd infra/
    databricks bundle deploy --profile dev
 ```
 The infra.yml within this folder builds the Catalog resources (Volumes for raw JSONs and the job creating the Bronze Delta table empty) to prepare the landing zone.

3. Application Deployment (The Logic)
 * Re-uses the previous databricks authentication.
 * Deploy the polling and processing jobs.
 ```bash
    cd ..
    databricks bundle deploy --profile dev
 ```
 This command deploys 3 jobs: the polling script (be careful, he is paused, but it's a continuous job, so cost can escalade pretty quickly), the streaming transformation, and the LLM enrichment tasks.
 
### 🏃 Operation
1. Send a Message: Post a message to your configured Telegram channel.
2. Activate: Go to the Workflows tab in your Databricks workspace.
3. Unpause: Locate the polling job and click "Unpause" to begin the live stream.
4. Verify: Check your Delta tables to see the data flow from raw JSON to enriched insights.

(If you want to config a railway endpoint and ingest data : do not forget to activate telegram webhook & use '1-telegram_pull_finite_webhook' instead of '0-telegram_pull_finite.py')
