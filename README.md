# ⚽ FPL Analytics:

An Fantasy Premier League ELT pipeline built with Python, dbt, and Google Cloud Platform. 

**[View the Looker Studio Dashboard Here](https://datastudio.google.com/u/0/reporting/e2ae6fe0-ee44-4414-9cac-730268780c6e/page/quJtF)**

## Project Overview
Daily, this pipeline extracts player, team and fixture data from the Fantasy Premier League API with serverless architecture (Google Cloud Run). This data is then transformed with dbt, compiling roughly 300,000 data points into a dynamic Looker Studio dashboard that tracks player form, expected goals (xG), fixture difficulty, and more.

## Key Engineering Features
* **Asynchronous Ingestion:** Used `asyncio` and `aiohttp` to asynchronously fetch player data, significantly reducing data collection time while safely navigating API rate limits.
* **Robust Error Handling:** Built a fault-tolerant system that survives network instability and flaky APIs. Used `Tenacity` to protect against flaky apis with retry logic. Created a Dead Letter Queue to route failed requests to a BigQuery table without halting the main pipeline. Implemented robust and informative Google Cloud logging.
* **Automation & ELT Architecture:** The pipeline containerised with Docker and deployed serverlessly on Google Cloud Run, enabling completely hands-off data collection. Once the data lands Google Cloud Storage, dbt orchestrates a 3-tier data model (raw, staging, curated) in BigQuery, utilising custom Jinja macros.

---

## Repository Structure
* `code/main.py` — The central orchestrator for the pipeline.
* `code/scripts/` — Contains data extraction pipelines and the dbt runner.
* `code/src/` — Contains the modular, custom ELT functions.
* `code/fpl_dbt/` — The dbt project containing all SQL models and custom macros.

---

## Requirements
* Python 3.13+
* `dbt-bigquery`
* An active Google Cloud Project

---

## Setup & Execution

**1. Clone Repo and Install Dependencies:**
Clone the repo, create a virtual environment and install the required packages.
* `git clone https://github.com/LucasCairnes/fpl_analytics/`
* `conda create -n "fpl_analytics" python=3.13`
* `conda activate fpl_analytics`
* `pip install -r requirements.txt`

**2. Configure GCP Credentials:**
Set up your Google credentials to authenticate with BigQuery and GCS.
* `export GOOGLE_APPLICATION_CREDENTIALS="/path/to/key.json"`

**3. Configure dbt:**
Edit fpl_dbt/profiles.yml to point to your specific GCP project (`PROJECT_ID`) and default dataset (`BQ_DATASET`).

**4. Load Initial Player Backlog:**
Before running the standard pipeline, execute the script to fetch the full player data backlog.
* `cd code`
* `python -m scripts.load_all_player_data`

**5. Run the Main Pipeline:**
Execute the main orchestrator to the rest of the data.
* `python -m main`



