# FPL Analytics

A Fantasy Premier League data analysis project built with Python and dbt.

## Structure
- code/main.py - the main orchestrator for the pipeline
- code/scripts/ — contains the data extraction pipelines along and script to carry out dbt transforms
- code/src/ — contains all used ELT functions and all used sql queries
- code/fpl_dbt/ — contains the dbt models, macros, and sources

## Requirements
- Python 3.13+
- dbt
- An active google cloud project

## Setup
1. Create a venv and install the dependencies:
    - `conda create -n "fpl_analytics" python=3.13`
    - `pip install -r requirements.txt`

2. Configure dbt connection:
    - Edit fpl_dbt/profiles.yml

3. Configure GCP credentials
    - GOOGLE_APPLICATION_CREDENTIALS="/path/to/key.json"

4. To run the full pipeline
    - `cd code`
    - `python -m scripts.main`

