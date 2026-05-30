import os
import requests
import logging

def send_slack_summary(status, logging_context):
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    run_id = logging_context.get("run_id")
    phase = logging_context.get("pipeline_phase")

    if status == "success":
        color = "#36a64f" # Green
        title = "FPL Pipeline: Success"
        text = f"*Run ID:* {run_id}\n Pipeline executed successfully."
    
    else:
        color = "#D30D0D" # Red
        title = "FPL Pipeline: Failed"
        text = f"*Run ID:* {run_id}\n*Failed Phase:* {phase}\n."

    payload = {
        "attachments": [
            {
                "color": color,
                "title": title,
                "text": text
            }
        ]
    }

    try:
        requests.post(webhook_url, json=payload, timeout=10)
    except Exception as e:
        logging.error(f"Failed to send Slack message: {e}")