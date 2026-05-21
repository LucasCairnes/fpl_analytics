import os
from datetime import date
from google.cloud import storage
import logging, google.cloud.logging, uuid
from dotenv import load_dotenv
load_dotenv()

from src.extract.fpl_api import fetch_fpl_data
from src.transform.utils import dig
from src.load.gcs_functions import load_to_storage
from src.load.bq_functions import gcs_to_bq

def fetch_data(logging_context):
    logging_context["pipeline_phase"] = "api_data_extraction"
    logging.info(f"Beginning api data pipeline.", extra={"json_fields":logging_context})

    api_urls = ["https://fantasy.premierleague.com/api/bootstrap-static/",
                "https://fantasy.premierleague.com/api/fixtures/"]

    current_date = date.today().isoformat()

    gcs_paths = [(f"raw-fpl-player/raw-players-{current_date}.json", [0, "elements"]),
                (f"raw-fpl-team/raw-teams-{current_date}.json", [0, "teams"]),
                (f"raw-fpl-fixture/raw-fixtures-{current_date}.json", [1]),
                (f"raw-fpl-json/raw-fpl-{current_date}.json", [0])]

    bq_tables = ["fpl-analytics-488811.raw_player.raw_player_data",
                "fpl-analytics-488811.raw_team.raw_team_data",
                "fpl-analytics-488811.raw_fixture.raw_fixture_data"]
    

    try:
        raw_responses = [fetch_fpl_data(url) for url in api_urls]
        raw_data = [data for data in raw_responses if data]

    except Exception:
        logging.critical(
            "Failed to extract api data. Stopping pipeline.",
            exc_info=True,
            extra={"json_fields":logging_context}
        )
        return False
    
    if len(raw_data) != len(api_urls):
        logging.critical(
            "Failed to extract all api data. Stopping pipeline.",
            exc_info=True,
            extra={"json_fields":logging_context}
        )
        return False

    logging.info(f"Successfully collected api data.", extra={"json_fields":logging_context})
    logging_context["pipeline_phase"] = "uploading_to_gcs"

    try:
        client = storage.Client()
        bucket = client.bucket(os.getenv("GCS_BUCKET_NAME"))
        for path in gcs_paths:
            load_to_storage(bucket, path[0], dig(raw_data, path[1]))

    except Exception:
        logging.critical(
            "Failed to upload data to gcs. Stopping pipeline.",
            exc_info=True,
            extra={"json_fields":logging_context}
        )
        return False
    
    logging.info(f"Successfully uploaded data to gcs.", extra={"json_fields":logging_context})
    logging_context["pipeline_phase"] = "uploading_to_bq"

    try:
        for path, table in zip(gcs_paths[:3], bq_tables):
            gcs_to_bq(path[0], os.getenv("GCS_BUCKET_NAME"), table)

    except Exception:
        logging.critical(
            "Failed to upload data to BigQuery. Stopping pipeline.",
            exc_info=True,
            extra={"json_fields":logging_context}
        )
        return False
    
    logging.info(f"Successfully uploaded data to BigQuery.", extra={"json_fields":logging_context})
    logging_context["pipeline_phase"] = "complete"
    logging.info(f"Pipeline execution complete.", extra={"json_fields":logging_context})
    return True
            
def main(logging_context):
    success = fetch_data(logging_context)
    if success:
        return True

if __name__ == "__main__":
    main()
