import os, json, asyncio
from datetime import datetime, date
from google.cloud import storage, bigquery
import pandas_gbq, pandas as pd
import logging, google.cloud.logging, uuid
from dotenv import load_dotenv
load_dotenv()

from src.extract.fpl_api import fetch_player_histories
from src.load.gcs_functions import load_to_storage
from src.load.bq_functions import gcs_to_bq
from src.transform.bq_functions import merge

def fetch_data(logging_context):
    logging_context["pipeline_phase"] = "dbt_transforms"
    logging.info(f"Beginning all player data pipeline.", extra={"json_fields":logging_context})

    try:
        bq_client = bigquery.Client()

        base_path = os.path.dirname(os.path.dirname(__file__))
        sql_file = os.path.join(base_path, 'src', 'queries', 'all_player_ids.sql')

        with open(sql_file, 'r') as file:
            query = file.read()

        query_job = bq_client.query(query)

        player_urls = [f"https://fantasy.premierleague.com/api/element-summary/{row['player_id']}/" for row in query_job]
        url_count = len(player_urls)

    except Exception:
        logging.critical(
            "Failed to extract player ids from BigQuery. Stopping pipeline.",
            exc_info=True,
            extra={"json_fields":logging_context}
        )
        return
    
    logging.info(f"Successfully fetched {url_count} player urls.", extra={"json_fields":logging_context})

    logging_context["pipeline_phase"] = "data_extraction"
    logging_context["player_count"] = url_count
    
    player_histories, failures = asyncio.run(fetch_player_histories(player_urls, logging_context))
    failure_count = len(failures)
    failed_urls = [item[0] for item in failures]
    
    if len(player_histories) == 0:
        logging.critical(f"Failed to collect any player data. Stopping pipeline.", extra={"json_fields":logging_context})
        return

    elif failure_count == 0:
        logging.info(f"Successfully collected data for all players.", extra={"json_fields":logging_context})

    else:
        logging.warning(f"Failed to collect data for {failure_count} urls: {failed_urls}. Continuing...", extra={"json_fields":logging_context})
    
    logging_context["failure_count"] = failure_count
    logging_context["failed_urls"] = failed_urls

    if failures:
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
            failure_data = [{"URL" : url, "timestamp" : timestamp, "reason":reason} for url, reason in failures]
            failed_df = pd.DataFrame(failure_data)
            failed_table = "fpl-analytics-488811.raw_player.failed_data_collection"
            pandas_gbq.to_gbq(failed_df, failed_table, if_exists='append')
            logging.info(f"Uploaded failed urls to {failed_table}.", extra={"json_fields":logging_context})
        
        except Exception:
            logging.warning(
                "Couldn't load failed urls to BigQuery. Continuing...",
                exc_info=True,
                extra={"json_fields":logging_context}
            )

    current_date = date.today().isoformat()
    gcs_path = f"raw-fpl-player-stats/raw-stats-{current_date}.json"
    logging_context["pipeline_phase"] = "uploading_data"
    
    try:
        client = storage.Client()
        bucket = client.bucket(os.getenv("GCS_BUCKET_NAME"))
        load_to_storage(bucket, gcs_path, player_histories)
    
    except Exception:
        logging.critical(
            "Couldn't upload player data to GCS. Stopping pipeline.",
            exc_info=True,
            extra={"json_fields":logging_context}
        )
        return
    
    logging.info(f"Successfully uploaded player data to GCS.", extra={"json_fields":logging_context})

    target_id = "fpl-analytics-488811.raw_player.raw_player_stats"

    try:
        gcs_to_bq(gcs_path, bucket, target_id)
    
    except Exception:
        logging.critical(
            f"Couldn't upload player data to {target_id}. Stopping pipeline.",
            exc_info=True,
            extra={"json_fields":logging_context}
        )
        return
    
    logging.info(f"Successfully uploaded player data to {target_id}.", extra={"json_fields":logging_context})
    
    logging_context["pipeline_phase"] = "complete"
    logging.info(f"Pipeline execution complete.", extra={"json_fields":logging_context})
    return True

def main(logging_context):
    fetch_data(logging_context)

if __name__ == "__main__":
    main()