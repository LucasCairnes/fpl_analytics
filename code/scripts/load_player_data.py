import os
from datetime import date
from google.cloud import storage
import asyncio
from dotenv import load_dotenv
load_dotenv()

from src.extract.fpl_api import fetch_player_histories
from src.load.gcs_functions import load_to_storage
from src.load.bq_functions import gcs_to_bq

player_histories = asyncio.run(fetch_player_histories())

current_date = date.today().isoformat()
gcs_path = f"raw-fpl-player-stats/raw-stats-{current_date}.json"

client = storage.Client()
bucket = client.bucket(os.getenv("GCS_BUCKET_NAME"))

table_id = "fpl-analytics-488811.raw_player.raw_player_stats"

load_to_storage(bucket, gcs_path, player_histories)
gcs_to_bq(gcs_path, bucket, table_id)
