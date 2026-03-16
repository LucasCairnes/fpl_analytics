import os
from datetime import date
from google.cloud import storage
from dotenv import load_dotenv
load_dotenv()

from src.extract.fpl_api import fetch_fpl_data
from src.transform.utils import dig
from src.load.gcs_functions import load_to_storage
from src.load.bq_functions import gcs_to_bq

api_urls = ["https://fantasy.premierleague.com/api/bootstrap-static/",
            "https://fantasy.premierleague.com/api/fixtures/"]

current_date = date.today().isoformat()

gcs_paths = [(f"raw-fpl-player/raw-players-{current_date}.json", [0, "elements"]),
             (f"raw-fpl-team/raw-teams-{current_date}.json", [0, "teams"]),
             (f"raw-fpl-fixture/raw-fixtures-{current_date}.json", [1]),
             (f"raw-fpl-json/raw-fpl-{current_date}.json", [0])]

bq_tables = ["fpl-analytics-488811.raw_player_data.full_player_data",
             "fpl-analytics-488811.raw_team_data.full_team_data",
             "fpl-analytics-488811.raw_fixture_data.full_fixture_data"] 

raw_responses = [fetch_fpl_data(url) for url in api_urls]
raw_data = [data for data in raw_responses if data]

if raw_data:
    client = storage.Client()
    bucket = client.bucket(os.getenv("GCS_BUCKET_NAME"))

    for path in gcs_paths:
        load_to_storage(bucket, path[0], dig(raw_data, path[1]))

    for path, table in zip(gcs_paths[:3], bq_tables):
        gcs_to_bq(path[0], bucket, table)
