import os
from datetime import date
from google.cloud import storage
from dotenv import load_dotenv
load_dotenv()

from src.extract.fpl_api import fetch_fpl_data
from src.load.gcs_functions import load_to_storage
from src.load.bq_functions import gcs_to_bq

current_date = date.today().isoformat()

fixture_url = "https://fantasy.premierleague.com/api/fixtures/"
fixture_gcs_path = f"raw-fpl-fixture/raw-fixtures-{current_date}.json"

raw_fixtures = fetch_fpl_data(fixture_url)

client = storage.Client()
bucket = client.bucket(os.getenv("GCS_BUCKET_NAME"))

fixture_bq_table = "fpl-analytics-488811.raw_fixture.raw_fixture"

load_to_storage(bucket, fixture_gcs_path, raw_fixtures)
gcs_to_bq(fixture_gcs_path, bucket, fixture_bq_table)

