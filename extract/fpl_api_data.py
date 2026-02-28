import os
import json
import requests
from datetime import date
from google.cloud import storage
from google.api_core import exceptions

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/sparelaptop4/Documents/fpl_analytics/extract/storage_service.json"

storage_client = storage.Client()
bucket_name = "fpl-raw-data-lake"
current_date = date.today().isoformat()

full_json_path = f"raw-fpl-api-data/raw-fpl-{current_date}.json"
player_json_path = f"raw-fpl-player-data/raw-players-{current_date}.json"
team_json_path = f"raw-fpl-team-data/raw-teams-{current_date}.json"
fixture_json_path = f"raw-fpl-fixture-data/raw-fixtures-{current_date}.json"

def fetch_fpl_data():
    print("Fetching FPL data...")
    static_url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    fixture_url = "https://fantasy.premierleague.com/api/fixtures/"

    try:
        response = requests.get(static_url)
        response.raise_for_status()
        static_data = response.json() 

        response = requests.get(fixture_url)
        response.raise_for_status()
        fixture_data = response.json() 

        print(f"Data fetched succesfully")
        return [static_data, fixture_data]

    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch FPL data, error: {e}")
        return False

def load_to_storage(storage_client, bucket_name, destination_path, raw_data):
    try:
        print(f"Uploading to {destination_path}...")
        bucket = storage_client.get_bucket(bucket_name)
        blob =  bucket.blob(destination_path)
        blob.upload_from_string(json.dumps(raw_data))
        print(f"Upload successful.")

    except Exception as e:
        print(f"An error occured during upload: {e}, continuing...")

def run_fpl_pipeline():
    raw_data = fetch_fpl_data()
    if raw_data:
        load_to_storage(storage_client, bucket_name, full_json_path, raw_data[0])
        load_to_storage(storage_client, bucket_name, player_json_path, raw_data[0]["elements"])
        load_to_storage(storage_client, bucket_name, team_json_path, raw_data[0]["teams"])
        load_to_storage(storage_client, bucket_name, fixture_json_path, raw_data[1])

if __name__ == "__main__":
    run_fpl_pipeline()

