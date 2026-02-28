import json
import requests
from google.cloud import storage
from google.api_core import exceptions

from extract.gcp_auth import get_fpl_bucket, get_fpl_api_paths, dig

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

def load_to_storage(bucket, destination_path, raw_data):
    try:
        print(f"Uploading to {destination_path}...")
        blob = bucket.blob(destination_path)
        blob.upload_from_string(json.dumps(raw_data), content_type="application/json")
        print(f"Upload successful.")

    except Exception as e:
        print(f"An error occured during upload: {e}, continuing...")

def run_fpl_pipeline():
    raw_data = fetch_fpl_data()
    if raw_data:
        bucket = get_fpl_bucket()
        for path in get_fpl_api_paths(["all"]):
            load_to_storage(bucket, path[0], dig(raw_data, path[1]))

if __name__ == "__main__":
    run_fpl_pipeline()

