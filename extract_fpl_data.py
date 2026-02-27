import os
import json
import requests
from google.cloud import storage
from google.api_core import exceptions

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "fpl-gcp-key.json"

bucket_name = "fpl-raw-data-lake"
destination_path = "data/fpl-raw-api-data.json"

def fetch_fpl_data():
    print("Fetching FPL data...")
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"

    try:
        response = requests.get(url)
        response.raise_for_status()

        data = response.json() 
        print(f"Data fetched succesfully")

        return data

    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data from FPL. Error: {e}")
        return False

def load_to_storage(bucket_name, destination_path, fetched_data):
    print("Uploading data to GCP...")
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob =  bucket.blob(destination_path)
        blob.upload_from_string(json.dumps(fetched_data))
        print(f"Succesfully uploaded to {destination_path}")
    
    except Exception as e:
        print(f"Unexpected error occured during upload: {e}")


def run_fpl_pipeline():
    fetched_data = fetch_fpl_data()
    if fetched_data:
        load_to_storage(bucket_name, destination_path, fetched_data)

if __name__ == "__main__":
    run_fpl_pipeline()

