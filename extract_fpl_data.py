import os
import json
import requests
from google.cloud import storage
from google.api_core import exceptions

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "fpl-gcp-key.json"

bucket_name = "fpl-raw-data-lake"
source_path = "fpl-raw-data.json"
destination_path = "data/fpl-raw-api-data.json"

def fetch_fpl_data(source_path):
    print("Fetching FPL data...")
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"

    try:
        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
            
        with open(source_path, "w") as file:
            json.dump(data, file)     

        print(f"Data fetched and saved succesfully to {source_path}")
        return True

    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data from FPL. Error: {e}")
        return False

def load_to_storage(bucket_name, destination_path, source_path):
    print("Uploading data to GCP...")
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob =  bucket.blob(destination_path)
        blob.upload_from_filename(source_path)
        print(f"Succesfully uploaded {source_path} to {destination_path}")

        if os.path.exists(source_path):
            os.remove(source_path)

        print("Pipeline executed succesfully.")
    
    except Exception as e:
        print(f"Unexpected error occured during upload: {e}")


def run_fpl_pipeline():
    fetched = fetch_fpl_data(source_path)
    if fetched:
        load_to_storage(bucket_name, destination_path, source_path)

if __name__ == "__main__":
    run_fpl_pipeline()

