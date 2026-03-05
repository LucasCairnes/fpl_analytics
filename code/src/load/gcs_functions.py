from google.cloud import storage
from datetime import date
from google.oauth2 import service_account
import pandas as pd
import json

def get_fpl_bucket():
    bucket_name = "fpl-analytics-data-lake"
    client = storage.Client()
    
    return client.bucket(bucket_name)

def get_fpl_paths(requested_paths):
    if not requested_paths:
        requested_paths = ["all"]

    current_date = date.today().isoformat()
    path_map = {
                "full":[f"raw-fpl-json/raw-fpl-{current_date}.json", [0]],
                "player":[f"raw-fpl-player/raw-players-{current_date}.json", [0, "elements"]],
                "team":[f"raw-fpl-team/raw-teams-{current_date}.json", [0, "teams"]],
                "fixture":[f"raw-fpl-fixture/raw-fixtures-{current_date}.json", [1]]
                }
    
    if "all" in requested_paths:
        return list(path_map.values())
    
    if len(requested_paths) == 1:
        return path_map[requested_paths[0]]
    
    return [path_map[p] for p in requested_paths if p in path_map]

def load_to_storage(bucket, destination_path, raw_data):
    try:
        print(f"Uploading to {destination_path}...")
        blob = bucket.blob(destination_path)
        blob.upload_from_string(json.dumps(raw_data), content_type="application/json")
        print(f"Upload successful.")

    except Exception as e:
        print(f"An error occured during upload: {e}, continuing...")

def dig(data, indices):
    for index in indices:
        data = data[index]
    return data