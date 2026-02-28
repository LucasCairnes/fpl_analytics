import os
from google.cloud import storage
from datetime import date
from google.oauth2 import service_account

def get_fpl_bucket():
    bucket_name = "fpl-analytics-data-lake"
    cred_path = "/Users/sparelaptop4/Documents/fpl_analytics/extract/storage_admin_key.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path
    client = storage.Client()
    
    return client.bucket(bucket_name)

def get_fpl_api_paths(requested_paths):
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

def dig(data, indices):
    for index in indices:
        data = data[index]
    return data

def get_credentials():
    return service_account.Credentials.from_service_account_file("/Users/sparelaptop4/Documents/fpl_analytics/extract/storage_admin_key.json")



