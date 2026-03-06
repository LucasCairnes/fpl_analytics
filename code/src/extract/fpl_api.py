import requests
from google.cloud import storage
from google.api_core import exceptions

from src.load.gcs_functions import get_fpl_bucket, get_fpl_paths, dig, load_to_storage

def fetch_fpl_data():
    print("Fetching FPL data...")
    static_url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    fixture_url = "https://fantasy.premierleague.com/api/fixtures/"

    headers = headers={"User-Agent": "Dalvik/2.1.0 (Linux; U; Android 5.1; PRO 5 Build/LMY47D)"}

    try:
        response = requests.get(static_url, headers=headers)
        response.raise_for_status()
        static_data = response.json() 

        response = requests.get(fixture_url, headers=headers)
        response.raise_for_status()
        fixture_data = response.json() 

        print(f"Data fetched succesfully")
        return [static_data, fixture_data]

    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch FPL data, error: {e}")
        return False

def run_fpl_pipeline():
    raw_data = fetch_fpl_data()
    if raw_data:
        bucket = get_fpl_bucket()
        for path in get_fpl_paths(["all"]):
            load_to_storage(bucket, path[0], dig(raw_data, path[1]))

if __name__ == "__main__":
    run_fpl_pipeline()

