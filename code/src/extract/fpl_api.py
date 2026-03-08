import requests
from google.cloud import storage
from google.api_core import exceptions

from src.load.gcs_functions import get_fpl_bucket, get_fpl_paths, dig, load_to_storage

def fetch_fpl_data(url):
    print(f"Fetching data from {url}...")
    try:
        response = requests.get(url)
        response.raise_for_status()
        static_data = response.json() 

        print(f"Fetched succesfully.")
        return static_data

    except requests.exceptions.RequestException as e:
        print(f"Unexpected error occured: {e}")
        return False

def run_fpl_pipeline():
    urls = ["https://fantasy.premierleague.com/api/bootstrap-static/",
            "https://fantasy.premierleague.com/api/fixtures/"]
    raw_data = [fetch_fpl_data(url) for url in urls if fetch_fpl_data(url)]

    if raw_data:
        bucket = get_fpl_bucket()
        for path in get_fpl_paths(["all"]):
            load_to_storage(bucket, path[0], dig(raw_data, path[1]))

