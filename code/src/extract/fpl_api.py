import requests
import aiohttp
import asyncio  
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from google.api_core import exceptions
from dotenv import load_dotenv
load_dotenv()

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

def get_player_urls():
    bq_client = bigquery.Client()
    query = 'SELECT player_id FROM `fpl-analytics-488811.curated_player_data.player_taxonomies`'
    query_job = bq_client.query(query)

    return [f"https://fantasy.premierleague.com/api/element-summary/{row['player_id']}/" for row in query_job]

async def fetch_player_data(session, url, semaphore):
    async with semaphore: 
        print(f"Fetching: {url}")
        headers = {'User-Agent': 'Mozilla/5.0'}

        async with session.get(url, headers=headers) as response:
            await asyncio.sleep(5)
            data = await response.json()
            print(f"Completed task: {url}")
            return data["history"]

async def fetch_player_histories():
    urls = get_player_urls()

    semaphore = asyncio.Semaphore(10)
    connector = aiohttp.TCPConnector(limit=10)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_player_data(session, url, semaphore) for url in urls]
        results = await asyncio.gather(*tasks)

        flat_history = [item for sublist in results if sublist for item in sublist]
        return flat_history



