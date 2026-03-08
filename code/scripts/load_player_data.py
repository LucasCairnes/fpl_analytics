import aiohttp
import asyncio  
import pandas as pd
from google.cloud import bigquery

def get_player_ids():
    bq_client = bigquery.Client()
    query = 'SELECT player_id FROM `fpl-analytics-488811.curated_player_data.player_taxonomies`'
    query_job = bq_client.query(query)

    return [f"https://fantasy.premierleague.com/api/element-summary/{row['player_id']}/" for row in query_job]

async def fetch_player_data(session, url):
    print(f"Fetching: {url}")
    headers = {'User-Agent': 'Mozilla/5.0'}

    async with session.get(url, headers=headers) as response:
        await asyncio.sleep(1)
        data = await response.json()
        print(f"Completed task: {url}")
        if data:
            return pd.DataFrame(data['history'])

async def player_data_pipeline():
    urls = get_player_ids()
    
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_player_data(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
        return results

