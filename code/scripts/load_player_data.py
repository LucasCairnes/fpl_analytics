import aiohttp
import asyncio  
import pandas as pd
from google.cloud import bigquery
from src.extract.fpl_api import fetch_fpl_data

def get_player_ids():
    bq_client = bigquery.Client()
    QUERY="""
    SELECT player_id
    FROM `fpl-analytics-488811.curated_player_data.player_taxonomies` 
    """
    query_job = bq_client.query(QUERY)
    return [f"https://fantasy.premierleague.com/api/element-summary/{row.player_id}/" for row in query_job]

async def fetch_player_data(session, url):
    print(f"Fetching: {url}")
    async with session.get(url) as response:
        await asyncio.sleep(1)
        data = await response.json()
        print(f"Completed task: {url}")
        return data

async def player_data_pipeline():
    urls = get_player_ids()
    
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_player_data(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
        
    
if __name__ == '__main__':
    asyncio.run(player_data_pipeline)
