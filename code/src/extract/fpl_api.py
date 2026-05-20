import requests, asyncio, os
from google.api_core import exceptions
import logging, aiohttp
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from dotenv import load_dotenv
load_dotenv()

def fetch_fpl_data(url):
    try:
        print(f"Fetching: {url}...")
        API_KEY = os.getenv("SCRAPER_API_KEY")
        scraperapi_url = f"http://api.scraperapi.com?api_key={API_KEY}&url={url}"
        response = requests.get(scraperapi_url)
        response.raise_for_status()
        static_data = response.json() 

        print(f"Fetched successfully.")
        return static_data
    
    except requests.exceptions.RequestException as e:
        print(f"Unexpected error occured: {e}")
        return False

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10), retry=retry_if_exception_type(aiohttp.ClientError), reraise=True)
async def fetch_player_data(session, url, semaphore):
    async with semaphore:
        API_KEY = os.getenv("SCRAPER_API_KEY")
        scraperapi_url = f"http://api.scraperapi.com?api_key={API_KEY}&url={url}"
        headers = {'User-Agent': 'Mozilla/5.0'}

        async with session.get(scraperapi_url, headers=headers) as response:
            data = await response.json()

            return (url, data["history"], None)

async def safe_fetch(session, url, semaphore, logging_context):
    try:
        return await fetch_player_data(session, url, semaphore)
    
    except Exception as e:
        logging.warning(
            f"Failed to extract url: {url}. Continuing...",
            exc_info=True,
            extra={"json_fields":logging_context}
        )
        return (url, None, str(e))
        
async def fetch_player_histories(urls, logging_context):
    semaphore = asyncio.Semaphore(5)
    connector = aiohttp.TCPConnector(limit=5)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [safe_fetch(session, url, semaphore, logging_context) for url in urls]
        results = await asyncio.gather(*tasks)

        gathered_data = []
        failures = []

        for result in results:
            if result[1]:
                gathered_data.extend(result[1])
            else:
                failures.append((result[0], result[2]))

        return [gathered_data, failures]
