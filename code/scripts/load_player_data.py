import os
from datetime import date
from google.cloud import storage
from google.cloud import bigquery
import asyncio
from dotenv import load_dotenv
load_dotenv()

from src.extract.fpl_api import fetch_player_histories
from src.load.gcs_functions import load_to_storage
from src.load.bq_functions import gcs_to_bq

client = bigquery.Client()
query ="""
    WITH todays_teams AS (
        SELECT
        team_h AS team_id
        FROM `fpl-analytics-488811.stg_fixture.stg_fixture_data`
        WHERE date = DATE_SUB(CURRENT_DATE('Europe/London'), INTERVAL 1 DAY )

        UNION ALL

        SELECT
        team_a AS team_id
        FROM `fpl-analytics-488811.stg_fixture.stg_fixture_data`
        WHERE date = DATE_SUB(CURRENT_DATE('Europe/London'), INTERVAL 1 DAY )
    )

    SELECT
    DISTINCT(player_id)
    FROM `fpl-analytics-488811.curated_player.cur_player_taxonomies`
    WHERE team_id IN (SELECT team_id FROM todays_teams)
    """ 

query_job = client.query(query)
player_urls = [f"https://fantasy.premierleague.com/api/element-summary/{row['player_id']}/" for row in query_job]

if player_urls:
    # player_histories = asyncio.run(fetch_player_histories(player_urls))

    current_date = date.today().isoformat()
    gcs_path = f"raw-fpl-player-stats/raw-stats-{current_date}.json"

    client = storage.Client()
    bucket = client.bucket(os.getenv("GCS_BUCKET_NAME"))

    # load_to_storage(bucket, gcs_path, player_histories)

    table_id = "fpl-analytics-488811.raw_player.raw_player_stats"

    gcs_to_bq(gcs_path, bucket, table_id, method="merge")
