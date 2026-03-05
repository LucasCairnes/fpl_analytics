from src.extract.fpl_api import run_fpl_pipeline
from src.load.bq_functions import gcs_to_bq
from dotenv import load_dotenv

try:
    load_dotenv()

    run_fpl_pipeline()

    gcs_path = f"raw-fpl-team/raw-teams-2026-02-28.json"
    table_id = "fpl-analytics-488811.raw_team_data.full_team_data"
    gcs_to_bq(gcs_path, table_id)

    gcs_path = f"raw-fpl-player/raw-players-2026-02-28.json"
    table_id = "fpl-analytics-488811.raw_player_data.full_player_data"
    gcs_to_bq(gcs_path, table_id)
    
    print("Pipeline executed successfully!")

except Exception as e:
    print(f"Load FPL data API encountered unexpected error: {e}")

print("testing autobuild")