
from load.bq_functions import gcs_to_bq

gcs_path = f"raw-fpl-team/raw-teams-2026-02-28.json"
table_id = "fpl-analytics-488811.raw_team_data.full_team_data"

gcs_to_bq(gcs_path, table_id)

gcs_path = f"raw-fpl-player/raw-players-2026-02-28.json"
table_id = "fpl-analytics-488811.raw_player_data.full_player_data"

gcs_to_bq(gcs_path, table_id)

print("Upload successful!")