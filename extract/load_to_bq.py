import pandas as pd
import io
from google.cloud import storage
from datetime import date
from google.api_core import exceptions
import pandas_gbq

from gcp_auth import get_fpl_bucket, get_fpl_api_paths, get_credentials

player_path = get_fpl_api_paths(["player"])[0]
bucket = get_fpl_bucket()

player_blob = bucket.blob(player_path)
player_json = player_blob.download_as_text()

player_df = pd.read_json(io.StringIO(player_json))
project_id = "fpl-analytics-488811"
table_id = f"raw_player_data.players_{date.today().strftime("%Y_%d_%d")}"

pandas_gbq.to_gbq(player_df, table_id, project_id=project_id, credentials=get_credentials(), if_exists="replace")