import pandas as pd
import io
from google.cloud import storage
from datetime import date
from google.api_core import exceptions
import pandas_gbq

from gcp_auth import get_fpl_api_paths, get_credentials

def gcs_to_bq(bucket, gcs_path, table_id):
    chosen_blob = bucket.blob(gcs_path)
    chosen_json = chosen_blob.download_as_text()

    df = pd.read_json(io.StringIO(chosen_json))
    project_id = "fpl-analytics-488811"

    pandas_gbq.to_gbq(df, table_id, project_id=project_id, credentials=get_credentials(), if_exists="replace")
