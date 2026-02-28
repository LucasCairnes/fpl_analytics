import os
from google.cloud import storage
from datetime import date
from google.oauth2 import service_account
import pandas as pd
import pandas_gbq
import io

def get_credentials():
    return service_account.Credentials.from_service_account_file("/Users/sparelaptop4/Documents/fpl_analytics/extract/storage_admin_key.json")

def gcs_to_bq(bucket, gcs_path, table_id):
    chosen_blob = bucket.blob(gcs_path)
    chosen_json = chosen_blob.download_as_text()

    df = pd.read_json(io.StringIO(chosen_json))
    project_id = "fpl-analytics-488811"

    pandas_gbq.to_gbq(df, table_id, project_id=project_id, credentials=get_credentials(), if_exists="replace")