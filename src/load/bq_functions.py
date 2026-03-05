from google.cloud import storage
import pandas as pd
import pandas_gbq
import io

from src.load.gcs_functions import get_fpl_bucket

def gcs_to_bq(gcs_path, table_id):
    print(f"Uploading {gcs_path} to {table_id}...")
    bucket = get_fpl_bucket()
    chosen_blob = bucket.blob(gcs_path)
    chosen_json = chosen_blob.download_as_text()

    df = pd.read_json(io.StringIO(chosen_json))
    project_id = "fpl-analytics-488811"

    pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists="replace", location='europe-west2')
    print("Upload successful.")