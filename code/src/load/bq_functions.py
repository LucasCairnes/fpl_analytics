from google.cloud import storage
import pandas as pd
import pandas_gbq
import io
import os
from dotenv import load_dotenv
load_dotenv()

def gcs_to_bq(gcs_path, bucket, table_id):
    print(f"Uploading {gcs_path} to {table_id}...")
    chosen_blob = bucket.blob(gcs_path)
    chosen_json = chosen_blob.download_as_text()
    df = pd.read_json(io.StringIO(chosen_json))

    pandas_gbq.to_gbq(df, table_id, project_id=os.getenv("PROJECT_ID"), if_exists="replace", location='europe-west2')
    print("Upload successful.")