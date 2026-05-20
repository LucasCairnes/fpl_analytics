import pandas_gbq, pandas as pd
import io, os
from tenacity import retry, wait_exponential, stop_after_attempt
from dotenv import load_dotenv
load_dotenv()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10), reraise=True)
def gcs_to_bq(gcs_path, bucket, table_id):
    chosen_blob = bucket.blob(gcs_path)
    chosen_json = chosen_blob.download_as_text()
    df = pd.read_json(io.StringIO(chosen_json))

    pandas_gbq.to_gbq(df, table_id, project_id=os.getenv("PROJECT_ID"), if_exists="replace", location='europe-west2')    