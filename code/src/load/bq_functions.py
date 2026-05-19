from google.cloud import storage
import pandas as pd
import pandas_gbq
import io
import os
import json
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dotenv import load_dotenv
load_dotenv()

def gcs_to_bq(gcs_path, bucket, table_id, method="replace"):
    print(f"Uploading {gcs_path} to {table_id}...")
    chosen_blob = bucket.blob(gcs_path)
    chosen_json = chosen_blob.download_as_text()
    df = pd.read_json(io.StringIO(chosen_json))

    if method == "replace":
        pandas_gbq.to_gbq(df, table_id, project_id=os.getenv("PROJECT_ID"), if_exists="replace", location='europe-west2')
        print("Upload successful.")
    
    elif method == "merge":
        table_name = table_id.split(".")[-1]
        temp_table = f"fpl-analytics-488811.temporary.temp_{table_name}"
        pandas_gbq.to_gbq(df, temp_table, project_id=os.getenv("PROJECT_ID"), if_exists="replace", location='europe-west2')
        
        os.chdir("fpl_dbt")

        dbt_vars = {
            "input_table" : temp_table,
            "output_table" : table_name
        }

        dbt = dbtRunner()
        dbt_args = ["run", "--select", "raw_player_merge", "--vars", json.dumps(dbt_vars)]
        
        res: dbtRunnerResult = dbt.invoke(dbt_args)