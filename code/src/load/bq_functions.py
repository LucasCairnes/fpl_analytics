from google.cloud import bigquery
import os
from tenacity import retry, wait_exponential, stop_after_attempt
from dotenv import load_dotenv
load_dotenv()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10), reraise=True)
def gcs_to_bq(gcs_path, bucket_name, table_id, write_disposition="WRITE_TRUNCATE"):
    client = bigquery.Client(project=os.getenv("PROJECT_ID"))
    gcs_uri = f"gs://{bucket_name}/{gcs_path}"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=getattr(bigquery.WriteDisposition, write_disposition) 
    )
    
    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()