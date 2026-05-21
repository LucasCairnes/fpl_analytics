import json
from tenacity import retry, wait_exponential, stop_after_attempt

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10), reraise=True)
def load_to_storage(bucket, destination_path, raw_data):
    blob = bucket.blob(destination_path)
    ndjson_data = "\n".join([json.dumps(record) for record in raw_data])
    blob.upload_from_string(ndjson_data, content_type="application/x-ndjson")
