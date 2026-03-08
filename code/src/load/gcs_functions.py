from google.cloud import storage
import json

def load_to_storage(bucket, destination_path, raw_data):
    try:
        print(f"Uploading to {destination_path}...")
        blob = bucket.blob(destination_path)
        blob.upload_from_string(json.dumps(raw_data), content_type="application/json")
        print(f"Upload successful.")

    except Exception as e:
        print(f"An error occured during upload: {e}, continuing...")