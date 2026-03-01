import os

def verify_gcs_credentials():
    cred_path = "/Users/sparelaptop4/Documents/fpl_analytics/extract/storage_admin_key.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path

def verify_vertex_ai_credentials():
    cred_path = "/Users/sparelaptop4/Documents/fpl_analytics/transform/vertex_api_key.json"
    os.environ["GEMINI_API_KEY"] = cred_path