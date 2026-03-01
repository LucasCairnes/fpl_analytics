from google.cloud import bigquery
from google import genai
from google.genai import types
from pydantic import BaseModel, Field
import json

from transform_functions import verify_gcs_credentials, verify_vertex_ai_credentials

verify_gcs_credentials()
bq_client = bigquery.Client()

query = """
SELECT 
p.player_id,
p.player_name,
t.team_name,
p.position
FROM `fpl-analytics-488811.curated_player_data.player_taxonomies` p
LEFT JOIN `fpl-analytics-488811.curated_team_data.team_taxonomies` t
ON p.team_id = t.team_id
"""

rows = bq_client.query_and_wait(query)
json_output = json.dumps([dict(row) for row in rows])

class PlayerNames(BaseModel):
    player_id: str
    player_name: str

client = genai.Client()

prompt = f"For the following PL players please output the inputted player_id along with their most commonly used full name, e.g. for input: David Raya Martin, output: David Raya"

response = client.models.generate_content(
    model='gemini-3-flash-preview',
    contents=prompt,
    config=types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=PlayerNames, 
    )
)
