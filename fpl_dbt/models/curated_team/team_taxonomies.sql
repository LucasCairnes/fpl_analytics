{{ config(materialized='table') }}

WITH team_data AS(
  SELECT * FROM {{ source('raw_team_data', 'full_team_data')}}
),

team_taxonomies AS (
  SELECT 
    id AS team_id,
    name AS team_name,
    short_name
  FROM team_data
)

SELECT * FROM team_taxonomies