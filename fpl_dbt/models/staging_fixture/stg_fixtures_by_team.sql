{{ config(materialized='table') }}

WITH fixture_data AS (
    SELECT * FROM {{ source('raw_fixture', 'raw_fixture_data') }}
),

stg_fixtures_by_team AS (
  SELECT 
    team_h AS team_id, 
    team_a AS opponent_id,
    team_h_difficulty AS match_difficulty,
    CAST(event AS int) AS gameweek, 
    id AS fixture_id,
    'H' AS venue
  FROM fixture_data
  WHERE finished = FALSE
  
  UNION ALL
  
  SELECT 
    team_a AS team_id, 
    team_h AS opponent_id, 
    team_a_difficulty AS match_difficulty,
    CAST(event AS int) AS gameweek, 
    id AS fixture_id,
    'A' AS venue
  FROM fixture_data
  WHERE finished = FALSE
)

SELECT * FROM stg_fixtures_by_team