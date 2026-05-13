{{ config(materialized='table') }}

WITH fixture_data AS (
    SELECT * FROM {{ source('stg_fixture_data', 'stg_fixture_data') }}
),

stg_fixtures_by_team AS (
  SELECT 
    team_h AS team_id, 
    team_a AS opponent_id,
    team_a_difficulty AS match_difficulty,
    gameweek, 
    fixture_id
  FROM fixture_data
  WHERE finished = FALSE
  
  UNION ALL
  
  SELECT 
    team_a AS team_id, 
    team_h AS opponent_id, 
    team_h_difficulty AS match_difficulty,
    gameweek, 
    fixture_id
  FROM fixture_data
  WHERE finished = FALSE
)

SELECT * FROM fixtures_by_team