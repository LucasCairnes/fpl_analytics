

WITH fixture_data AS (
    SELECT * FROM `fpl-analytics-488811`.`stg_fixture_data`.`stg_fixture_data`
),

all_team_fixtures AS (
  SELECT 
    team_h AS team_id, 
    team_a AS opponent_id, 
    gameweek, 
    fixture_id
  FROM fixture_data
  WHERE finished = FALSE
  
  UNION ALL
  
  SELECT 
    team_a AS team_id, 
    team_h AS opponent_id, 
    gameweek, 
    fixture_id
  FROM fixture_data
  WHERE finished = FALSE
),

SELECT * FROM all_team_fixtures