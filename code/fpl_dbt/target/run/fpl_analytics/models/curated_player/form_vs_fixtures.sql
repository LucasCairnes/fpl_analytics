
  
    

    create or replace table `fpl-analytics-488811`.`curated_player_data`.`form_vs_fixtures`
      
    
    

    
    OPTIONS()
    as (
      

WITH player_data AS (
    SELECT * FROM `fpl-analytics-488811`.`stg_player_data`.`stg_player_data`
),

fixture_data AS (
    SELECT * FROM `fpl-analytics-488811`.`curated_fixture_data`.`upcoming_fixtures`
),

form_vs_fixtures AS (
    SELECT
        p.player_id,
        p.form,
        p.status,
        f.mean_difficulty_next_5
    FROM player_data p
    LEFT JOIN fixture_data f
    ON p.team_id = f.team_id
)

SELECT * FROM form_vs_fixtures
    );
  