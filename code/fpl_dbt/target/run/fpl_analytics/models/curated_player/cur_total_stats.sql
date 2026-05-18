
  
    

    create or replace table `fpl-analytics-488811`.`curated_player`.`cur_total_stats`
      
    
    

    
    OPTIONS()
    as (
      

WITH player_data AS (
    SELECT * FROM `fpl-analytics-488811`.`stg_player`.`stg_player_data`
),

cur_total_stats AS (
    SELECT
        player_id,
        total_points,
        minutes,
        goals_scored,
        assists,
        clean_sheets,
        influence,
        creativity,
        threat,
        expected_goals,
        expected_assists,
        form,
        yellow_cards,
        red_cards,
        bps
    FROM player_data
)

SELECT * FROM cur_total_stats
    );
  