
  
    

    create or replace table `fpl-analytics-488811`.`fpl_prod`.`player_form`
      
    
    

    
    OPTIONS()
    as (
      

WITH player_data AS (
    SELECT * FROM `fpl-analytics-488811`.`curated_player`.`cur_player_taxonomies`
),

player_stats AS (
    SELECT * FROM `fpl-analytics-488811`.`curated_player`.`cur_total_stats`
),

team_info AS (
    SELECT * FROM `fpl-analytics-488811`.`curated_team`.`cur_team_taxonomies` 
),

player_form AS (
    SELECT
        pd.player_name AS player,
        pd.player_image,
        ps.form,
        t.team_logo
    FROM player_data pd
    LEFT JOIN team_info t 
        ON pd.team_name = t.team_name
    LEFT JOIN player_stats ps 
        ON pd.player_id = ps.player_id                    
)

SELECT * FROM player_form
    );
  