
  
    

    create or replace table `fpl-analytics-488811`.`fpl_prod`.`individual_players`
      
    
    

    
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

transfer_stats AS (
    SELECT * FROM `fpl-analytics-488811`.`curated_player`.`cur_selection_info`
),

individual_players AS (
    SELECT
        pd.player_name AS player,
        pd.player_image,
        pd.position,
        ps.*,
        CONCAT(ps.goals_scored, ' (', ps.expected_goals, ')') AS goals_and_expected,
        CONCAT(ps.assists, ' (', ps.expected_assists, ')') AS assists_and_expected,
        t.*,
        ts.transfer_value,
        ts.selected_by_percent,
        ts.transfers_in,
        ts.transfers_out,
        ts.status
    FROM player_data pd
    LEFT JOIN team_info t 
        ON pd.team_name = t.team_name
    LEFT JOIN player_stats ps 
        ON pd.player_id = ps.player_id   
    LEFT JOIN transfer_stats ts 
        ON pd.player_id = ts.player_id                    
)

SELECT * FROM individual_players
    );
  