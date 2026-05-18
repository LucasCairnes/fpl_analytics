
  
    

    create or replace table `fpl-analytics-488811`.`stg_player`.`stg_player_stats`
      
    
    

    
    OPTIONS()
    as (
      

WITH player_stats AS (
    SELECT * FROM `fpl-analytics-488811`.`raw_player`.`raw_player_stats`
),

stg_player_stats AS (
  SELECT
    element AS player_id,
    round AS gameweek,
    COUNT(fixture) AS fixtures_played, 
    SUM(total_points) AS total_points,
    SUM(minutes) AS minutes,
    SUM(goals_scored) AS goals_scored,
    SUM(assists) AS assists,
    SUM(clean_sheets) AS clean_sheets,
    SUM(goals_conceded) AS goals_conceded,
    SUM(own_goals) AS own_goals,
    SUM(penalties_missed) AS penalties_missed,
    SUM(penalties_saved) AS penalties_saved,
    SUM(yellow_cards) AS yellow_cards,
    SUM(red_cards) AS red_cards,
    SUM(saves) AS saves,
    SUM(bonus) AS bonus,
    SUM(bps) AS bps,
    SUM(influence) AS influence,
    SUM(creativity) AS creativity,
    SUM(threat) AS threat,
    SUM(ict_index) AS ict_index,
    SUM(expected_goals) AS expected_goals,
    SUM(expected_assists) AS expected_assists,
    SUM(expected_goals_conceded) AS expected_goals_conceded,
    SUM(expected_goal_involvements) AS expected_goal_involvements

  FROM player_stats
  GROUP BY
    player_id,
    gameweek
)

SELECT * FROM stg_player_stats
    );
  