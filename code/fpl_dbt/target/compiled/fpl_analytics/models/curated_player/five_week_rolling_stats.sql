

WITH player_data AS(
  SELECT * FROM `fpl-analytics-488811`.`stg_player_data`.`stg_player_stats`
),

rolling_stats AS (
  SELECT
    player_id,
    gameweek,
    

    
        SUM(goals_scored) OVER(
            PARTITION BY player_id
            ORDER BY gameweek 
            RANGE BETWEEN 4 PRECEDING AND CURRENT ROW
        )
    

 AS five_gw_goals,
    

    
        SUM(assists) OVER(
            PARTITION BY player_id
            ORDER BY gameweek 
            RANGE BETWEEN 4 PRECEDING AND CURRENT ROW
        )
    

 AS five_gw_assists,
    

    
        SUM(expected_goals) OVER(
            PARTITION BY player_id
            ORDER BY gameweek 
            RANGE BETWEEN 4 PRECEDING AND CURRENT ROW
        )
    

 AS five_gw_xg,
    

    
        SUM(expected_assists) OVER(
            PARTITION BY player_id
            ORDER BY gameweek 
            RANGE BETWEEN 4 PRECEDING AND CURRENT ROW
        )
    

 AS five_gw_xa,
    

    
        SUM(minutes) OVER(
            PARTITION BY player_id
            ORDER BY gameweek 
            RANGE BETWEEN 4 PRECEDING AND CURRENT ROW
        )
    

 AS five_gw_minutes
  FROM player_data
),

five_gw_rolling_stats AS (
  SELECT
    r.*,
    r.five_gw_goals - five_gw_xg AS xg_performance,
    r.five_gw_assists - five_gw_xa AS xa_performance,
    r.five_gw_goals + r.five_gw_assists - r.five_gw_xg - r.five_gw_xa AS xga_performance
  FROM rolling_stats r
  WHERE
    five_gw_minutes > 270
)

SELECT * FROM five_gw_rolling_stats