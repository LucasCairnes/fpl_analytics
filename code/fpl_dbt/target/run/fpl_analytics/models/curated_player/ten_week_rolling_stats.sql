
  
    

    create or replace table `fpl-analytics-488811`.`curated_player_data`.`ten_week_rolling_stats`
      
    
    

    
    OPTIONS()
    as (
      

WITH player_data AS(
  SELECT * FROM `fpl-analytics-488811`.`stg_player_data`.`stg_player_stats`
),

rolling_stats AS (
  SELECT
    player_id,
    gameweek,
    

    
        SUM(minutes) OVER(
            PARTITION BY player_id
            ORDER BY gameweek 
            RANGE BETWEEN 9 PRECEDING AND CURRENT ROW
        )
    

 AS ten_gw_minutes,
    

    
        ROUND(AVG(total_points) OVER(
            PARTITION BY player_id
            ORDER BY gameweek 
            RANGE BETWEEN 9 PRECEDING AND CURRENT ROW
        ), 3)
    

 AS ten_gw_avg_pts
  FROM player_data
),

ten_gw_rolling_stats AS (
  SELECT
    r.*
  FROM rolling_stats r
  WHERE
    ten_gw_minutes > 540
)

SELECT * FROM ten_gw_rolling_stats
    );
  