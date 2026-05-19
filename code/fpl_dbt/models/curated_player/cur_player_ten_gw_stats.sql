{{ config(materialized='table') }}

WITH player_data AS(
  SELECT * FROM {{ source('stg_player', 'stg_player_stats')}}
),

rolling_stats AS (
  SELECT
    player_id,
    gameweek,
    {{ rolling_value('minutes', 'SUM', 10) }} AS ten_gw_minutes,
    {{ rolling_value('total_points', 'AVG', 10) }} AS ten_gw_avg_pts
  FROM player_data
),

cur_player_ten_gw_stats AS (
  SELECT
    r.*
  FROM rolling_stats r
  WHERE
    ten_gw_minutes > 540
)

SELECT * FROM cur_player_ten_gw_stats