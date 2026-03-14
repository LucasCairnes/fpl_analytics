{{ config(materialized='table') }}

WITH player_data AS(
  SELECT * FROM {{ source('stg_player_data', 'stg_player_stats')}}
),

five_gw_rolling_stats AS (
  SELECT
    player_id,
    gameweek,
    {{ rolling_value('goals_scored', 'SUM', 5) }} AS five_gw_goals,
    {{ rolling_value('assists', 'SUM', 5) }} AS five_gw_assists,
    {{ rolling_value('expected_goals', 'AVG', 5) }} AS five_gw_xg_avg,
    {{ rolling_value('expected_assists', 'AVG', 5) }} AS five_gw_xa_avg
  FROM player_data
  WHERE
    minutes > 0
)

SELECT * FROM five_gw_rolling_stats