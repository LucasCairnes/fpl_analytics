{{ config(materialized='table') }}

WITH player_data AS(
  SELECT * FROM {{ source('curated_player_data', 'current_stats')}}
),

rolling_stats AS (
  SELECT
    player_id,
    gameweek,
    {{ rolling_value('goals_scored', 'SUM', 5) }} AS five_gw_goals,
    {{ rolling_value('assists', 'SUM', 5) }} AS five_gw_assists,
    {{ rolling_value('expected_goals', 'SUM', 5) }} AS five_gw_xg,
    {{ rolling_value('expected_assists', 'SUM', 5) }} AS five_gw_xa,
    {{ rolling_value('minutes', 'SUM', 5) }} AS five_gw_minutes
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