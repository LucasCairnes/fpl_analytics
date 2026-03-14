{{ config(materialized='table') }}

WITH player_stats AS (
    SELECT * FROM {{ source('raw_player_data', 'full_player_stats') }}
),

stg_player_stats AS (
  SELECT
    element AS player_id,
    fixture AS fixture_id,
    round AS gameweek,
    total_points,
    minutes,
    goals_scored,
    assists,
    clean_sheets,
    goals_conceded,
    own_goals,
    penalties_missed,
    penalties_saved,
    yellow_cards,
    red_cards,
    saves,
    bonus,
    bps,
    influence,
    creativity,
    threat,
    ict_index,
    expected_goals,
    expected_assists,
    expected_goals_conceded,
    expected_goal_involvements,

  FROM player_stats
)

SELECT * FROM stg_player_stats