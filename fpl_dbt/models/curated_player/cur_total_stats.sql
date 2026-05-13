{{ config(materialized='table') }}

WITH player_data AS (
    SELECT * FROM {{ source('stg_player', 'stg_player_data') }}
),

cur_total_stats AS (
    SELECT
        player_id,
        total_points,
        minutes,
        goals_scored,
        assists,
        influence,
        creativity,
        threat,
        expected_goals,
        expected_assists,
        form
    FROM player_data
)

SELECT * FROM cur_total_stats

