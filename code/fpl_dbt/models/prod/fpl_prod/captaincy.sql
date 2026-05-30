{{ config(materialized='view') }}

WITH player_data AS (
    SELECT * FROM {{ ref('obt_player_reporting') }}
),
fixture_data AS (
    SELECT * FROM {{ ref('cur_next_five_fixtures') }}
),
team_info AS (
    SELECT * FROM {{ ref('cur_team_taxonomies') }}
)

SELECT
    p.player,
    p.player_image,
    p.position,
    p.team_name,
    p.transfer_value,
    p.form,
    t_opp.team_logo AS next_opponent,
    ROUND(f.mean_difficulty_next_5, 2) AS mean_difficulty_next_5
FROM player_data p
LEFT JOIN fixture_data f ON p.team_id = f.team_id
LEFT JOIN team_info t_opp ON f.opp_1 = t_opp.team_id
ORDER BY p.form DESC