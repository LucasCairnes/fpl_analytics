{{ config(materialized='table') }}

WITH player_data AS (
    SELECT * FROM {{ ref('cur_player_taxonomies') }}
),

player_stats AS (
    SELECT * FROM {{ ref('cur_total_stats') }}
),

selection_info AS (
    SELECT * FROM {{ ref('cur_selection_info') }}
),

fixture_data AS (
    SELECT * FROM {{ ref('cur_next_five_fixtures') }}
),

team_info AS (
    SELECT * FROM {{ ref('cur_team_taxonomies') }}
),

captaincy_matrix AS (
    SELECT
        p.player_name AS player,
        p.player_image,
        p.position,
        p.team_name,
        s.transfer_value,
        CAST(ps.form AS FLOAT64) AS form,
        t_opp.team_logo AS next_opponent,
        ROUND(f.mean_difficulty_next_5, 2) AS mean_difficulty_next_5
    FROM player_data p
    INNER JOIN player_stats ps 
        ON p.player_id = ps.player_id
    INNER JOIN selection_info s 
        ON p.player_id = s.player_id
    LEFT JOIN fixture_data f 
        ON p.team_id = f.team_id
    LEFT JOIN team_info t_opp 
        ON f.opp_1 = t_opp.team_id
)

SELECT * FROM captaincy_matrix
ORDER BY form DESC