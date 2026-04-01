{{ config(materialized='table') }}

WITH player_data AS (
    SELECT * FROM {{ source('curated_player_data', 'player_taxonomies')}}
),

rolling_stats AS (
    SELECT * FROM {{ source('curated_player_data', 'ten_week_rolling_stats')}}
),

transfer_stats AS (
    SELECT * FROM {{ source('curated_player_data', 'transfer_stats')}}
),

team_info AS (
    SELECT * FROM {{ ref('curated_team_data', 'team_taxonomies') }} 
)

best_value AS (
    SELECT
        p.player_name AS player,
        p.player_image,
        p.team_name AS team,
        ti.logo AS team_logo,
        p.position,
        t.transfer_value,
        r.ten_gw_avg_pts,
        ROUND(r.ten_gw_avg_pts / t.transfer_value, 3) AS points_per_value
    FROM rolling_stats r
    LEFT JOIN player_data p
        ON r.player_id = p.player_id
    LEFT JOIN transfer_stats t
        ON r.player_id = t.player_id
    LEFT JOIN team_info ti 
        ON p.team_name = ti.team_name 
    WHERE r.gameweek = (SELECT MAX(gameweek) FROM rolling_stats)
    ORDER BY ten_gw_avg_pts ASC
)

SELECT * FROM best_value