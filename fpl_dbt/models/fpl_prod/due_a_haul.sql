{{ config(materialized='table') }}

WITH player_data AS (
    SELECT * FROM {{ source('curated_player_data', 'player_taxonomies')}}
),

rolling_stats AS (
    SELECT * FROM {{ source('curated_player_data', 'five_week_rolling_stats')}}
),

team_info AS (
    SELECT * FROM {{ ref('curated_player_data', 'team_taxonomies') }} 
),

due_a_haul AS (
    SELECT
        p.player_name AS player,
        p.player_image,
        p.team_name AS team,
        ti.logo AS team_logo,
        p.position,
        r.five_gw_goals AS goals,
        r.five_gw_assists AS assists,
        ROUND(r.five_gw_xg, 3) AS xG,
        ROUND(r.five_gw_xa, 3) AS xA,
        ROUND(r.xga_performance, 3) AS xGA_performance
    FROM rolling_stats r
    LEFT JOIN player_data p
        ON r.player_id = p.player_id
    LEFT JOIN team_info ti 
        ON p.team_name = ti.team_name 
    WHERE r.gameweek = (SELECT MAX(gameweek) FROM rolling_stats)
    ORDER BY xga_performance ASC
)

SELECT * FROM due_a_haul