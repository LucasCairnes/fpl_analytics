{{ config(materialized='table') }}

WITH player_data AS (
    SELECT * FROM {{ source('curated_player_data', 'player_taxonomies')}}
),

rolling_stats AS (
    SELECT * FROM {{ source('curated_player_data', 'five_week_rolling_stats')}}
),

due_a_haul AS (
    SELECT
        p.player_name AS player,
        p.team_name AS team,
        p.position,
        r.five_gw_goals AS goals,
        r.five_gw_assists AS assists,
        ROUND(r.xg_performance, 3) AS xG
        ROUND(r.xa_performance, 3) AS xA,
        ROUND(r.xga_performance, 3) AS xG + xA
    FROM rolling_stats r
    LEFT JOIN player_data p
    ON r.player_id = p.player_id
    WHERE r.gameweek = (SELECT MAX(gameweek) FROM rolling_stats)
    ORDER BY xga_performance ASC
)

SELECT * FROM due_a_haul

