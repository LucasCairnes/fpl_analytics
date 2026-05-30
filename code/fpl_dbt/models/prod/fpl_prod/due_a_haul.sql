{{ config(materialized='view') }}

SELECT
    player,
    player_image,
    team_name AS team,
    team_logo,
    position,
    five_gw_goals AS goals,
    five_gw_assists AS assists,
    five_gw_xg AS xG,
    five_gw_xa AS xA,
    xga_performance
FROM {{ ref('obt_player_reporting') }}
WHERE five_gw_goals IS NOT NULL
ORDER BY xga_performance ASC