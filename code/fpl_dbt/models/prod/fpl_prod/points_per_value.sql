{{ config(materialized='view') }}

SELECT
    player,
    player_image,
    team_name,
    team_logo,
    position,
    transfer_value,
    ten_gw_avg_pts,
    selected_by_percent,
    ROUND(ten_gw_avg_pts / transfer_value, 3) AS points_per_value
FROM {{ ref('obt_player_reporting') }}
WHERE ten_gw_avg_pts IS NOT NULL
ORDER BY ten_gw_avg_pts ASC