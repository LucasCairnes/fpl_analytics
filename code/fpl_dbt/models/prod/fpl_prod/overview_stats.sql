{{ config(materialized='view') }}

SELECT
    player,
    player_image,
    position,
    goals_scored,
    assists,
    clean_sheets,
    team_logo,
    transfer_value,
    net_transfers,
    total_points
FROM {{ ref('obt_player_reporting') }}