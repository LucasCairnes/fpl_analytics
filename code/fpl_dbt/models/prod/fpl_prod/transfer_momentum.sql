{{ config(materialized='view') }}

SELECT
    player,
    player_image,
    position,
    team_name,
    transfer_value,
    selected_by_percent,
    transfers_in,
    transfers_out,
    net_transfers,
    status
FROM {{ ref('obt_player_reporting') }}
WHERE (transfers_in > 0 OR transfers_out > 0)