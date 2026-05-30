{{ config(materialized='view') }}

SELECT
    player,
    player_image,
    position,
    team_name,
    transfer_value,
    form
FROM {{ ref('obt_player_reporting') }}
WHERE transfer_value <= 4.5