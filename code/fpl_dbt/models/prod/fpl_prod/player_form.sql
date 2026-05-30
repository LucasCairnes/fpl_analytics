{{ config(materialized='view') }}

SELECT
    player,
    player_image,
    position,
    form,
    transfer_value,
    team_logo
FROM {{ ref('obt_player_reporting') }}