{{ config(materialized='view') }}

SELECT
    player,
    position,
    player_image,
    xgi,
    selected_by_percent
FROM {{ ref('obt_player_reporting') }}
WHERE selected_by_percent < 5