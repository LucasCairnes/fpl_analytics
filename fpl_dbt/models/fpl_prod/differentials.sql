{{ config(materialized='table') }}

WITH ownership_data AS (
    SELECT * FROM {{ source('curated_player','current_stats') }}
),

player_data AS (
    SELECT * FROM {{ source('stg_player', 'stg_player_data')}}
)

differentials AS (
    SELECT
        p.player_name AS player,
        p.position,
        p.player_image,
        p.form,
        o.selected_by_percent
    FROM player_data p
    LEFT JOIN ownership_data o
    ON p.player_id = o.player_id
    WHERE selected_by_percent < 0.05
)

SELECT * FROM differentials