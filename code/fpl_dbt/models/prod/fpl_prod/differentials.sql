{{ config(materialized='table') }}

WITH taxonomies AS (
    SELECT * FROM {{ ref('cur_player_taxonomies') }}
),

selection AS (
    SELECT * FROM {{ ref('cur_selection_info') }}
),

stats AS (
    SELECT * FROM {{ ref('cur_total_stats') }}
),

differentials AS (
    SELECT
        t.player_name AS player,
        t.position,
        t.player_image,
        s.form,
        sel.selected_by_percent
    FROM taxonomies t
    LEFT JOIN selection sel 
        ON t.player_id = sel.player_id
    LEFT JOIN stats s 
        ON t.player_id = s.player_id
    WHERE sel.selected_by_percent < 0.05
)

SELECT * FROM differentials