{{ config(materialized='table') }}

WITH player_data AS (
    SELECT * FROM {{ ref('cur_player_taxonomies') }}
),

player_stats AS (
    SELECT * FROM {{ ref('cur_total_stats') }}
),

selection_info AS (
    SELECT * FROM {{ ref('cur_selection_info') }}
),

fixtures AS (
    SELECT * FROM {{ ref('fixture_matrix') }}
),

cheap_form_picks AS (
    SELECT
        p.player_name AS player,
        p.player_image,
        p.position,
        p.team_name,
        s.transfer_value,
        CAST(ps.form AS FLOAT64) AS form,
    FROM player_data p
    INNER JOIN player_stats ps 
        ON p.player_id = ps.player_id
    INNER JOIN selection_info s
        ON p.player_id = s.player_id
    LEFT JOIN fixtures f
        ON p.team_name = f.team_name
    WHERE 
        s.transfer_value <= 4.5
)

SELECT * FROM cheap_form_picks
