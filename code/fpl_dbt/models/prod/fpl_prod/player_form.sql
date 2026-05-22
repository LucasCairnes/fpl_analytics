{{ config(materialized='table') }}

WITH player_data AS (
    SELECT * FROM {{ ref('cur_player_taxonomies') }}
),

player_stats AS (
    SELECT * FROM {{ ref('cur_total_stats') }}
),

team_info AS (
    SELECT * FROM {{ ref('cur_team_taxonomies') }}
),

selection_info AS (
    SELECT * FROM {{ ref('cur_selection_info') }}
),

player_form AS (
    SELECT
        pd.player_name AS player,
        pd.player_image,
        pd.position,
        ps.form,
        si.transfer_value,
        t.team_logo
    FROM player_data pd
    LEFT JOIN team_info t 
        ON pd.team_name = t.team_name
    LEFT JOIN player_stats ps 
        ON pd.player_id = ps.player_id
    LEFT JOIN selection_info si 
        ON pd.player_id = si.player_id
)

SELECT * FROM player_form