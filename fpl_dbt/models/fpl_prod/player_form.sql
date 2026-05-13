{{ config(materialized='table') }}

WITH player_data AS (
    SELECT * FROM {{ source('curated_player', 'cur_total_stats')}}
),

team_info AS (
    SELECT * FROM {{ ref('curated_team', 'cur_team_taxonomies') }} 
)

player_form AS (
    SELECT
        p.player_name AS player,
        p.player_image,
        p.form,
        t.logo AS team_logo
    FROM player_data p
    LEFT JOIN team_info t 
    ON p.team_name = t.team_name                   
)

SELECT * FROM player_form