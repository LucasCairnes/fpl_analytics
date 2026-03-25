{{ config(materialized='table') }}

WITH player_data AS (
    SELECT * FROM {{ source('raw_player_data', 'full_player_data')}}
),

fixture_data AS (
    SELECT * FROM {{ source('curated_fixture_data', 'upcoming_fixtures')}}
),

form_vs_fixtures AS (
    p.id AS player_id,
    p.form,
    p.status,
    f.mean_difficulty_next_5
    FROM player_data p
    LEFT JOIN fixture_data f
    
)