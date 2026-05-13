{{ config(materialized='table') }}

WITH fixture_data AS (
    SELECT * FROM {{ source('raw_fixture', 'raw_fixture_data') }}
),

stg_fixture_data AS (
    SELECT
        id AS fixture_id,
        CAST(event AS int) AS gameweek,
        team_h,
        team_a,
        CAST(team_h_score AS int) AS team_h_score,
        CAST(team_a_score AS int) AS team_a_score,
        team_h_difficulty,
        team_a_difficulty,
        CASE
            WHEN started = 1.0 THEN true
            WHEN started = 0.0 THEN false
            ELSE null
        END AS started,
        finished
    FROM fixture_data
)

SELECT * FROM stg_fixtures