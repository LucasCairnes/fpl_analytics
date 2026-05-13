{{ config(materialized='table') }}

WITH fixture_data AS (
    SELECT * FROM {{ source('stg_fixture', 'stg_fixtures') }}
),

cur_current_gameweek AS (
    SELECT
        gameweek
    FROM fixture_data
    WHERE finished = false AND gameweek IS NOT NULL
    ORDER BY gameweek ASC
    LIMIT 1
)

SELECT * FROM current_gameweek