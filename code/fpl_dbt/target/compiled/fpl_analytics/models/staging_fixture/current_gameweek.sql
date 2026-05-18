

WITH fixture_data AS (
    SELECT * FROM `fpl-analytics-488811`.`stg_fixture_data`.`stg_fixtures`
),

current_gameweek AS (
    SELECT
        gameweek
    FROM fixture_data
    WHERE finished = false AND gameweek IS NOT NULL
    ORDER BY gameweek ASC
    LIMIT 1
)

SELECT * FROM current_gameweek