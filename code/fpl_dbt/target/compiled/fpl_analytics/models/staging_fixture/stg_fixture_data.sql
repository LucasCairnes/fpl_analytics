

WITH fixture_data AS (
    SELECT * FROM `fpl-analytics-488811`.`raw_fixture`.`raw_fixture_data`
),

stg_fixture_data AS (
    SELECT
        id AS fixture_id,
        CAST(event AS int) AS gameweek,
        CAST(kickoff_time AS date) AS date,
        team_h,
        team_a,
        CAST(team_h_score AS int) AS team_h_score,
        CAST(team_a_score AS int) AS team_a_score,
        team_h_difficulty,
        team_a_difficulty,
        started,
        finished
    FROM fixture_data
)

SELECT * FROM stg_fixture_data