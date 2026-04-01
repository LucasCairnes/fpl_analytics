{{ config(materialized='table') }}

WITH fixture_data AS (
    SELECT * FROM {{ source('curated_fixture_data', 'upcoming_fixtures')}}
),

team_info AS (
    SELECT * FROM {{ ref('curated_team_data', 'team_taxonomies') }} 
)

fixture_matrix AS (
    SELECT
        t.logo,
        t.team_name,
        t1.logo,
        t2.logo,
        t3.logo,
        t4.logo,
        t5.logo,
        f.mean_difficulty_next_5
    FROM fixture_data f
    LEFT JOIN team_info t
    ON f.team_id = t.team_id
    LEFT JOIN team_info t1
    ON f.opp_1 = t.team_id
    LEFT JOIN team_info t2
    ON f.opp_2 = t.team_id
    LEFT JOIN team_info t3
    ON f.opp_3 = t.team_id
    LEFT JOIN team_info t4
    ON f.opp_4 = t.team_id
    LEFT JOIN team_info t5
    ON f.opp_5 = t.team_id
)

SELECT * FROM fixture_matrix