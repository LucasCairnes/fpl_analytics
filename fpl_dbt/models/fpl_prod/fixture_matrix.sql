{{ config(materialized='table') }}

WITH fixture_data AS (
    SELECT * FROM {{ source('curated_fixture_data', 'cur_next_five_fixtures')}}
),

team_info AS (
    SELECT * FROM {{ ref('curated_team_data', 'cur_team_taxonomies') }} 
)

fixture_matrix AS (
    SELECT
        t.team_logo,
        t.team_name,
        t1.team_logo,
        t2.team_logo,
        t3.team_logo,
        t4.team_logo,
        t5.team_logo,
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