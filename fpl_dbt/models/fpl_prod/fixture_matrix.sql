{{ config(materialized='table') }}

WITH fixture_data AS (
    SELECT * FROM {{ source('curated_fixture', 'cur_next_five_fixtures')}}
),

team_info AS (
    SELECT * FROM {{ source('curated_team', 'cur_team_taxonomies') }} 
),

fixture_matrix AS (
    SELECT
        t.team_logo,
        t.team_name,
        t1.team_logo AS opp1,
        t2.team_logo AS opp2,
        t3.team_logo AS opp3,
        t4.team_logo AS opp4,
        t5.team_logo AS opp5,
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