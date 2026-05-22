{{ config(materialized='table') }}

WITH raw_team AS (
    SELECT * FROM {{ source('raw_team', 'raw_team_data') }}
),

player_data AS (
    SELECT * FROM {{ ref('stg_player_data') }}
),

team_taxonomies AS (
    SELECT * FROM {{ ref('cur_team_taxonomies') }}
),

team_aggregates AS (
    SELECT
        team_id,
        ROUND(SUM(CAST(expected_goals AS FLOAT64)), 2) AS total_xg,
        ROUND(SUM(CAST(expected_goals_conceded AS FLOAT64)), 2) AS total_xgc,
        SUM(CAST(goals_scored AS INT64)) AS total_goals_scored,
        SUM(CAST(goals_conceded AS INT64)) AS total_goals_conceded,
        ROUND(AVG(CASE WHEN CAST(minutes AS INT64) > 0 THEN CAST(expected_goals_per_90 AS FLOAT64) END), 3) AS avg_xg_per_90,
        ROUND(AVG(CASE WHEN CAST(minutes AS INT64) > 0 THEN CAST(expected_goals_conceded_per_90 AS FLOAT64) END), 3) AS avg_xgc_per_90
    FROM player_data
    GROUP BY team_id
)

SELECT
    t.team_id,
    t.team_name,
    t.short_name,
    
    CAST(r.strength_attack_home AS INT64) AS strength_attack_home,
    CAST(r.strength_attack_away AS INT64) AS strength_attack_away,
    CAST(r.strength_defence_home AS INT64) AS strength_defence_home,
    CAST(r.strength_defence_away AS INT64) AS strength_defence_away,
    CAST(r.strength_overall_home AS INT64) AS strength_overall_home,
    CAST(r.strength_overall_away AS INT64) AS strength_overall_away,
    
    a.total_xg,
    a.total_xgc,
    a.total_goals_scored,
    a.total_goals_conceded,
    a.avg_xg_per_90,
    a.avg_xgc_per_90

FROM team_taxonomies t
LEFT JOIN raw_team r ON t.team_id = r.id
LEFT JOIN team_aggregates a ON t.team_id = a.team_id