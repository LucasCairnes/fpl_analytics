{{ config(materialized='table') }}

WITH team_stats AS (
    SELECT * FROM {{ ref('cur_team_stats') }}
),

team_info AS (
    SELECT * FROM {{ ref('cur_team_taxonomies') }}
),

expected_team_stats AS(
    SELECT
        s.team_id,
        s.team_name,
        i.team_logo,
        
        s.total_xg,
        s.total_goals_scored,
        ROUND(s.total_goals_scored - s.total_xg, 2) AS xg_performance,
        
        s.total_xgc,
        s.total_goals_conceded,
        ROUND(s.total_goals_conceded - s.total_xgc, 2) AS xgc_performance,
        
        s.avg_xg_per_90,
        s.avg_xgc_per_90
    FROM team_stats s
    LEFT JOIN team_info i ON s.team_id = i.team_id
)

SELECT * FROM expected_team_stats