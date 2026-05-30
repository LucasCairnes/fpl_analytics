{{ config(materialized='table') }}

WITH team_stats AS (
    SELECT * FROM {{ ref('cur_team_stats') }}
),
team_info AS (
    SELECT * FROM {{ ref('cur_team_taxonomies') }}
)

SELECT
    s.team_id,
    s.team_name,
    s.short_name,
    i.team_logo,
    s.strength_attack_home,
    s.strength_attack_away,
    s.strength_defence_home,
    s.strength_defence_away,
    s.strength_overall_home,
    s.strength_overall_away,
    (s.strength_attack_home + s.strength_attack_away) / 2.0 AS avg_attack_strength,
    (s.strength_defence_home + s.strength_defence_away) / 2.0 AS avg_defence_strength,
    s.total_xg,
    s.total_xgc,
    s.total_goals_scored,
    s.total_goals_conceded,
    ROUND(s.total_goals_scored - s.total_xg, 2) AS xg_performance,
    ROUND(s.total_goals_conceded - s.total_xgc, 2) AS xgc_performance,
    s.avg_xg_per_90,
    s.avg_xgc_per_90
FROM team_stats s
LEFT JOIN team_info i ON s.team_id = i.team_id