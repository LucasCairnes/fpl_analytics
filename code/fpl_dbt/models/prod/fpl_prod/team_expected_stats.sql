{{ config(materialized='view') }}

SELECT
    team_id,
    team_name,
    team_logo,
    total_xg,
    total_goals_scored,
    xg_performance,
    total_xgc,
    total_goals_conceded,
    xgc_performance,
    avg_xg_per_90,
    avg_xgc_per_90
FROM {{ ref('obt_team_reporting') }}