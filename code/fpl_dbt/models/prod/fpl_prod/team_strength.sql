{{ config(materialized='view') }}

SELECT
    team_id,
    team_name,
    short_name,
    strength_attack_home,
    strength_attack_away,
    strength_defence_home,
    strength_defence_away,
    strength_overall_home,
    strength_overall_away,
    avg_attack_strength,
    avg_defence_strength
FROM {{ ref('obt_team_reporting') }}