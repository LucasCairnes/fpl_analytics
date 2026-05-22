{{ config(materialized='table') }}

WITH team_stats AS (
    SELECT * FROM {{ ref('cur_team_stats') }}
),

team_strength_profiler AS(
    team_id,
    team_name,
    short_name,
    strength_attack_home,
    strength_attack_away,
    strength_defence_home,
    strength_defence_away,
    strength_overall_home,
    strength_overall_away,
    
    (strength_attack_home + strength_attack_away) / 2.0 AS avg_attack_strength,
    (strength_defence_home + strength_defence_away) / 2.0 AS avg_defence_strength
    FROM team_stats
)

SELECT * FROM team_strength_profiler