{{ config(materialized='table') }}

WITH player_data AS (
    SELECT * FROM {{ ref('cur_player_taxonomies') }}
),

rolling_stats AS (
    SELECT * FROM {{ ref('cur_player_five_gw_stats') }}
    WHERE gameweek = (SELECT MAX(gameweek) FROM {{ ref('cur_player_five_gw_stats') }})
),

selection_info AS (
    SELECT * FROM {{ ref('cur_selection_info') }}
),

fixture_data AS (
    SELECT * FROM {{ ref('cur_next_five_fixtures') }}
),

team_info AS (
    SELECT * FROM {{ ref('cur_team_taxonomies') }}
),

captaincy_matrix AS (
    SELECT
        p.player_name AS player,
        p.player_image,
        p.position,
        p.team_name,
        s.transfer_value,
        ROUND(r.five_gw_xg + r.five_gw_xa, 3) AS five_gw_xgi,
        ROUND(r.five_gw_xg, 3) AS five_gw_xg,
        ROUND(r.five_gw_xa, 3) AS five_gw_xa,
        t_opp.short_name AS next_opponent,
        f.venue_1 AS next_match_venue,
        CONCAT(t_opp.short_name, ' (', f.venue_1, ')') AS next_fixture_display,
        ROUND(f.mean_difficulty_next_5, 2) AS mean_difficulty_next_5,
        s.status
    FROM player_data p
    INNER JOIN rolling_stats r 
        ON p.player_id = r.player_id
    INNER JOIN selection_info s 
        ON p.player_id = s.player_id
    LEFT JOIN fixture_data f 
        ON p.team_id = f.team_id
    LEFT JOIN team_info t_opp 
        ON f.opp_1 = t_opp.team_id
)

SELECT * FROM captaincy_matrix
ORDER BY five_gw_xgi DESC