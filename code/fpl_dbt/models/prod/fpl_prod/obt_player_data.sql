{{ config(materialized='table') }}

WITH taxonomies AS (
    SELECT * FROM {{ ref('cur_player_taxonomies') }}
),
stats AS (
    SELECT * FROM {{ ref('cur_total_stats') }}
),
selection AS (
    SELECT * FROM {{ ref('cur_selection_info') }}
),
teams AS (
    SELECT * FROM {{ ref('cur_team_taxonomies') }}
),
five_gw AS (
    SELECT * FROM {{ ref('cur_player_five_gw_stats') }}
    WHERE gameweek = (SELECT MAX(gameweek) FROM {{ ref('cur_player_five_gw_stats') }})
),
ten_gw AS (
    SELECT * FROM {{ ref('cur_player_ten_gw_stats') }}
    WHERE gameweek = (SELECT MAX(gameweek) FROM {{ ref('cur_player_ten_gw_stats') }})
)

SELECT
    t.player_id,
    t.player_name AS player,
    t.position,
    t.player_image,
    t.team_id,
    t.team_name,
    tm.team_logo,
    sel.transfer_value,
    sel.selected_by_percent,
    sel.transfers_in,
    sel.transfers_out,
    (sel.transfers_in - sel.transfers_out) AS net_transfers,
    sel.status,
    s.goals_scored,
    s.assists,
    s.clean_sheets,
    s.expected_goals,
    s.expected_assists,
    ROUND(CAST(s.expected_goals AS FLOAT64) + CAST(s.expected_assists AS FLOAT64), 3) AS xgi,
    s.total_points,
    CAST(s.form AS FLOAT64) AS form,
    s.influence,
    s.creativity,
    s.threat,
    s.yellow_cards,
    s.red_cards,
    s.bps,
    s.minutes,
    f.five_gw_goals,
    f.five_gw_assists,
    ROUND(f.five_gw_xg, 3) AS five_gw_xg,
    ROUND(f.five_gw_xa, 3) AS five_gw_xa,
    ROUND(f.xga_performance, 3) AS xga_performance,
    tn.ten_gw_avg_pts
FROM taxonomies t
LEFT JOIN stats s ON t.player_id = s.player_id
LEFT JOIN selection sel ON t.player_id = sel.player_id
LEFT JOIN teams tm ON t.team_id = tm.team_id
LEFT JOIN five_gw f ON t.player_id = f.player_id
LEFT JOIN ten_gw tn ON t.player_id = tn.player_id