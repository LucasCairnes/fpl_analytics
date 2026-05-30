{{ config(materialized='view') }}

SELECT
    player,
    player_image,
    position,
    team_name,
    team_logo,
    total_points,
    minutes,
    CONCAT(goals_scored, ' (', expected_goals, ')') AS goals_and_expected,
    CONCAT(assists, ' (', expected_assists, ')') AS assists_and_expected,
    clean_sheets,
    influence,
    creativity,
    threat,
    form,
    yellow_cards,
    red_cards,
    bps,
    transfer_value,
    selected_by_percent,
    transfers_in,
    transfers_out,
    status
FROM {{ ref('obt_player_reporting') }}