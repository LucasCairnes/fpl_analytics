{{ config(materialized='table') }}

WITH player_data AS (
    SELECT * FROM {{ ref('cur_player_taxonomies') }}
),

selection_info AS (
    SELECT * FROM {{ ref('cur_selection_info') }}
),

transfer_momentum AS (
    SELECT
        p.player_name AS player,
        p.player_image,
        p.position,
        p.team_name,
        s.transfer_value,
        s.selected_by_percent,
        s.transfers_in,
        s.transfers_out,
        (s.transfers_in - s.transfers_out) AS net_transfers,
        s.status
    FROM player_data p
    INNER JOIN selection_info s
        ON p.player_id = s.player_id
    WHERE (s.transfers_in > 0 OR s.transfers_out > 0)
)

SELECT * FROM transfer_momentum