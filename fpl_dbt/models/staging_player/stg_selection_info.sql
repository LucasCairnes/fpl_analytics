{{ config(materialized='table') }}

WITH player_data AS (
    SELECT * FROM {{ source('raw_player_data', 'full_player_data')}}
),

stg_selection_info AS (
    SELECT
        id AS player_id,
        can_select,
        CAST(now_cost / 10 AS FLOAT64) AS transfer_value,
        selected_by_percent,
        cost_change_event,
        cost_change_event_fall,
        cost_change_start,
        cost_change_start_fall,
        price_change_percent,
        transfers_in,
        transfers_in_event,
        transfers_out,
        transfers_out_event,
        value_season,
        selected_rank,
        selected_rank_type,
    FROM player_data
)

SELECT * FROM stg_selection_info
