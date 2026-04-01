{{ config(materialized='table') }}

WITH player_data AS (
    SELECT * FROM {{ source('stg_player_data','stg_selection_info') }}
)

transfer_stats AS (
    SELECT
        player_id,
        transfer_value,
        selected_by_percent,
        transfers_in,
        transfers_out
    FROM player_data
)

SELECT * FROM transfer_stats