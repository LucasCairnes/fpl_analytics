{{ config(materialized='table') }}

WITH player_data AS (
    SELECT * FROM {{ source('stg_player','stg_selection_info') }}
),

cur_selection_info AS (
    SELECT
        player_id,
        transfer_value,
        selected_by_percent,
        transfers_in,
        transfers_out,
        CASE 
            WHEN status = "a" THEN "Available"
            WHEN status = "d" THEN "Doubtful"
            WHEN status = "s" THEN "Suspended"
            WHEN status = "u" THEN "Unavailable"
            WHEN status = "n" THEN "Not Available"
            ELSE "Unknown"
        END AS status,
    FROM player_data
)

SELECT * FROM cur_selection_info