{{ config(
    materialized='incremental',
    alias=var('output_table', 'raw_player_stats')
) }}

WITH source_data AS (
    SELECT * FROM {{ var('input_table', 'raw_player_stats') }}
)

SELECT * FROM source_data

{% if is_incremental() %}

  EXCEPT DISTINCT
  SELECT * FROM {{ this }}

{% endif %}