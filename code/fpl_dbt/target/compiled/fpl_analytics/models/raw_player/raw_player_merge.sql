

WITH source_data AS (
    SELECT * FROM fpl-analytics-488811.temporary.temp_raw_player_stats
)

SELECT * FROM source_data



  EXCEPT DISTINCT
  SELECT * FROM `fpl-analytics-488811`.`raw_player`.`raw_player_stats`

