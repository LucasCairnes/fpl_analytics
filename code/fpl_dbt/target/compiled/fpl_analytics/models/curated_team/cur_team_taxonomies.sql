

WITH team_data AS (
  SELECT * FROM `fpl-analytics-488811`.`raw_team`.`raw_team_data`
),

cur_team_taxonomies AS (
  SELECT 
    id AS team_id,
    name AS team_name,
    short_name,
    code AS pl_code,
    CONCAT('https://resources.premierleague.com/premierleague/badges/t', code,'.png') AS team_logo
  FROM team_data
)

SELECT * FROM cur_team_taxonomies