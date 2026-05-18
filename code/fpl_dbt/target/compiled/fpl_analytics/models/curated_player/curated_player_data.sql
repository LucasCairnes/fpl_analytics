

WITH player_data AS (
    SELECT * FROM `fpl-analytics-488811`.`raw_player_data`.`full_player_data`
),

team_data AS (
    SELECT * FROM `fpl-analytics-488811`.`curated_team_data`.`team_taxonomies`
),


curated_player_data AS (
  SELECT
    p.id AS player_id,
    CASE
      WHEN LENGTH(p.known_name) > 0 THEN p.known_name
      ELSE CONCAT(p.first_name, ' ', p.second_name)
      END AS player_name,
    t.team_name AS team_name,
    CASE
      WHEN p.element_type = 1 THEN 'GK'
      WHEN p.element_type = 2 THEN 'DEF'
      WHEN p.element_type = 3 THEN 'MID'
      WHEN p.element_type = 4 THEN 'FWD'
      ELSE 'Unknown'
    END AS position,
    p.code AS pl_id,
    CONCAT('https://resources.premierleague.com/premierleague/photos/players/110x140/p', p.code, '.png') AS image_url
  FROM 
    player_data p
  LEFT JOIN 
    team_data t
    ON p.team = t.team_id
)

SELECT * FROM curated_player_data