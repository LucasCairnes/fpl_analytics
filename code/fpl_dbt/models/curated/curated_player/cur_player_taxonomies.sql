{{ config(materialized='table') }}

WITH player_data AS (
    SELECT * FROM {{ ref('stg_player_data') }}
),

team_data AS (
    SELECT * FROM {{ ref('curated_team','cur_team_taxonomies') }}
),

cur_player_taxonomies AS (
  SELECT
    p.player_id,
    CASE
      WHEN LENGTH(p.known_name) > 0 THEN p.known_name
      ELSE CONCAT(p.first_name, ' ', p.second_name)
      END AS player_name,
    t.team_name,
    t.team_id,
    CASE
      WHEN p.element_type = 1 THEN 'GK'
      WHEN p.element_type = 2 THEN 'DEF'
      WHEN p.element_type = 3 THEN 'MID'
      WHEN p.element_type = 4 THEN 'FWD'
      ELSE 'Unknown'
    END AS position,
    p.pl_id,
    CONCAT('https://resources.premierleague.com/premierleague25/photos/players/110x140/', p.pl_id, '.png') AS player_image
  FROM 
    player_data p
  LEFT JOIN 
    team_data t
    ON p.team_id = t.team_id
)

SELECT * FROM cur_player_taxonomies