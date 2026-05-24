{{ config(materialized='table') }}

WITH player_data AS (
    SELECT * FROM {{ ref('cur_player_taxonomies') }}
),

selection_info AS (
    SELECT * FROM {{ ref('cur_selection_info') }}
),

fixtures AS (
    SELECT * FROM {{ ref('fixture_matrix') }}
),

rotational_players AS (
    SELECT
        p.player_name AS player,
        p.player_image,
        p.position,
        p.team_name,
        s.transfer_value,
        f.opp1,
        f.opp2,
        f.opp3,
        f.opp4,
        f.opp5,
        f.mean_difficulty_next_5
    FROM player_data p
    INNER JOIN selection_info s
        ON p.player_id = s.player_id
    LEFT JOIN fixtures f
        ON p.team_name = f.team_name
    WHERE 
        AND s.transfer_value <= 4.5
)

SELECT * FROM rotational_players
