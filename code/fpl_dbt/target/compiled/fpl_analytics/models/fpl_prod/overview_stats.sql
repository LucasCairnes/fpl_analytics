

WITH player_data AS (
    SELECT * FROM `fpl-analytics-488811`.`curated_player`.`cur_player_taxonomies`
),

player_stats AS (
    SELECT * FROM `fpl-analytics-488811`.`curated_player`.`cur_total_stats` 
),

team_info AS (
    SELECT * FROM `fpl-analytics-488811`.`curated_team`.`cur_team_taxonomies` 
),

selection_info AS (
    SELECT * FROM `fpl-analytics-488811`.`curated_player`.`cur_selection_info` 
),

overview_stats AS (
    SELECT
        d.player_name,
        d.player_image,
        d.position,
        s.goals_scored,
        s.assists,
        s.clean_sheets AS clean_sheets,
        t.team_logo,
        si.transfer_value,
        (si.transfers_in - si.transfers_out) AS net_transfers,
        total_points
    FROM player_data d
    LEFT JOIN player_stats s
        ON d.player_id = s.player_id
    LEFT JOIN selection_info si
        ON d.player_id = si.player_id
    LEFT JOIN team_info t 
        ON d.team_name = t.team_name
)

SELECT * FROM overview_stats