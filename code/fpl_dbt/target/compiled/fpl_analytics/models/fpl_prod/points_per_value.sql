

WITH player_data AS (
    SELECT * FROM `fpl-analytics-488811`.`curated_player`.`cur_player_taxonomies`
),

rolling_stats AS (
    SELECT * FROM `fpl-analytics-488811`.`curated_player`.`cur_player_ten_gw_stats`
),

transfer_stats AS (
    SELECT * FROM `fpl-analytics-488811`.`curated_player`.`cur_selection_info`
),

team_info AS (
    SELECT * FROM `fpl-analytics-488811`.`curated_team`.`cur_team_taxonomies` 
),

points_per_value AS (
    SELECT
        p.player_name,
        p.player_image,
        p.team_name,
        ti.team_logo,
        p.position,
        ts.transfer_value,
        r.ten_gw_avg_pts,
        ts.selected_by_percent,
        ROUND(r.ten_gw_avg_pts / ts.transfer_value, 3) AS points_per_value
    FROM rolling_stats r
    LEFT JOIN player_data p
        ON r.player_id = p.player_id
    LEFT JOIN transfer_stats ts
        ON r.player_id = ts.player_id
    LEFT JOIN team_info ti
        ON p.team_name = ti.team_name 
    WHERE r.gameweek = (SELECT MAX(gameweek) FROM rolling_stats)
    ORDER BY ten_gw_avg_pts ASC
)

SELECT * FROM points_per_value