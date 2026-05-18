

WITH fixture_data AS (
    SELECT * FROM `fpl-analytics-488811`.`curated_fixture`.`cur_next_five_fixtures`
),

team_info AS (
    SELECT * FROM `fpl-analytics-488811`.`curated_team`.`cur_team_taxonomies` 
),

fixture_matrix AS (
    SELECT
        t.team_logo,
        t.team_name,
        CASE WHEN t1.short_name IS NOT NULL THEN t1.short_name || ' (' || f.venue_1 || ')' ELSE '' END AS opp1,
        CASE WHEN t2.short_name IS NOT NULL THEN t2.short_name || ' (' || f.venue_2 || ')' ELSE '' END AS opp2,
        CASE WHEN t3.short_name IS NOT NULL THEN t3.short_name || ' (' || f.venue_3 || ')' ELSE '' END AS opp3,
        CASE WHEN t4.short_name IS NOT NULL THEN t4.short_name || ' (' || f.venue_4 || ')' ELSE '' END AS opp4,
        CASE WHEN t5.short_name IS NOT NULL THEN t5.short_name || ' (' || f.venue_5 || ')' ELSE '' END AS opp5,
        f.mean_difficulty_next_5
    FROM fixture_data f
    LEFT JOIN team_info t
        ON f.team_id = t.team_id
    LEFT JOIN team_info t1
        ON f.opp_1 = t1.team_id
    LEFT JOIN team_info t2
        ON f.opp_2 = t2.team_id
    LEFT JOIN team_info t3
        ON f.opp_3 = t3.team_id
    LEFT JOIN team_info t4
        ON f.opp_4 = t4.team_id
    LEFT JOIN team_info t5
        ON f.opp_5 = t5.team_id
)

SELECT * FROM fixture_matrix