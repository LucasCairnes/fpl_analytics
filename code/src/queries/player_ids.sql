WITH todays_teams AS (
    SELECT
    team_h AS team_id
    FROM `fpl-analytics-488811.stg_fixture.stg_fixture_data`
    WHERE date = DATE_SUB(CURRENT_DATE('Europe/London'), INTERVAL 1 DAY )

    UNION ALL

    SELECT
    team_a AS team_id
    FROM `fpl-analytics-488811.stg_fixture.stg_fixture_data`
    WHERE date = DATE_SUB(CURRENT_DATE('Europe/London'), INTERVAL 1 DAY )
)

SELECT
DISTINCT(player_id)
FROM `fpl-analytics-488811.curated_player.cur_player_taxonomies`
WHERE team_id IN (SELECT team_id FROM todays_teams)