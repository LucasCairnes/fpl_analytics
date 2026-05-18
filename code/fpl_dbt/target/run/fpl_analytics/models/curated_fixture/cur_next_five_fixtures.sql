
  
    

    create or replace table `fpl-analytics-488811`.`curated_fixture`.`cur_next_five_fixtures`
      
    
    

    
    OPTIONS()
    as (
      

WITH fixture_data AS (
    SELECT * FROM `fpl-analytics-488811`.`stg_fixture`.`stg_fixtures_by_team`
),

ordered_fixtures AS (
    SELECT 
        team_id,
        opponent_id,
        match_difficulty,
        venue,
        ROW_NUMBER() OVER (
            PARTITION BY team_id 
            ORDER BY gameweek ASC, fixture_id ASC
        ) AS match_order
    FROM fixture_data
),

cur_next_five_fixtures AS (
    SELECT
        team_id,
        MAX(CASE WHEN match_order = 1 THEN opponent_id END) AS opp_1,
        MAX(CASE WHEN match_order = 1 THEN venue END) AS venue_1,
        MAX(CASE WHEN match_order = 2 THEN opponent_id END) AS opp_2,
        MAX(CASE WHEN match_order = 2 THEN venue END) AS venue_2,
        MAX(CASE WHEN match_order = 3 THEN opponent_id END) AS opp_3,
        MAX(CASE WHEN match_order = 3 THEN venue END) AS venue_3,
        MAX(CASE WHEN match_order = 4 THEN opponent_id END) AS opp_4,
        MAX(CASE WHEN match_order = 4 THEN venue END) AS venue_4,
        MAX(CASE WHEN match_order = 5 THEN opponent_id END) AS opp_5,
        MAX(CASE WHEN match_order = 5 THEN venue END) AS venue_5,
        AVG(CASE WHEN match_order <= 5 THEN CAST(match_difficulty AS FLOAT64) END) AS mean_difficulty_next_5
    FROM ordered_fixtures
    WHERE match_order <= 5
    GROUP BY team_id
)

SELECT * FROM cur_next_five_fixtures
    );
  