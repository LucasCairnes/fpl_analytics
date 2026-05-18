
  
    

    create or replace table `fpl-analytics-488811`.`curated_fixture`.`cur_current_gameweek`
      
    
    

    
    OPTIONS()
    as (
      

WITH fixture_data AS (
    SELECT * FROM `fpl-analytics-488811`.`stg_fixture`.`stg_fixture_data`
),

cur_current_gameweek AS (
    SELECT
        gameweek
    FROM fixture_data
    WHERE finished = false AND gameweek IS NOT NULL
    ORDER BY gameweek ASC
    LIMIT 1
)

SELECT * FROM cur_current_gameweek
    );
  