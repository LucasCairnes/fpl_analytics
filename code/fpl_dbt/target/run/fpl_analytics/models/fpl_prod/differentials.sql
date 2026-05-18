
  
    

    create or replace table `fpl-analytics-488811`.`fpl_prod`.`differentials`
      
    
    

    
    OPTIONS()
    as (
      

WITH taxonomies AS (
    SELECT * FROM `fpl-analytics-488811`.`curated_player`.`cur_player_taxonomies`
),

selection AS (
    SELECT * FROM `fpl-analytics-488811`.`curated_player`.`cur_selection_info`
),

stats AS (
    SELECT * FROM `fpl-analytics-488811`.`curated_player`.`cur_total_stats`
),

differentials AS (
    SELECT
        t.player_name AS player,
        t.position,
        t.player_image,
        s.form,
        sel.selected_by_percent
    FROM taxonomies t
    LEFT JOIN selection sel ON t.player_id = sel.player_id
    LEFT JOIN stats s ON t.player_id = s.player_id
    WHERE sel.selected_by_percent < 0.05
)

SELECT * FROM differentials
    );
  