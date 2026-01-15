CREATE OR REFRESH STREAMING TABLE silver_tank_inventory
COMMENT "Cleansed tank inventory with status classification"
AS SELECT
    *,
    CASE
        WHEN fill_percentage < 0.10 THEN 'CRITICALLY_LOW'
        WHEN fill_percentage < 0.25 THEN 'LOW'
        WHEN fill_percentage > 0.95 THEN 'OVER_FILLED'
        WHEN fill_percentage > 0.85 THEN 'NEAR_CAPACITY'
        ELSE 'NORMAL'
    END AS inventory_status,
    
    CASE
        WHEN fill_percentage < 0.15 OR fill_percentage > 0.92 OR water_bottom_inches > 6.0
        THEN true
        ELSE false
    END AS requires_attention,
    
    CASE
        WHEN fill_percentage < 0.95 
        THEN CAST((0.95 - fill_percentage) * volume_barrels / fill_percentage AS INT)
        ELSE 0
    END AS available_capacity_barrels,
    
    current_timestamp() AS processed_at
    
FROM STREAM(bronze_fact_tank_inventory)
WHERE volume_barrels >= 0
  AND fill_percentage BETWEEN 0 AND 1;
