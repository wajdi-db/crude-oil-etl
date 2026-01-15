-- Please edit the sample below

CREATE MATERIALIZED VIEW gold_refinery_kpis AS
SELECT 
    r.refinery_id,
    r.refinery_name,
    r.location_city,
    r.location_state,
    r.capacity_bpd,
    DATE(po.production_hour) AS production_date,
    
    -- Production metrics
    SUM(po.input_volume_barrels) AS total_crude_processed,
    SUM(po.output_volume_barrels) AS total_products_produced,
    SUM(po.output_volume_barrels) / SUM(po.input_volume_barrels) AS overall_yield,
    
    -- Capacity utilization
    SUM(po.input_volume_barrels) / r.capacity_bpd AS capacity_utilization,
    
    -- Energy efficiency
    SUM(po.energy_consumed_mmbtu) / SUM(po.output_volume_barrels) AS energy_intensity,
    
    -- Downtime
    SUM(po.downtime_minutes) AS total_downtime_minutes,
    SUM(po.downtime_minutes) / (COUNT(DISTINCT po.unit_id) * 60) AS downtime_percentage,
    
    -- Quality
    SUM(CASE WHEN po.quality_grade = 'On-Spec' THEN 1 ELSE 0 END) / COUNT(*) AS on_spec_rate
    
    FROM silver_production po
    JOIN bronze_dim_refineries r ON po.refinery_id = r.refinery_id
    GROUP BY r.refinery_id, r.refinery_name, r.location_city, r.location_state, 
             r.capacity_bpd, DATE(po.production_hour)
