-- Please edit the sample below

CREATE OR REFRESH MATERIALIZED VIEW gold_equipment_health AS
    WITH sensor_stats AS (
        SELECT 
            unit_id,
            sensor_type,
            DATE(reading_timestamp) AS reading_date,
            AVG(reading_value) AS avg_value,
            STDDEV(reading_value) AS value_volatility,
            SUM(CASE WHEN alert_status != 'NORMAL' THEN 1 ELSE 0 END) / COUNT(*) AS anomaly_rate
        FROM silver_sensor_readings
        WHERE reading_timestamp >= DATE_SUB(current_date(), 7)
        GROUP BY unit_id, sensor_type, DATE(reading_timestamp)
    ),
    unit_scores AS (
        SELECT 
            unit_id,
            reading_date,
            AVG(anomaly_rate) AS avg_anomaly_rate,
            AVG(value_volatility) AS avg_volatility
        FROM sensor_stats
        GROUP BY unit_id, reading_date
    )
    SELECT 
        u.unit_id,
        u.unit_name,
        u.unit_type,
        u.refinery_id,
        r.refinery_name,
        us.reading_date,
        
        -- Health score (100 = perfect, lower = worse)
        GREATEST(0, 100 - (us.avg_anomaly_rate * 500) - (us.avg_volatility / 10)) AS health_score,
        
        -- Days since maintenance
        DATEDIFF(current_date(), u.last_maintenance_date) AS days_since_maintenance,
        u.maintenance_interval_days,
        
        -- Maintenance urgency
        CASE 
            WHEN DATEDIFF(current_date(), u.last_maintenance_date) > u.maintenance_interval_days THEN 'OVERDUE'
            WHEN DATEDIFF(current_date(), u.last_maintenance_date) > u.maintenance_interval_days * 0.9 THEN 'DUE_SOON'
            ELSE 'OK'
        END AS maintenance_status
        
    FROM bronze_dim_processing_units u
    LEFT JOIN unit_scores us ON u.unit_id = us.unit_id
    LEFT JOIN bronze_dim_refineries r ON u.refinery_id = r.refinery_id
