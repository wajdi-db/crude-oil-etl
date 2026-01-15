-- Please edit the sample below

CREATE OR REFRESH MATERIALIZED VIEW gold_anomaly_alerts AS
SELECT 
    refinery_id,
    unit_id,
    sensor_type,
    window(reading_timestamp, '5 minutes') AS time_window,
    COUNT(*) AS reading_count,
    SUM(CASE WHEN alert_status = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_count,
    SUM(CASE WHEN alert_status = 'WARNING' THEN 1 ELSE 0 END) AS warning_count,
    AVG(reading_value) AS avg_value,
    STDDEV(reading_value) AS stddev_value,
    MIN(reading_value) AS min_value,
    MAX(reading_value) AS max_value,
    
    -- Alert severity score
    (SUM(CASE WHEN alert_status = 'CRITICAL' THEN 10 
                WHEN alert_status = 'WARNING' THEN 3 
                ELSE 0 END) / COUNT(*)) AS alert_severity_score
                
    FROM silver_sensor_readings
    GROUP BY refinery_id, unit_id, sensor_type, window(reading_timestamp, '5 minutes')
    HAVING critical_count > 0 OR warning_count > 5
