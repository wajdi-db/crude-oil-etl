
-- Please edit the sample below

CREATE OR REFRESH STREAMING TABLE silver_sensor_readings AS
SELECT 
    sr.reading_id,
    sr.sensor_id,
    sr.unit_id,
    sr.refinery_id,
    sr.reading_timestamp,
    sr.reading_value,
    sr.reading_quality,
    s.sensor_type,
    s.measurement_unit,
    s.warning_low,
    s.warning_high,
    s.critical_low,
    s.critical_high,
    -- Anomaly detection
    CASE 
        WHEN sr.reading_value < s.critical_low OR sr.reading_value > s.critical_high THEN 'CRITICAL'
        WHEN sr.reading_value < s.warning_low OR sr.reading_value > s.warning_high THEN 'WARNING'
        ELSE 'NORMAL'
    END AS alert_status,
    -- Data quality flags
    (sr.reading_value < s.min_value OR sr.reading_value > s.max_value) AS is_out_of_range,
    sr.event_time,
    sr.processing_time,
    current_timestamp() AS silver_processed_at
FROM STREAM(bronze_fact_sensor_readings) sr
LEFT JOIN bronze_dim_sensors s ON sr.sensor_id = s.sensor_id
WHERE s.is_active = true

UNION ALL

SELECT 
    srt.reading_id,
    srt.sensor_id,
    srt.unit_id,
    srt.refinery_id,
    srt.reading_timestamp,
    srt.reading_value,
    srt.reading_quality,
    s.sensor_type,
    s.measurement_unit,
    s.warning_low,
    s.warning_high,
    s.critical_low,
    s.critical_high,
    -- Anomaly detection
    CASE 
        WHEN srt.reading_value < s.critical_low OR srt.reading_value > s.critical_high THEN 'CRITICAL'
        WHEN srt.reading_value < s.warning_low OR srt.reading_value > s.warning_high THEN 'WARNING'
        ELSE 'NORMAL'
    END AS alert_status,
    -- Data quality flags
    (srt.reading_value < s.min_value OR srt.reading_value > s.max_value) AS is_out_of_range,
    srt.event_time,
    srt.processing_time,
    current_timestamp() AS silver_processed_at
FROM STREAM(bronze_fact_sensor_readings_stream) srt
LEFT JOIN bronze_dim_sensors s ON srt.sensor_id = s.sensor_id
WHERE s.is_active = true