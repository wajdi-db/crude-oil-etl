CREATE OR REFRESH STREAMING TABLE bronze_fact_sensor_readings_stream AS
SELECT
  value::string:reading_id::string as reading_id,
  value::string:sensor_id::string as sensor_id,
  value::string:unit_id::string as unit_id,
  value::string:refinery_id::string as refinery_id,
  value::string:reading_timestamp::string as reading_timestamp,
  value::string:reading_value::double as reading_value,
  value::string:reading_quality::string as reading_quality,
  value::string:is_interpolated:boolean as is_interpolated,
  value::string:event_time::string as event_time,
  value::string:processing_time:string as processing_time
FROM
  STREAM read_kafka(
    bootstrapServers => "parex-demo.servicebus.windows.net:9093",
    subscribe => "sensor-readings",
    `kafka.security.protocol` => 'SASL_SSL',
    `kafka.sasl.mechanism` => 'PLAIN',
    `kafka.sasl.jaas.config` =>
      'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="\\$ConnectionString" password="Endpoint=sb://parex-demo.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=gSro4wk2U7a8TzBuZgAVXhIaPPloFmt6E+AEhOJP0wo=";',
    `kafka.request.timeout.ms` => '60000',
    `kafka.session.timeout.ms` => '30000',
    startingOffsets => 'latest'
  );

CREATE OR REFRESH STREAMING TABLE bronze_fact_sensor_readings AS
select
  reading_id,
  sensor_id,
  unit_id,
  refinery_id,
  reading_timestamp,
  reading_value,
  reading_quality,
  is_interpolated,
  event_time,
  processing_time
from
  STREAM (fact_sensor_readings);

create or refresh streaming table bronze_fact_sensor_readings_coastalenergy as
select
  *
from
  STREAM (wajdi_bounouara.coastalenergy.fact_sensor_readings);
-- CREATE OR REFRESH STREAMING TABLE bronze_fact_sensor_readings_files AS
-- SELECT
--   *
-- FROM
--   STREAM read_files(
--     "/Volumes/wajdi_bounouara/parex_resources/parex-demo-volume/",
--     format => "csv",
--     header => "true",
--     schema =>
--       "reading_id STRING, sensor_id STRING, unit_id STRING, refinery_id STRING, sensor_type STRING, measurement_unit STRING, reading_timestamp STRING, reading_value DOUBLE, reading_quality STRING,is_anomaly BOOLEAN, is_interpolated BOOLEAN, event_time STRING"
--   );
