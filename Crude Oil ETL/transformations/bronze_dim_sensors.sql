
-- Please edit the sample below

CREATE OR REFRESH STREAMING TABLE bronze_dim_sensors AS
SELECT
    *
FROM STREAM(dim_sensors);