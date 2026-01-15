
-- Please edit the sample below

CREATE OR REFRESH STREAMING TABLE bronze_dim_processing_units AS
SELECT
    *
FROM STREAM(dim_processing_units);