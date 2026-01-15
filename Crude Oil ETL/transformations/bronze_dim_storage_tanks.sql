
-- Please edit the sample below

CREATE OR REFRESH STREAMING TABLE bronze_dim_storage_tanks AS
SELECT
    *
FROM STREAM(dim_storage_tanks);