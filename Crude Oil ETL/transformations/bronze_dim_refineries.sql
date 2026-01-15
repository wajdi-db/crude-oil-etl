
-- Please edit the sample below

CREATE OR REFRESH STREAMING TABLE bronze_dim_refineries AS
SELECT
    *
FROM STREAM(dim_refineries);