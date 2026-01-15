
-- Please edit the sample below

CREATE OR REFRESH STREAMING TABLE bronze_dim_crude_types AS
SELECT
*
FROM STREAM(dim_crude_types);