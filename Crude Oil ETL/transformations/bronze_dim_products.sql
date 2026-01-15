
-- Please edit the sample below

CREATE OR REFRESH STREAMING TABLE bronze_dim_products AS
SELECT
    *
FROM STREAM(dim_products);