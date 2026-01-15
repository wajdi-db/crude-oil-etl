
-- Please edit the sample below

CREATE OR REFRESH STREAMING TABLE bronze_fact_crude_shipments AS
SELECT
    *
FROM STREAM(fact_crude_shipments);