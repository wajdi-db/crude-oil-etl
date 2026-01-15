
-- Please edit the sample below

CREATE OR REFRESH STREAMING TABLE bronze_fact_production_output AS
SELECT
    *
FROM STREAM(fact_production_output);