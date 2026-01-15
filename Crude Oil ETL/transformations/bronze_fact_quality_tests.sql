
-- Please edit the sample below

CREATE OR REFRESH STREAMING TABLE bronze_fact_quality_tests AS
SELECT
    *
FROM STREAM(fact_quality_tests);