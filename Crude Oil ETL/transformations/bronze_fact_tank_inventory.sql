
-- Please edit the sample below

CREATE OR REFRESH STREAMING TABLE bronze_fact_tank_inventory AS
SELECT
    *
FROM STREAM(fact_tank_inventory);