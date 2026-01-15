
CREATE OR REFRESH STREAMING TABLE silver_shipments AS
SELECT 
    cs.*,
    r.refinery_name,
    r.location_city,
    r.location_state,
    r.capacity_bpd AS refinery_capacity_bpd,
    ct.crude_name,
    ct.classification AS crude_classification,
    ct.sweetness AS crude_sweetness,
    ct.api_gravity_typical,
    ct.sulfur_content_typical,
    
    -- Calculated fields
    cs.api_gravity_actual - ct.api_gravity_typical AS api_gravity_variance,
    cs.sulfur_content_actual - ct.sulfur_content_typical AS sulfur_variance,
    DATEDIFF(cs.arrival_date, cs.expected_arrival_date) AS days_delayed,
    
    -- Cost metrics
    cs.total_cost / cs.volume_barrels AS cost_per_barrel,
    
    current_timestamp() AS silver_processed_at
    
    FROM STREAM(bronze_fact_crude_shipments) cs
    LEFT JOIN bronze_dim_refineries r ON cs.refinery_id = r.refinery_id
    LEFT JOIN bronze_dim_crude_types ct ON cs.crude_type_id = ct.crude_type_id