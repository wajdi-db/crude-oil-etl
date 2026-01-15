
CREATE OR REFRESH STREAMING TABLE silver_production AS
SELECT 
    po.*,
    u.unit_name,
    u.unit_type,
    u.design_efficiency,
    u.capacity_bpd AS unit_capacity_bpd,
    r.refinery_name,
    p.product_name,
    p.product_category,
    ct.crude_name,
    po.yield_percentage / u.design_efficiency AS efficiency_ratio,
    po.output_volume_barrels / (u.capacity_bpd / 24.0) AS capacity_utilization,
    po.energy_consumed_mmbtu / po.output_volume_barrels AS energy_per_barrel,
    current_timestamp() AS silver_processed_at
FROM STREAM(bronze_fact_production_output) po
LEFT JOIN bronze_dim_processing_units u ON po.unit_id = u.unit_id
LEFT JOIN bronze_dim_refineries r ON po.refinery_id = r.refinery_id
LEFT JOIN bronze_dim_products p ON po.output_product_id = p.product_id
LEFT JOIN bronze_dim_crude_types ct ON po.input_crude_type_id = ct.crude_type_id