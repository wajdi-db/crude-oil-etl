-- Please edit the sample below

CREATE MATERIALIZED VIEW gold_product_yield AS
SELECT 
        ct.crude_type_id,
        ct.crude_name,
        ct.classification AS crude_class,
        ct.sweetness,
        p.product_id,
        p.product_name,
        p.product_category,
        
        -- Average yield by crude/product combination
        AVG(po.yield_percentage) AS avg_yield,
        STDDEV(po.yield_percentage) AS yield_stddev,
        MIN(po.yield_percentage) AS min_yield,
        MAX(po.yield_percentage) AS max_yield,
        COUNT(*) AS sample_count,
        
        -- Compare to typical yield
        AVG(po.yield_percentage) - p.typical_yield_pct AS yield_vs_typical
        
    FROM silver_production po
    JOIN bronze_dim_crude_types ct ON po.input_crude_type_id = ct.crude_type_id
    JOIN bronze_dim_products p ON po.output_product_id = p.product_id
    GROUP BY ct.crude_type_id, ct.crude_name, ct.classification, ct.sweetness,
             p.product_id, p.product_name, p.product_category, p.typical_yield_pct