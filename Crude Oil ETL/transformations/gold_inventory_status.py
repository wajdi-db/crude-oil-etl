from pyspark.pipelines import table
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number, desc, col, when
)


@table(
    name="gold_inventory_status",
    comment="Current inventory status across all tanks",
    table_properties={"quality": "gold"}
)
def gold_inventory_status():
    """
    Get latest inventory reading for each tank with status.
    This is a batch/materialized view since we need the latest row per tank.
    """
    return (
        spark.read.table("silver_tank_inventory")
        
        # Get latest reading per tank using window function
        .withColumn("row_num",
            row_number().over(
                Window.partitionBy("tank_id").orderBy(desc("measurement_timestamp"))
            )
        )
        .filter(col("row_num") == 1)
        .drop("row_num")
        
        # Join with tank dimensions for additional context
        .join(
            spark.read.table("bronze_dim_storage_tanks")
                .select("tank_id", "tank_name", "capacity_barrels", 
                       "min_operating_level", "max_operating_level"),
            "tank_id",
            "left"
        )
        
        # Calculate days of supply (simplified - would need consumption rate)
        .withColumn("capacity_utilization", 
            col("volume_barrels") / col("capacity_barrels")
        )
        
        # Refine status based on operating levels
        .withColumn("operating_status",
            when(col("fill_percentage") < col("min_operating_level"), "BELOW_MIN")
            .when(col("fill_percentage") > col("max_operating_level"), "ABOVE_MAX")
            .otherwise("WITHIN_RANGE")
        )
        
        .select(
            "tank_id",
            "tank_name",
            "refinery_id",
            "product_type",
            "volume_barrels",
            "capacity_barrels",
            "fill_percentage",
            "capacity_utilization",
            "inventory_status",
            "operating_status",
            "requires_attention",
            "available_capacity_barrels",
            "temperature_f",
            "water_bottom_inches",
            "measurement_timestamp",
            "processed_at"
        )
    )
