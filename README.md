# Crude Oil Refinery ETL Pipeline Project

## Executive Summary

This document outlines a complete end-to-end ETL pipeline demonstrating both batch and streaming capabilities in Databricks, centered around **refinery operations optimization and anomaly detection**. The pipeline ingests data from multiple sources, transforms it through the medallion architecture (Bronze → Silver → Gold), and surfaces insights on operational dashboards.

**Total Data Volume Target: ~100GB**

---

## Business Problem Statement

### Problem: Refinery Operational Intelligence Platform

Oil refineries operate 24/7 with thousands of sensors monitoring critical equipment. The challenge is to:

1. **Detect equipment anomalies in real-time** before they cause unplanned shutdowns (streaming)
2. **Optimize crude oil blending** based on quality characteristics and processing unit capabilities (batch)
3. **Track yield efficiency** across processing units and identify underperforming assets (batch + streaming aggregations)
4. **Monitor inventory levels** and predict supply chain disruptions (batch)

### Key Business Questions the Dashboard Should Answer

- Which processing units are showing early warning signs of failure?
- What is our current refinery utilization vs. capacity?
- How does crude oil quality (API gravity, sulfur content) affect product yield?
- What is our daily/weekly production efficiency trend?
- Are there any sensor readings outside normal operating parameters?
- What is our current inventory status across all storage tanks?

---

## Data Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA SOURCES                                    │
├─────────────────┬─────────────────┬─────────────────┬───────────────────────┤
│   STREAMING     │     BATCH       │     BATCH       │        BATCH          │
│                 │                 │                 │                       │
│ Sensor Readings │ Crude Shipments │ Quality Tests   │ Reference/Dimension   │
│ (IoT Events)    │ (Daily loads)   │ (Hourly batches)│ Tables                │
└────────┬────────┴────────┬────────┴────────┬────────┴───────────┬───────────┘
         │                 │                 │                    │
         ▼                 ▼                 ▼                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           BRONZE LAYER (Raw)                                 │
│  - Raw sensor events       - Raw shipment records   - Raw quality results   │
│  - No transformations      - Raw inventory snapshots - Reference data       │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          SILVER LAYER (Cleansed)                            │
│  - Validated sensor data   - Enriched shipments    - Standardized quality  │
│  - Outlier flagging        - Joined with refs      - Unit conversions      │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           GOLD LAYER (Business)                             │
│  - Anomaly alerts          - Yield analytics       - Efficiency KPIs       │
│  - Equipment health scores - Blending optimization - Inventory forecasts   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DASHBOARDS                                      │
│  - Real-time Operations    - Management KPIs       - Predictive Alerts     │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Dataset Specifications

### Overview Table

| Dataset | Type | Volume | Rows (Est.) | Update Frequency |
|---------|------|--------|-------------|------------------|
| Sensor Readings | Streaming | ~70GB | ~2 billion | Continuous |
| Crude Shipments | Batch | ~5GB | ~10 million | Daily |
| Quality Tests | Batch | ~3GB | ~15 million | Hourly |
| Tank Inventory | Batch | ~8GB | ~50 million | Every 15 min |
| Product Output | Batch | ~10GB | ~30 million | Hourly |
| Refineries (Dim) | Batch | ~1MB | ~500 | Weekly |
| Processing Units (Dim) | Batch | ~5MB | ~5,000 | Weekly |
| Storage Tanks (Dim) | Batch | ~2MB | ~2,000 | Weekly |
| Crude Types (Dim) | Batch | ~500KB | ~200 | Monthly |
| Products (Dim) | Batch | ~200KB | ~50 | Monthly |
| Sensors (Dim) | Batch | ~50MB | ~500,000 | Daily |

---

## Detailed Schema Definitions

### 1. DIMENSION TABLES (Reference Data)

#### 1.1 dim_refineries
Master list of refineries in the network.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| refinery_id | STRING | Primary key (UUID) | "REF-001" |
| refinery_name | STRING | Facility name | "Gulf Coast Refinery Alpha" |
| location_city | STRING | City | "Houston" |
| location_state | STRING | State/Province | "TX" |
| location_country | STRING | Country code | "USA" |
| latitude | DOUBLE | GPS latitude | 29.7604 |
| longitude | DOUBLE | GPS longitude | -95.3698 |
| capacity_bpd | INT | Barrels per day capacity | 500000 |
| complexity_index | DOUBLE | Nelson Complexity Index | 12.5 |
| commissioned_date | DATE | When refinery started | 1985-03-15 |
| operator_name | STRING | Operating company | "PetroCorp Inc" |
| is_active | BOOLEAN | Currently operational | true |
| last_turnaround_date | DATE | Last major maintenance | 2023-06-01 |

#### 1.2 dim_processing_units
Individual processing equipment within refineries.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| unit_id | STRING | Primary key | "UNIT-00001" |
| refinery_id | STRING | Foreign key to refinery | "REF-001" |
| unit_name | STRING | Unit designation | "CDU-1" |
| unit_type | STRING | Type of processing unit | "Crude Distillation" |
| unit_subtype | STRING | More specific type | "Atmospheric" |
| capacity_bpd | INT | Unit capacity | 150000 |
| design_efficiency | DOUBLE | Expected efficiency % | 0.94 |
| installation_date | DATE | When installed | 2010-05-20 |
| last_maintenance_date | DATE | Last service date | 2024-01-15 |
| maintenance_interval_days | INT | Days between services | 180 |
| manufacturer | STRING | Equipment maker | "Honeywell UOP" |
| model_number | STRING | Model designation | "CDU-5000X" |
| is_active | BOOLEAN | Currently operational | true |

**Unit Types (for reference):**
- Crude Distillation Unit (CDU)
- Vacuum Distillation Unit (VDU)
- Fluid Catalytic Cracker (FCC)
- Hydrocracker
- Reformer
- Alkylation Unit
- Coker
- Hydrotreater
- Sulfur Recovery Unit

#### 1.3 dim_storage_tanks
Tank farm inventory.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| tank_id | STRING | Primary key | "TANK-00001" |
| refinery_id | STRING | Foreign key | "REF-001" |
| tank_name | STRING | Tank designation | "TK-101" |
| tank_type | STRING | Type of tank | "Floating Roof" |
| product_type | STRING | What it stores | "Crude Oil" |
| capacity_barrels | INT | Max capacity | 500000 |
| min_operating_level | DOUBLE | Minimum fill % | 0.10 |
| max_operating_level | DOUBLE | Maximum fill % | 0.95 |
| diameter_feet | DOUBLE | Tank diameter | 250 |
| height_feet | DOUBLE | Tank height | 48 |
| material | STRING | Construction material | "Carbon Steel" |
| has_heating | BOOLEAN | Heated tank | true |
| has_mixer | BOOLEAN | Has mixing capability | true |
| last_inspection_date | DATE | API inspection date | 2024-02-01 |
| is_active | BOOLEAN | Currently in service | true |

#### 1.4 dim_crude_types
Reference data for crude oil varieties.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| crude_type_id | STRING | Primary key | "CRUDE-001" |
| crude_name | STRING | Common name | "West Texas Intermediate" |
| crude_abbreviation | STRING | Short code | "WTI" |
| origin_country | STRING | Source country | "USA" |
| origin_region | STRING | Source region | "Permian Basin" |
| api_gravity_typical | DOUBLE | Typical API gravity | 39.6 |
| sulfur_content_typical | DOUBLE | Typical sulfur % | 0.24 |
| classification | STRING | Light/Medium/Heavy | "Light" |
| sweetness | STRING | Sweet/Sour | "Sweet" |
| benchmark_price_differential | DOUBLE | vs Brent $/barrel | -3.50 |

#### 1.5 dim_products
Refined petroleum products.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| product_id | STRING | Primary key | "PROD-001" |
| product_name | STRING | Product name | "Regular Gasoline" |
| product_category | STRING | Category | "Gasoline" |
| product_grade | STRING | Quality grade | "87 Octane" |
| density_min | DOUBLE | Min density kg/m³ | 720 |
| density_max | DOUBLE | Max density kg/m³ | 775 |
| sulfur_max_ppm | INT | Max sulfur content | 10 |
| typical_yield_pct | DOUBLE | % from avg crude | 0.46 |
| unit_of_measure | STRING | Standard UOM | "barrels" |

#### 1.6 dim_sensors
Sensor metadata for IoT devices.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| sensor_id | STRING | Primary key | "SENS-000001" |
| unit_id | STRING | FK to processing unit | "UNIT-00001" |
| refinery_id | STRING | FK to refinery | "REF-001" |
| sensor_name | STRING | Sensor designation | "CDU1-TEMP-001" |
| sensor_type | STRING | Type of measurement | "Temperature" |
| measurement_unit | STRING | Unit of measure | "Fahrenheit" |
| min_value | DOUBLE | Minimum valid reading | 0 |
| max_value | DOUBLE | Maximum valid reading | 1000 |
| warning_low | DOUBLE | Low warning threshold | 200 |
| warning_high | DOUBLE | High warning threshold | 750 |
| critical_low | DOUBLE | Low critical threshold | 100 |
| critical_high | DOUBLE | High critical threshold | 850 |
| reading_interval_seconds | INT | How often it reports | 10 |
| manufacturer | STRING | Sensor manufacturer | "Emerson" |
| model | STRING | Sensor model | "Rosemount 3051" |
| installation_date | DATE | When installed | 2022-01-15 |
| calibration_date | DATE | Last calibrated | 2024-06-01 |
| is_active | BOOLEAN | Currently reporting | true |

**Sensor Types:**
- Temperature
- Pressure
- Flow Rate
- Level
- Vibration
- pH
- Density
- Viscosity

---

### 2. FACT TABLES (Transactional Data)

#### 2.1 fact_sensor_readings (STREAMING - Primary streaming dataset)
High-frequency IoT sensor data. **This is your main streaming table.**

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| reading_id | STRING | Unique event ID | UUID |
| sensor_id | STRING | FK to sensor | "SENS-000001" |
| unit_id | STRING | FK to processing unit | "UNIT-00001" |
| refinery_id | STRING | FK to refinery | "REF-001" |
| reading_timestamp | TIMESTAMP | When reading occurred | 2024-01-15T14:30:05.123Z |
| reading_value | DOUBLE | The sensor reading | 425.7 |
| reading_quality | STRING | Quality indicator | "GOOD" |
| is_interpolated | BOOLEAN | Was value interpolated | false |
| event_time | TIMESTAMP | Event timestamp | 2024-01-15T14:30:05.123Z |
| processing_time | TIMESTAMP | When ingested | 2024-01-15T14:30:05.500Z |

**Volume Calculation:**
- 500 refineries × 1000 sensors avg = 500,000 sensors
- Each sensor reports every 10 seconds = 8,640 readings/day/sensor
- 500,000 × 8,640 = 4.32 billion readings/day
- For demo: Generate 1 year of historical data + live stream
- ~35 bytes per row compressed = ~70GB for 2 billion rows

#### 2.2 fact_crude_shipments (BATCH)
Crude oil deliveries to refineries.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| shipment_id | STRING | Primary key | "SHIP-0000001" |
| refinery_id | STRING | Destination refinery | "REF-001" |
| crude_type_id | STRING | Type of crude | "CRUDE-001" |
| supplier_id | STRING | Crude supplier | "SUPP-001" |
| vessel_name | STRING | Ship/pipeline name | "MT Pacific Star" |
| transport_mode | STRING | How delivered | "Tanker" |
| origin_port | STRING | Where loaded | "Ras Tanura" |
| shipment_date | DATE | Departure date | 2024-01-10 |
| arrival_date | DATE | Arrival date | 2024-01-25 |
| expected_arrival_date | DATE | Originally scheduled | 2024-01-24 |
| volume_barrels | INT | Quantity delivered | 1500000 |
| api_gravity_actual | DOUBLE | Measured API gravity | 38.5 |
| sulfur_content_actual | DOUBLE | Measured sulfur % | 0.28 |
| water_content_pct | DOUBLE | Water contamination | 0.15 |
| price_per_barrel | DOUBLE | Purchase price | 78.50 |
| total_cost | DOUBLE | Total shipment cost | 117750000.00 |
| destination_tank_id | STRING | Where stored | "TANK-00001" |
| status | STRING | Shipment status | "DELIVERED" |
| created_at | TIMESTAMP | Record created | 2024-01-10T08:00:00Z |
| updated_at | TIMESTAMP | Last updated | 2024-01-25T14:30:00Z |

#### 2.3 fact_quality_tests (BATCH)
Laboratory quality test results.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| test_id | STRING | Primary key | "TEST-00000001" |
| refinery_id | STRING | Where tested | "REF-001" |
| sample_source_type | STRING | What was tested | "Crude Shipment" |
| sample_source_id | STRING | FK to source | "SHIP-0000001" |
| tank_id | STRING | If tank sample | "TANK-00001" |
| test_datetime | TIMESTAMP | When tested | 2024-01-25T16:00:00Z |
| test_type | STRING | Type of test | "Full Assay" |
| technician_id | STRING | Who performed | "TECH-001" |
| api_gravity | DOUBLE | API gravity result | 38.2 |
| sulfur_content_pct | DOUBLE | Sulfur percentage | 0.29 |
| water_sediment_pct | DOUBLE | BS&W percentage | 0.18 |
| viscosity_cst | DOUBLE | Viscosity at 40°C | 7.5 |
| pour_point_f | DOUBLE | Pour point °F | 15 |
| reid_vapor_pressure | DOUBLE | RVP psi | 8.2 |
| octane_number | DOUBLE | If gasoline | NULL |
| cetane_number | DOUBLE | If diesel | NULL |
| flash_point_f | DOUBLE | Flash point °F | 125 |
| test_result_status | STRING | Pass/Fail/Marginal | "PASS" |
| notes | STRING | Technician notes | "Within spec" |

#### 2.4 fact_tank_inventory (BATCH - frequent snapshots)
Tank level measurements taken every 15 minutes.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| inventory_id | STRING | Primary key | UUID |
| tank_id | STRING | FK to tank | "TANK-00001" |
| refinery_id | STRING | FK to refinery | "REF-001" |
| measurement_timestamp | TIMESTAMP | When measured | 2024-01-25T14:15:00Z |
| volume_barrels | INT | Current volume | 425000 |
| fill_percentage | DOUBLE | % of capacity | 0.85 |
| temperature_f | DOUBLE | Product temp | 95.5 |
| water_bottom_inches | DOUBLE | Water at bottom | 2.5 |
| product_type | STRING | What's stored | "Crude Oil" |
| crude_type_id | STRING | If crude | "CRUDE-001" |
| product_id | STRING | If product | NULL |
| measurement_method | STRING | How measured | "Radar Gauge" |
| is_verified | BOOLEAN | Manually verified | false |

#### 2.5 fact_production_output (BATCH)
Hourly production from processing units.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| output_id | STRING | Primary key | UUID |
| unit_id | STRING | FK to unit | "UNIT-00001" |
| refinery_id | STRING | FK to refinery | "REF-001" |
| production_hour | TIMESTAMP | Hour of production | 2024-01-25T14:00:00Z |
| input_volume_barrels | INT | Crude input | 6500 |
| input_crude_type_id | STRING | What crude processed | "CRUDE-001" |
| output_product_id | STRING | Product made | "PROD-001" |
| output_volume_barrels | INT | Product output | 2990 |
| yield_percentage | DOUBLE | Output/Input | 0.46 |
| energy_consumed_mmbtu | DOUBLE | Energy used | 1250.5 |
| operating_temperature_f | DOUBLE | Avg temp | 650.0 |
| operating_pressure_psi | DOUBLE | Avg pressure | 45.5 |
| downtime_minutes | INT | Minutes offline | 0 |
| quality_grade | STRING | Output quality | "On-Spec" |
| shift_id | STRING | Operating shift | "SHIFT-A" |

---

## Streaming Architecture Details

### Streaming Dataset: fact_sensor_readings

#### Why This Dataset for Streaming?
- High velocity: Sensors report every 10 seconds
- Time-sensitive: Anomalies need real-time detection
- Volume: Largest dataset by far
- Business value: Early warning prevents costly shutdowns

#### Streaming Implementation Options

**Option 1: Kafka + Structured Streaming (Recommended)**
```
[Data Generator] → [Kafka Topic] → [Spark Structured Streaming] → [Delta Lake]
```

**Option 2: Auto Loader with continuous file arrival**
```
[Data Generator] → [Cloud Storage] → [Auto Loader] → [Delta Lake]
```

**Option 3: Delta Live Tables with continuous mode**
```
[Data Generator] → [Kafka/Kinesis] → [DLT Pipeline] → [Delta Lake]
```

### Simulating Real-Time Data

For demonstration, you can simulate streaming by:

1. **Generate historical batch data** (1 year of history)
2. **Create a streaming simulator** that reads historical patterns and generates new events in real-time
3. **Write to Kafka topic** or drop files continuously to a landing zone

---

## Data Generation Code (dbldatagen)

### Installation
```python
%pip install dbldatagen
```

### Complete Generation Script

```python
# Databricks notebook: 01_generate_data

import dbldatagen as dg
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

# Configuration
NUM_REFINERIES = 500
SENSORS_PER_REFINERY = 1000
DAYS_OF_HISTORY = 365
OUTPUT_PATH = "/mnt/data/crude_oil_pipeline"

# ============================================================
# DIMENSION TABLES
# ============================================================

# --- dim_refineries ---
def generate_refineries(spark, num_refineries=500):
    cities = [
        ("Houston", "TX", "USA", 29.76, -95.37),
        ("Beaumont", "TX", "USA", 30.08, -94.10),
        ("Lake Charles", "LA", "USA", 30.21, -93.22),
        ("Baton Rouge", "LA", "USA", 30.45, -91.15),
        ("New Orleans", "LA", "USA", 29.95, -90.07),
        ("Port Arthur", "TX", "USA", 29.90, -93.93),
        ("Corpus Christi", "TX", "USA", 27.80, -97.40),
        ("Philadelphia", "PA", "USA", 39.95, -75.17),
        ("Los Angeles", "CA", "USA", 34.05, -118.24),
        ("San Francisco", "CA", "USA", 37.77, -122.42),
        ("Chicago", "IL", "USA", 41.88, -87.63),
        ("Detroit", "MI", "USA", 42.33, -83.05),
        ("Rotterdam", "ZH", "NLD", 51.92, 4.48),
        ("Singapore", "SG", "SGP", 1.35, 103.82),
        ("Jamnagar", "GJ", "IND", 22.47, 70.07),
    ]
    
    operators = ["PetroCorp", "GlobalRefining", "CoastalEnergy", "TransOil", 
                 "PacificPetroleum", "AtlanticFuels", "MidwestRefinery", "SunsetOil"]
    
    refinery_spec = (
        dg.DataGenerator(spark, name="refineries", rows=num_refineries, partitions=4)
        .withColumn("refinery_id", "string", expr="concat('REF-', lpad(cast(id as string), 3, '0'))")
        .withColumn("city_idx", "int", minValue=0, maxValue=len(cities)-1, random=True)
        .withColumn("refinery_name", "string", 
                   expr=f"concat(element_at(array{tuple([c[0] for c in cities])}, city_idx+1), ' Refinery ', chr(65 + (id % 26)))")
        .withColumn("location_city", "string", 
                   expr=f"element_at(array{tuple([c[0] for c in cities])}, city_idx+1)")
        .withColumn("location_state", "string",
                   expr=f"element_at(array{tuple([c[1] for c in cities])}, city_idx+1)")
        .withColumn("location_country", "string",
                   expr=f"element_at(array{tuple([c[2] for c in cities])}, city_idx+1)")
        .withColumn("latitude", "double",
                   expr=f"element_at(array{tuple([c[3] for c in cities])}, city_idx+1) + (rand() - 0.5) * 0.5")
        .withColumn("longitude", "double",
                   expr=f"element_at(array{tuple([c[4] for c in cities])}, city_idx+1) + (rand() - 0.5) * 0.5")
        .withColumn("capacity_bpd", "int", minValue=50000, maxValue=700000, step=10000, random=True)
        .withColumn("complexity_index", "double", minValue=5.0, maxValue=15.0, random=True)
        .withColumn("commissioned_date", "date", 
                   begin="1960-01-01", end="2020-12-31", random=True)
        .withColumn("operator_name", "string", values=operators, random=True)
        .withColumn("is_active", "boolean", expr="rand() > 0.05")  # 95% active
        .withColumn("last_turnaround_date", "date",
                   begin="2020-01-01", end="2024-06-30", random=True)
    )
    
    df = refinery_spec.build()
    return df.drop("city_idx", "id")


# --- dim_processing_units ---
def generate_processing_units(spark, refineries_df, avg_units_per_refinery=10):
    unit_types = [
        ("Crude Distillation", "Atmospheric", 0.95),
        ("Crude Distillation", "Vacuum", 0.93),
        ("Fluid Catalytic Cracker", "Resid FCC", 0.90),
        ("Hydrocracker", "Mild", 0.92),
        ("Hydrocracker", "Full Conversion", 0.88),
        ("Reformer", "Continuous", 0.91),
        ("Reformer", "Semi-Regenerative", 0.89),
        ("Alkylation", "HF Process", 0.94),
        ("Alkylation", "Sulfuric Acid", 0.93),
        ("Coker", "Delayed", 0.87),
        ("Coker", "Fluid", 0.85),
        ("Hydrotreater", "Naphtha", 0.96),
        ("Hydrotreater", "Diesel", 0.95),
        ("Sulfur Recovery", "Claus", 0.98),
    ]
    
    manufacturers = ["Honeywell UOP", "Axens", "Shell Global", "ExxonMobil", 
                    "Chevron Lummus", "KBR", "CB&I", "Technip"]
    
    refinery_ids = [row.refinery_id for row in refineries_df.select("refinery_id").collect()]
    total_units = len(refinery_ids) * avg_units_per_refinery
    
    unit_spec = (
        dg.DataGenerator(spark, name="processing_units", rows=total_units, partitions=8)
        .withColumn("unit_id", "string", expr="concat('UNIT-', lpad(cast(id as string), 5, '0'))")
        .withColumn("refinery_idx", "int", minValue=0, maxValue=len(refinery_ids)-1, random=True)
        .withColumn("refinery_id", "string", 
                   expr=f"element_at(array{tuple(refinery_ids)}, refinery_idx+1)")
        .withColumn("unit_type_idx", "int", minValue=0, maxValue=len(unit_types)-1, random=True)
        .withColumn("unit_type", "string",
                   expr=f"element_at(array{tuple([u[0] for u in unit_types])}, unit_type_idx+1)")
        .withColumn("unit_subtype", "string",
                   expr=f"element_at(array{tuple([u[1] for u in unit_types])}, unit_type_idx+1)")
        .withColumn("design_efficiency", "double",
                   expr=f"element_at(array{tuple([u[2] for u in unit_types])}, unit_type_idx+1)")
        .withColumn("unit_name", "string",
                   expr="concat(substring(unit_type, 1, 3), '-', (id % 10) + 1)")
        .withColumn("capacity_bpd", "int", minValue=10000, maxValue=200000, step=5000, random=True)
        .withColumn("installation_date", "date", begin="1980-01-01", end="2023-12-31", random=True)
        .withColumn("last_maintenance_date", "date", begin="2023-01-01", end="2024-06-30", random=True)
        .withColumn("maintenance_interval_days", "int", values=[90, 180, 365], random=True)
        .withColumn("manufacturer", "string", values=manufacturers, random=True)
        .withColumn("model_number", "string", 
                   expr="concat(substring(manufacturer, 1, 2), '-', cast(floor(rand()*9000+1000) as int))")
        .withColumn("is_active", "boolean", expr="rand() > 0.03")
    )
    
    df = unit_spec.build()
    return df.drop("refinery_idx", "unit_type_idx", "id")


# --- dim_sensors ---
def generate_sensors(spark, units_df, avg_sensors_per_unit=100):
    sensor_types = [
        ("Temperature", "Fahrenheit", 0, 1200, 200, 900, 100, 1000),
        ("Temperature", "Celsius", -50, 600, 50, 500, 0, 550),
        ("Pressure", "PSI", 0, 5000, 50, 3000, 10, 4000),
        ("Pressure", "Bar", 0, 350, 5, 200, 1, 280),
        ("Flow Rate", "BPH", 0, 50000, 1000, 40000, 500, 45000),
        ("Flow Rate", "GPM", 0, 100000, 2000, 80000, 1000, 90000),
        ("Level", "Percent", 0, 100, 10, 95, 5, 98),
        ("Level", "Feet", 0, 50, 5, 45, 2, 48),
        ("Vibration", "mm/s", 0, 50, 0, 20, 0, 35),
        ("Vibration", "mils", 0, 10, 0, 5, 0, 8),
        ("pH", "pH", 0, 14, 4, 10, 2, 12),
        ("Density", "API", 0, 80, 20, 50, 10, 60),
        ("Viscosity", "cSt", 0, 1000, 1, 500, 0.5, 800),
    ]
    
    manufacturers = ["Emerson", "Honeywell", "Siemens", "ABB", "Yokogawa", "Endress+Hauser"]
    
    unit_ids = [row.unit_id for row in units_df.select("unit_id").collect()]
    unit_refinery_map = {row.unit_id: row.refinery_id 
                        for row in units_df.select("unit_id", "refinery_id").collect()}
    
    total_sensors = len(unit_ids) * avg_sensors_per_unit
    
    sensor_spec = (
        dg.DataGenerator(spark, name="sensors", rows=total_sensors, partitions=16)
        .withColumn("sensor_id", "string", expr="concat('SENS-', lpad(cast(id as string), 6, '0'))")
        .withColumn("unit_idx", "int", minValue=0, maxValue=len(unit_ids)-1, random=True)
        .withColumn("unit_id", "string",
                   expr=f"element_at(array{tuple(unit_ids)}, unit_idx+1)")
        .withColumn("sensor_type_idx", "int", minValue=0, maxValue=len(sensor_types)-1, random=True)
        .withColumn("sensor_type", "string",
                   expr=f"element_at(array{tuple([s[0] for s in sensor_types])}, sensor_type_idx+1)")
        .withColumn("measurement_unit", "string",
                   expr=f"element_at(array{tuple([s[1] for s in sensor_types])}, sensor_type_idx+1)")
        .withColumn("min_value", "double",
                   expr=f"element_at(array{tuple([float(s[2]) for s in sensor_types])}, sensor_type_idx+1)")
        .withColumn("max_value", "double",
                   expr=f"element_at(array{tuple([float(s[3]) for s in sensor_types])}, sensor_type_idx+1)")
        .withColumn("warning_low", "double",
                   expr=f"element_at(array{tuple([float(s[4]) for s in sensor_types])}, sensor_type_idx+1)")
        .withColumn("warning_high", "double",
                   expr=f"element_at(array{tuple([float(s[5]) for s in sensor_types])}, sensor_type_idx+1)")
        .withColumn("critical_low", "double",
                   expr=f"element_at(array{tuple([float(s[6]) for s in sensor_types])}, sensor_type_idx+1)")
        .withColumn("critical_high", "double",
                   expr=f"element_at(array{tuple([float(s[7]) for s in sensor_types])}, sensor_type_idx+1)")
        .withColumn("sensor_name", "string",
                   expr="concat(substring(sensor_type, 1, 4), '-', lpad(cast((id % 1000) as string), 3, '0'))")
        .withColumn("reading_interval_seconds", "int", values=[5, 10, 30, 60], weights=[0.1, 0.5, 0.3, 0.1], random=True)
        .withColumn("manufacturer", "string", values=manufacturers, random=True)
        .withColumn("model", "string", expr="concat(substring(manufacturer, 1, 3), '-', cast(floor(rand()*900+100) as int))")
        .withColumn("installation_date", "date", begin="2015-01-01", end="2024-01-01", random=True)
        .withColumn("calibration_date", "date", begin="2023-06-01", end="2024-06-30", random=True)
        .withColumn("is_active", "boolean", expr="rand() > 0.02")
    )
    
    df = sensor_spec.build()
    
    # Add refinery_id via join (more efficient than lookup)
    df = df.join(units_df.select("unit_id", "refinery_id"), "unit_id", "left")
    
    return df.drop("unit_idx", "sensor_type_idx", "id")


# --- dim_crude_types ---
def generate_crude_types(spark):
    crude_data = [
        ("CRUDE-001", "West Texas Intermediate", "WTI", "USA", "Permian Basin", 39.6, 0.24, "Light", "Sweet", -3.50),
        ("CRUDE-002", "Brent", "BRENT", "UK", "North Sea", 38.3, 0.37, "Light", "Sweet", 0.00),
        ("CRUDE-003", "Dubai", "DUBAI", "UAE", "Persian Gulf", 31.0, 2.00, "Medium", "Sour", -2.00),
        ("CRUDE-004", "Arab Light", "AL", "SAU", "Ghawar", 33.0, 1.77, "Light", "Sour", -1.50),
        ("CRUDE-005", "Arab Heavy", "AH", "SAU", "Safaniya", 27.0, 2.80, "Heavy", "Sour", -5.00),
        ("CRUDE-006", "Bonny Light", "BONNY", "NGA", "Niger Delta", 33.4, 0.16, "Light", "Sweet", 2.00),
        ("CRUDE-007", "Mars", "MARS", "USA", "Gulf of Mexico", 31.0, 1.93, "Medium", "Sour", -4.00),
        ("CRUDE-008", "Maya", "MAYA", "MEX", "Campeche", 22.0, 3.30, "Heavy", "Sour", -10.00),
        ("CRUDE-009", "Urals", "URALS", "RUS", "Western Siberia", 31.7, 1.35, "Medium", "Sour", -15.00),
        ("CRUDE-010", "Tapis", "TAPIS", "MYS", "Malay Basin", 44.3, 0.04, "Light", "Sweet", 5.00),
        ("CRUDE-011", "Saharan Blend", "SAHARAN", "DZA", "Hassi Messaoud", 45.5, 0.09, "Light", "Sweet", 3.00),
        ("CRUDE-012", "Forcados", "FORCADOS", "NGA", "Niger Delta", 29.6, 0.18, "Light", "Sweet", 1.50),
        ("CRUDE-013", "Ekofisk", "EKO", "NOR", "North Sea", 43.4, 0.17, "Light", "Sweet", 1.00),
        ("CRUDE-014", "Louisiana Light", "LLS", "USA", "Gulf Coast", 36.0, 0.40, "Light", "Sweet", -1.00),
        ("CRUDE-015", "Heavy Louisiana Sweet", "HLS", "USA", "Gulf Coast", 32.5, 0.30, "Medium", "Sweet", -2.50),
    ]
    
    schema = StructType([
        StructField("crude_type_id", StringType()),
        StructField("crude_name", StringType()),
        StructField("crude_abbreviation", StringType()),
        StructField("origin_country", StringType()),
        StructField("origin_region", StringType()),
        StructField("api_gravity_typical", DoubleType()),
        StructField("sulfur_content_typical", DoubleType()),
        StructField("classification", StringType()),
        StructField("sweetness", StringType()),
        StructField("benchmark_price_differential", DoubleType()),
    ])
    
    return spark.createDataFrame(crude_data, schema)


# --- dim_products ---
def generate_products(spark):
    product_data = [
        ("PROD-001", "Regular Gasoline", "Gasoline", "87 Octane", 720, 775, 10, 0.25),
        ("PROD-002", "Premium Gasoline", "Gasoline", "91 Octane", 720, 770, 10, 0.15),
        ("PROD-003", "Super Premium Gasoline", "Gasoline", "93 Octane", 715, 765, 10, 0.10),
        ("PROD-004", "Ultra Low Sulfur Diesel", "Diesel", "ULSD", 820, 860, 15, 0.28),
        ("PROD-005", "Heating Oil", "Distillate", "No. 2", 830, 870, 500, 0.08),
        ("PROD-006", "Jet Fuel A", "Jet Fuel", "Jet A", 775, 840, 3000, 0.08),
        ("PROD-007", "Kerosene", "Distillate", "K-1", 780, 830, 40, 0.03),
        ("PROD-008", "Naphtha", "Feedstock", "Light", 650, 720, 500, 0.05),
        ("PROD-009", "LPG", "Gas", "Propane/Butane", 500, 600, 50, 0.04),
        ("PROD-010", "Residual Fuel Oil", "Residual", "No. 6", 950, 1010, 35000, 0.03),
        ("PROD-011", "Asphalt", "Residual", "Paving Grade", 1010, 1060, 50000, 0.02),
        ("PROD-012", "Petroleum Coke", "Solid", "Fuel Grade", 1400, 1600, 80000, 0.04),
        ("PROD-013", "Lubricant Base Oil", "Lubricant", "Group II", 850, 890, 10, 0.02),
        ("PROD-014", "Sulfur", "Byproduct", "Eleite", 1800, 2100, 0, 0.015),
    ]
    
    schema = StructType([
        StructField("product_id", StringType()),
        StructField("product_name", StringType()),
        StructField("product_category", StringType()),
        StructField("product_grade", StringType()),
        StructField("density_min", DoubleType()),
        StructField("density_max", DoubleType()),
        StructField("sulfur_max_ppm", IntegerType()),
        StructField("typical_yield_pct", DoubleType()),
    ])
    
    df = spark.createDataFrame(product_data, schema)
    return df.withColumn("unit_of_measure", F.lit("barrels"))


# --- dim_storage_tanks ---
def generate_storage_tanks(spark, refineries_df, avg_tanks_per_refinery=4):
    tank_types = ["Floating Roof", "Fixed Roof", "Spherical", "Horizontal"]
    product_types = ["Crude Oil", "Gasoline", "Diesel", "Jet Fuel", "Residual"]
    
    refinery_ids = [row.refinery_id for row in refineries_df.select("refinery_id").collect()]
    total_tanks = len(refinery_ids) * avg_tanks_per_refinery
    
    tank_spec = (
        dg.DataGenerator(spark, name="storage_tanks", rows=total_tanks, partitions=4)
        .withColumn("tank_id", "string", expr="concat('TANK-', lpad(cast(id as string), 5, '0'))")
        .withColumn("refinery_idx", "int", minValue=0, maxValue=len(refinery_ids)-1, random=True)
        .withColumn("refinery_id", "string",
                   expr=f"element_at(array{tuple(refinery_ids)}, refinery_idx+1)")
        .withColumn("tank_name", "string", expr="concat('TK-', lpad(cast((id % 500) + 100 as string), 3, '0'))")
        .withColumn("tank_type", "string", values=tank_types, weights=[0.4, 0.3, 0.15, 0.15], random=True)
        .withColumn("product_type", "string", values=product_types, weights=[0.3, 0.25, 0.2, 0.15, 0.1], random=True)
        .withColumn("capacity_barrels", "int", values=[100000, 250000, 500000, 750000, 1000000], 
                   weights=[0.2, 0.3, 0.25, 0.15, 0.1], random=True)
        .withColumn("min_operating_level", "double", minValue=0.08, maxValue=0.15, random=True)
        .withColumn("max_operating_level", "double", minValue=0.90, maxValue=0.98, random=True)
        .withColumn("diameter_feet", "double", minValue=100, maxValue=350, random=True)
        .withColumn("height_feet", "double", minValue=40, maxValue=65, random=True)
        .withColumn("material", "string", values=["Carbon Steel", "Stainless Steel"], weights=[0.8, 0.2], random=True)
        .withColumn("has_heating", "boolean", expr="rand() > 0.6")
        .withColumn("has_mixer", "boolean", expr="rand() > 0.5")
        .withColumn("last_inspection_date", "date", begin="2022-01-01", end="2024-06-30", random=True)
        .withColumn("is_active", "boolean", expr="rand() > 0.05")
    )
    
    df = tank_spec.build()
    return df.drop("refinery_idx", "id")


# ============================================================
# FACT TABLES
# ============================================================

# --- fact_sensor_readings (STREAMING) ---
def generate_sensor_readings_batch(spark, sensors_df, start_date, end_date, sample_rate=0.01):
    """
    Generate historical sensor readings.
    sample_rate: fraction of sensors × time intervals to generate (0.01 = 1%)
    For full data, use sample_rate=1.0 but expect ~70GB+
    """
    from pyspark.sql import functions as F
    
    # Get sensor metadata
    sensor_data = sensors_df.select(
        "sensor_id", "unit_id", "refinery_id", "sensor_type",
        "min_value", "max_value", "reading_interval_seconds"
    )
    
    # Calculate time range
    days = (end_date - start_date).days
    
    # Generate readings
    readings_spec = (
        dg.DataGenerator(spark, name="sensor_readings", 
                        rows=int(500000 * 8640 * days * sample_rate),  # Adjust based on sample_rate
                        partitions=64)
        .withColumn("reading_id", "string", expr="uuid()")
        .withColumn("sensor_idx", "int", minValue=0, maxValue=499999, random=True)  # 500K sensors
        .withColumn("day_offset", "int", minValue=0, maxValue=days-1, random=True)
        .withColumn("second_of_day", "int", minValue=0, maxValue=86399, random=True)
        .withColumn("base_value", "double", minValue=0.3, maxValue=0.7, random=True)  # Normalized
        .withColumn("noise", "double", minValue=-0.05, maxValue=0.05, random=True)
        .withColumn("anomaly_flag", "double", expr="rand()")  # For introducing anomalies
        .withColumn("reading_quality", "string", 
                   values=["GOOD", "UNCERTAIN", "BAD"], 
                   weights=[0.97, 0.025, 0.005], random=True)
        .withColumn("is_interpolated", "boolean", expr="rand() > 0.99")
    )
    
    df = readings_spec.build()
    
    # Join with sensor metadata to get proper ranges
    df = df.join(
        F.broadcast(sensor_data.withColumn("sensor_idx_join", 
            F.expr("cast(substring(sensor_id, 6) as int)"))),
        df.sensor_idx == F.col("sensor_idx_join"),
        "inner"
    )
    
    # Calculate actual reading value based on sensor's valid range
    df = df.withColumn(
        "reading_value",
        F.when(F.col("anomaly_flag") > 0.995,  # 0.5% anomalies
               F.col("min_value") + (F.col("base_value") + F.col("noise") + 0.3) * (F.col("max_value") - F.col("min_value")))
        .otherwise(
            F.col("min_value") + (F.col("base_value") + F.col("noise")) * (F.col("max_value") - F.col("min_value"))
        )
    )
    
    # Calculate timestamps
    start_timestamp = int(datetime.combine(start_date, datetime.min.time()).timestamp())
    df = df.withColumn(
        "reading_timestamp",
        F.from_unixtime(F.lit(start_timestamp) + F.col("day_offset") * 86400 + F.col("second_of_day"))
    ).withColumn(
        "event_time", F.col("reading_timestamp")
    ).withColumn(
        "processing_time", 
        F.from_unixtime(F.unix_timestamp(F.col("reading_timestamp")) + F.lit(1))
    )
    
    # Select final columns
    return df.select(
        "reading_id", "sensor_id", "unit_id", "refinery_id",
        "reading_timestamp", "reading_value", "reading_quality",
        "is_interpolated", "event_time", "processing_time"
    )


# --- fact_crude_shipments ---
def generate_crude_shipments(spark, refineries_df, crude_types_df, tanks_df, num_shipments=10000000):
    refinery_ids = [row.refinery_id for row in refineries_df.select("refinery_id").collect()]
    crude_ids = [row.crude_type_id for row in crude_types_df.select("crude_type_id").collect()]
    
    transport_modes = ["Tanker", "Pipeline", "Barge", "Rail"]
    ports = ["Ras Tanura", "Houston", "Rotterdam", "Singapore", "Fujairah", 
             "Corpus Christi", "Louisiana Offshore", "Bonny", "Primorsk"]
    statuses = ["SCHEDULED", "IN_TRANSIT", "DELIVERED", "DELAYED"]
    suppliers = [f"SUPP-{str(i).zfill(3)}" for i in range(1, 51)]
    vessels = [f"MT {name}" for name in ["Pacific Star", "Atlantic Grace", "Gulf Spirit", 
               "Ocean Pride", "Coastal Dawn", "Marine King", "Sea Fortune", "Tanker Alpha"]]
    
    shipment_spec = (
        dg.DataGenerator(spark, name="crude_shipments", rows=num_shipments, partitions=32)
        .withColumn("shipment_id", "string", expr="concat('SHIP-', lpad(cast(id as string), 7, '0'))")
        .withColumn("refinery_id", "string", values=refinery_ids, random=True)
        .withColumn("crude_type_id", "string", values=crude_ids, random=True)
        .withColumn("supplier_id", "string", values=suppliers, random=True)
        .withColumn("vessel_name", "string", values=vessels, random=True)
        .withColumn("transport_mode", "string", values=transport_modes, 
                   weights=[0.5, 0.3, 0.15, 0.05], random=True)
        .withColumn("origin_port", "string", values=ports, random=True)
        .withColumn("shipment_date", "date", begin="2020-01-01", end="2024-12-31", random=True)
        .withColumn("transit_days", "int", minValue=5, maxValue=45, random=True)
        .withColumn("volume_barrels", "int", minValue=100000, maxValue=2500000, step=50000, random=True)
        .withColumn("api_gravity_actual", "double", minValue=20.0, maxValue=50.0, random=True)
        .withColumn("sulfur_content_actual", "double", minValue=0.05, maxValue=4.0, random=True)
        .withColumn("water_content_pct", "double", minValue=0.0, maxValue=1.0, random=True)
        .withColumn("price_per_barrel", "double", minValue=40.0, maxValue=120.0, random=True)
        .withColumn("status", "string", values=statuses, weights=[0.05, 0.1, 0.8, 0.05], random=True)
    )
    
    df = shipment_spec.build()
    
    # Calculate derived columns
    df = df.withColumn("arrival_date", F.date_add("shipment_date", F.col("transit_days")))
    df = df.withColumn("expected_arrival_date", 
                       F.date_add("shipment_date", F.col("transit_days") - F.floor(F.rand() * 3).cast("int")))
    df = df.withColumn("total_cost", F.col("volume_barrels") * F.col("price_per_barrel"))
    df = df.withColumn("created_at", F.to_timestamp("shipment_date"))
    df = df.withColumn("updated_at", F.to_timestamp("arrival_date"))
    
    # Add destination tank (join with tanks)
    tank_ids = [row.tank_id for row in tanks_df.filter(F.col("product_type") == "Crude Oil")
                .select("tank_id").collect()]
    df = df.withColumn("destination_tank_id", 
                       F.element_at(F.array([F.lit(t) for t in tank_ids[:100]]), 
                                   (F.floor(F.rand() * len(tank_ids[:100])) + 1).cast("int")))
    
    return df.drop("transit_days", "id")


# --- fact_quality_tests ---
def generate_quality_tests(spark, refineries_df, shipments_df, tanks_df, num_tests=15000000):
    refinery_ids = [row.refinery_id for row in refineries_df.select("refinery_id").collect()]
    
    test_types = ["Full Assay", "Spot Check", "Blend Verification", "Product Certification"]
    source_types = ["Crude Shipment", "Tank Sample", "Process Stream", "Product Output"]
    statuses = ["PASS", "FAIL", "MARGINAL", "PENDING"]
    technicians = [f"TECH-{str(i).zfill(3)}" for i in range(1, 101)]
    
    test_spec = (
        dg.DataGenerator(spark, name="quality_tests", rows=num_tests, partitions=32)
        .withColumn("test_id", "string", expr="concat('TEST-', lpad(cast(id as string), 8, '0'))")
        .withColumn("refinery_id", "string", values=refinery_ids, random=True)
        .withColumn("sample_source_type", "string", values=source_types, 
                   weights=[0.3, 0.4, 0.2, 0.1], random=True)
        .withColumn("test_datetime", "timestamp", begin="2020-01-01", end="2024-12-31", random=True)
        .withColumn("test_type", "string", values=test_types, 
                   weights=[0.2, 0.5, 0.2, 0.1], random=True)
        .withColumn("technician_id", "string", values=technicians, random=True)
        .withColumn("api_gravity", "double", minValue=18.0, maxValue=55.0, random=True)
        .withColumn("sulfur_content_pct", "double", minValue=0.01, maxValue=5.0, random=True)
        .withColumn("water_sediment_pct", "double", minValue=0.0, maxValue=2.0, random=True)
        .withColumn("viscosity_cst", "double", minValue=1.0, maxValue=500.0, random=True)
        .withColumn("pour_point_f", "double", minValue=-40.0, maxValue=60.0, random=True)
        .withColumn("reid_vapor_pressure", "double", minValue=5.0, maxValue=15.0, random=True)
        .withColumn("flash_point_f", "double", minValue=100.0, maxValue=250.0, random=True)
        .withColumn("test_result_status", "string", values=statuses, 
                   weights=[0.85, 0.05, 0.08, 0.02], random=True)
        .withColumn("notes", "string", values=["Within spec", "Minor variance", "Requires retest", 
                                               "Approved", "See lab report"], random=True)
    )
    
    df = test_spec.build()
    
    # Add nullable fields
    df = df.withColumn("octane_number", 
                       F.when(F.rand() > 0.7, F.lit(None)).otherwise(F.rand() * 10 + 85))
    df = df.withColumn("cetane_number",
                       F.when(F.rand() > 0.7, F.lit(None)).otherwise(F.rand() * 15 + 40))
    df = df.withColumn("sample_source_id", F.expr("concat('SRC-', lpad(cast(floor(rand()*1000000) as string), 7, '0'))"))
    df = df.withColumn("tank_id", 
                       F.when(F.col("sample_source_type") == "Tank Sample",
                             F.expr("concat('TANK-', lpad(cast(floor(rand()*2000) as string), 5, '0'))"))
                       .otherwise(F.lit(None)))
    
    return df.drop("id")


# --- fact_tank_inventory ---
def generate_tank_inventory(spark, tanks_df, days_of_data=365):
    tank_ids = [row.tank_id for row in tanks_df.select("tank_id").collect()]
    tank_refineries = {row.tank_id: row.refinery_id 
                      for row in tanks_df.select("tank_id", "refinery_id").collect()}
    tank_products = {row.tank_id: row.product_type 
                    for row in tanks_df.select("tank_id", "product_type").collect()}
    tank_capacities = {row.tank_id: row.capacity_barrels 
                      for row in tanks_df.select("tank_id", "capacity_barrels").collect()}
    
    # 96 readings per day (every 15 min) × days × tanks
    readings_per_day = 96
    total_readings = len(tank_ids) * readings_per_day * days_of_data
    
    inv_spec = (
        dg.DataGenerator(spark, name="tank_inventory", rows=total_readings, partitions=32)
        .withColumn("inventory_id", "string", expr="uuid()")
        .withColumn("tank_idx", "int", minValue=0, maxValue=len(tank_ids)-1, random=True)
        .withColumn("tank_id", "string",
                   expr=f"element_at(array{tuple(tank_ids)}, tank_idx+1)")
        .withColumn("day_offset", "int", minValue=0, maxValue=days_of_data-1, random=True)
        .withColumn("time_slot", "int", minValue=0, maxValue=95, random=True)  # 0-95 for 15-min slots
        .withColumn("fill_percentage", "double", minValue=0.15, maxValue=0.92, random=True)
        .withColumn("temperature_f", "double", minValue=60.0, maxValue=120.0, random=True)
        .withColumn("water_bottom_inches", "double", minValue=0.0, maxValue=12.0, random=True)
        .withColumn("measurement_method", "string", 
                   values=["Radar Gauge", "Manual Dip", "Float Gauge", "Servo Gauge"],
                   weights=[0.6, 0.1, 0.15, 0.15], random=True)
        .withColumn("is_verified", "boolean", expr="rand() > 0.95")
    )
    
    df = inv_spec.build()
    
    # Calculate timestamp
    start_date = datetime(2024, 1, 1)
    start_ts = int(start_date.timestamp())
    df = df.withColumn(
        "measurement_timestamp",
        F.from_unixtime(F.lit(start_ts) + F.col("day_offset") * 86400 + F.col("time_slot") * 900)
    )
    
    # Add refinery_id and product info via lookup (simplified)
    # In real implementation, join with tanks_df
    df = df.join(F.broadcast(tanks_df.select("tank_id", "refinery_id", "product_type", "capacity_barrels")), 
                "tank_id", "left")
    
    df = df.withColumn("volume_barrels", 
                       (F.col("fill_percentage") * F.col("capacity_barrels")).cast("int"))
    
    # Add crude_type_id and product_id based on product_type
    df = df.withColumn("crude_type_id",
                       F.when(F.col("product_type") == "Crude Oil",
                             F.expr("concat('CRUDE-', lpad(cast(floor(rand()*15)+1 as string), 3, '0'))"))
                       .otherwise(F.lit(None)))
    df = df.withColumn("product_id",
                       F.when(F.col("product_type") != "Crude Oil",
                             F.expr("concat('PROD-', lpad(cast(floor(rand()*14)+1 as string), 3, '0'))"))
                       .otherwise(F.lit(None)))
    
    return df.select(
        "inventory_id", "tank_id", "refinery_id", "measurement_timestamp",
        "volume_barrels", "fill_percentage", "temperature_f", "water_bottom_inches",
        "product_type", "crude_type_id", "product_id", "measurement_method", "is_verified"
    )


# --- fact_production_output ---
def generate_production_output(spark, units_df, products_df, crude_types_df, days_of_data=365):
    unit_ids = [row.unit_id for row in units_df.select("unit_id").collect()]
    product_ids = [row.product_id for row in products_df.select("product_id").collect()]
    crude_ids = [row.crude_type_id for row in crude_types_df.select("crude_type_id").collect()]
    
    # 24 hours × days × units
    hours_of_data = 24 * days_of_data
    total_records = len(unit_ids) * hours_of_data // 10  # Sample 10%
    
    output_spec = (
        dg.DataGenerator(spark, name="production_output", rows=total_records, partitions=32)
        .withColumn("output_id", "string", expr="uuid()")
        .withColumn("unit_idx", "int", minValue=0, maxValue=len(unit_ids)-1, random=True)
        .withColumn("unit_id", "string",
                   expr=f"element_at(array{tuple(unit_ids[:1000])}, (unit_idx % 1000) + 1)")  # Limit for expr
        .withColumn("day_offset", "int", minValue=0, maxValue=days_of_data-1, random=True)
        .withColumn("hour", "int", minValue=0, maxValue=23, random=True)
        .withColumn("input_volume_barrels", "int", minValue=3000, maxValue=10000, step=100, random=True)
        .withColumn("input_crude_type_id", "string", values=crude_ids, random=True)
        .withColumn("output_product_id", "string", values=product_ids, random=True)
        .withColumn("yield_percentage", "double", minValue=0.75, maxValue=0.98, random=True)
        .withColumn("energy_consumed_mmbtu", "double", minValue=500.0, maxValue=2500.0, random=True)
        .withColumn("operating_temperature_f", "double", minValue=400.0, maxValue=900.0, random=True)
        .withColumn("operating_pressure_psi", "double", minValue=20.0, maxValue=100.0, random=True)
        .withColumn("downtime_minutes", "int", values=[0, 0, 0, 0, 0, 15, 30, 60], random=True)
        .withColumn("quality_grade", "string", 
                   values=["On-Spec", "Off-Spec", "Premium"], 
                   weights=[0.85, 0.10, 0.05], random=True)
        .withColumn("shift_id", "string", values=["SHIFT-A", "SHIFT-B", "SHIFT-C"], random=True)
    )
    
    df = output_spec.build()
    
    # Calculate production hour timestamp
    start_date = datetime(2024, 1, 1)
    start_ts = int(start_date.timestamp())
    df = df.withColumn(
        "production_hour",
        F.from_unixtime(F.lit(start_ts) + F.col("day_offset") * 86400 + F.col("hour") * 3600)
    )
    
    # Calculate output volume
    df = df.withColumn("output_volume_barrels",
                       (F.col("input_volume_barrels") * F.col("yield_percentage")).cast("int"))
    
    # Add refinery_id via join
    df = df.join(F.broadcast(units_df.select("unit_id", "refinery_id")), "unit_id", "left")
    
    return df.select(
        "output_id", "unit_id", "refinery_id", "production_hour",
        "input_volume_barrels", "input_crude_type_id", "output_product_id",
        "output_volume_barrels", "yield_percentage", "energy_consumed_mmbtu",
        "operating_temperature_f", "operating_pressure_psi", "downtime_minutes",
        "quality_grade", "shift_id"
    )


# ============================================================
# MAIN EXECUTION
# ============================================================

def generate_all_data(spark, output_path, sample_rate=0.1):
    """
    Main function to generate all datasets.
    sample_rate: Controls data volume (0.1 = ~10GB, 1.0 = ~100GB)
    """
    from pyspark.sql import functions as F
    from datetime import date
    
    print("=" * 60)
    print("CRUDE OIL REFINERY DATA GENERATION")
    print("=" * 60)
    
    # 1. Generate dimension tables
    print("\n[1/11] Generating dim_refineries...")
    refineries = generate_refineries(spark, 500)
    refineries.write.mode("overwrite").parquet(f"{output_path}/bronze/dim_refineries")
    print(f"       Generated {refineries.count()} refineries")
    
    print("\n[2/11] Generating dim_crude_types...")
    crude_types = generate_crude_types(spark)
    crude_types.write.mode("overwrite").parquet(f"{output_path}/bronze/dim_crude_types")
    print(f"       Generated {crude_types.count()} crude types")
    
    print("\n[3/11] Generating dim_products...")
    products = generate_products(spark)
    products.write.mode("overwrite").parquet(f"{output_path}/bronze/dim_products")
    print(f"       Generated {products.count()} products")
    
    print("\n[4/11] Generating dim_processing_units...")
    units = generate_processing_units(spark, refineries, 10)
    units.write.mode("overwrite").parquet(f"{output_path}/bronze/dim_processing_units")
    print(f"       Generated {units.count()} processing units")
    
    print("\n[5/11] Generating dim_storage_tanks...")
    tanks = generate_storage_tanks(spark, refineries, 4)
    tanks.write.mode("overwrite").parquet(f"{output_path}/bronze/dim_storage_tanks")
    print(f"       Generated {tanks.count()} tanks")
    
    print("\n[6/11] Generating dim_sensors...")
    sensors = generate_sensors(spark, units, 100)
    sensors.write.mode("overwrite").parquet(f"{output_path}/bronze/dim_sensors")
    print(f"       Generated {sensors.count()} sensors")
    
    # 2. Generate fact tables
    print("\n[7/11] Generating fact_crude_shipments...")
    shipments = generate_crude_shipments(spark, refineries, crude_types, tanks, 
                                        int(10000000 * sample_rate))
    shipments.write.mode("overwrite").parquet(f"{output_path}/bronze/fact_crude_shipments")
    print(f"       Generated {shipments.count()} shipments")
    
    print("\n[8/11] Generating fact_quality_tests...")
    quality_tests = generate_quality_tests(spark, refineries, shipments, tanks,
                                          int(15000000 * sample_rate))
    quality_tests.write.mode("overwrite").parquet(f"{output_path}/bronze/fact_quality_tests")
    print(f"       Generated {quality_tests.count()} quality tests")
    
    print("\n[9/11] Generating fact_tank_inventory...")
    tank_inventory = generate_tank_inventory(spark, tanks, 365)
    tank_inventory.write.mode("overwrite").parquet(f"{output_path}/bronze/fact_tank_inventory")
    print(f"       Generated {tank_inventory.count()} inventory records")
    
    print("\n[10/11] Generating fact_production_output...")
    production = generate_production_output(spark, units, products, crude_types, 365)
    production.write.mode("overwrite").parquet(f"{output_path}/bronze/fact_production_output")
    print(f"       Generated {production.count()} production records")
    
    print("\n[11/11] Generating fact_sensor_readings (batch - historical)...")
    start_date = date(2024, 1, 1)
    end_date = date(2024, 12, 31)
    sensor_readings = generate_sensor_readings_batch(spark, sensors, start_date, end_date, 
                                                    sample_rate=sample_rate * 0.1)
    sensor_readings.write.mode("overwrite").parquet(f"{output_path}/bronze/fact_sensor_readings")
    print(f"       Generated {sensor_readings.count()} sensor readings")
    
    print("\n" + "=" * 60)
    print("DATA GENERATION COMPLETE!")
    print("=" * 60)
    
    # Print summary
    print("\nData Location:", output_path)
    print("\nTo check sizes:")
    print(f"  dbutils.fs.ls('{output_path}/bronze')")


# Run generation
# generate_all_data(spark, "/mnt/data/crude_oil_pipeline", sample_rate=1.0)
```

---

## Streaming Data Simulator

For continuous streaming simulation, create a separate process:

```python
# Notebook: 02_stream_sensor_data

import time
import json
from datetime import datetime
from kafka import KafkaProducer
import random

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "your-kafka-broker:9092"
TOPIC_NAME = "sensor_readings"
SENSORS_TO_SIMULATE = 10000  # Subset for demo
READINGS_PER_SECOND = 1000

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Sensor simulation parameters
sensor_configs = {
    f"SENS-{str(i).zfill(6)}": {
        "unit_id": f"UNIT-{str(i // 100).zfill(5)}",
        "refinery_id": f"REF-{str(i // 1000).zfill(3)}",
        "base_value": random.uniform(300, 600),
        "variance": random.uniform(10, 50),
        "anomaly_probability": 0.005
    }
    for i in range(SENSORS_TO_SIMULATE)
}

def generate_reading(sensor_id, config):
    """Generate a single sensor reading."""
    is_anomaly = random.random() < config["anomaly_probability"]
    
    if is_anomaly:
        value = config["base_value"] + random.uniform(100, 200) * random.choice([-1, 1])
    else:
        value = config["base_value"] + random.gauss(0, config["variance"])
    
    return {
        "reading_id": str(uuid.uuid4()),
        "sensor_id": sensor_id,
        "unit_id": config["unit_id"],
        "refinery_id": config["refinery_id"],
        "reading_timestamp": datetime.utcnow().isoformat(),
        "reading_value": round(value, 2),
        "reading_quality": "GOOD" if not is_anomaly else "UNCERTAIN",
        "is_interpolated": False,
        "event_time": datetime.utcnow().isoformat(),
        "processing_time": datetime.utcnow().isoformat()
    }

def stream_data():
    """Continuously stream sensor data to Kafka."""
    sensor_ids = list(sensor_configs.keys())
    
    while True:
        batch_start = time.time()
        
        for _ in range(READINGS_PER_SECOND):
            sensor_id = random.choice(sensor_ids)
            reading = generate_reading(sensor_id, sensor_configs[sensor_id])
            producer.send(TOPIC_NAME, value=reading)
        
        producer.flush()
        
        # Maintain rate
        elapsed = time.time() - batch_start
        if elapsed < 1:
            time.sleep(1 - elapsed)
        
        print(f"Sent {READINGS_PER_SECOND} readings at {datetime.now()}")

# Run: stream_data()
```

---

## Alternative: Auto Loader Streaming (Without Kafka)

If you prefer not to set up Kafka, use file-based streaming:

```python
# Notebook: 02b_file_based_streaming

import os
import json
import time
from datetime import datetime
import random
import uuid

# Write JSON files to a landing zone that Auto Loader monitors
LANDING_ZONE = "/mnt/data/crude_oil_pipeline/landing/sensor_readings"
BATCH_SIZE = 10000
INTERVAL_SECONDS = 10

def generate_batch():
    """Generate a batch of sensor readings as JSON."""
    readings = []
    
    for _ in range(BATCH_SIZE):
        sensor_num = random.randint(0, 499999)
        base_value = 400 + random.gauss(0, 100)
        
        reading = {
            "reading_id": str(uuid.uuid4()),
            "sensor_id": f"SENS-{str(sensor_num).zfill(6)}",
            "unit_id": f"UNIT-{str(sensor_num // 100).zfill(5)}",
            "refinery_id": f"REF-{str(sensor_num // 1000).zfill(3)}",
            "reading_timestamp": datetime.utcnow().isoformat(),
            "reading_value": round(base_value, 2),
            "reading_quality": "GOOD" if random.random() > 0.03 else "UNCERTAIN",
            "is_interpolated": False,
            "event_time": datetime.utcnow().isoformat(),
            "processing_time": datetime.utcnow().isoformat()
        }
        readings.append(reading)
    
    return readings

def write_batch_to_landing():
    """Write batch to landing zone."""
    readings = generate_batch()
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    filename = f"{LANDING_ZONE}/readings_{timestamp}.json"
    
    with open(filename, 'w') as f:
        for reading in readings:
            f.write(json.dumps(reading) + '\n')
    
    print(f"Wrote {len(readings)} readings to {filename}")

# Continuous generation loop
while True:
    write_batch_to_landing()
    time.sleep(INTERVAL_SECONDS)
```

**Databricks Auto Loader consumer:**

```python
# Notebook: 03_stream_consumer_autoloader

from pyspark.sql.types import *

schema = StructType([
    StructField("reading_id", StringType()),
    StructField("sensor_id", StringType()),
    StructField("unit_id", StringType()),
    StructField("refinery_id", StringType()),
    StructField("reading_timestamp", TimestampType()),
    StructField("reading_value", DoubleType()),
    StructField("reading_quality", StringType()),
    StructField("is_interpolated", BooleanType()),
    StructField("event_time", TimestampType()),
    StructField("processing_time", TimestampType())
])

# Read from landing zone using Auto Loader
df_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/mnt/data/crude_oil_pipeline/schemas/sensor_readings")
    .schema(schema)
    .load("/mnt/data/crude_oil_pipeline/landing/sensor_readings")
)

# Write to Bronze Delta table
(
    df_stream.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/data/crude_oil_pipeline/checkpoints/sensor_readings_bronze")
    .outputMode("append")
    .table("bronze.sensor_readings")
)
```

---

## Medallion Architecture Transformations

### Bronze → Silver Transformations

```python
# Notebook: 04_bronze_to_silver

# ============================================================
# SILVER: Cleansed Sensor Readings with Anomaly Flags
# ============================================================

silver_sensor_readings = spark.sql("""
    SELECT 
        sr.reading_id,
        sr.sensor_id,
        sr.unit_id,
        sr.refinery_id,
        sr.reading_timestamp,
        sr.reading_value,
        sr.reading_quality,
        s.sensor_type,
        s.measurement_unit,
        s.warning_low,
        s.warning_high,
        s.critical_low,
        s.critical_high,
        
        -- Anomaly detection
        CASE 
            WHEN sr.reading_value < s.critical_low OR sr.reading_value > s.critical_high THEN 'CRITICAL'
            WHEN sr.reading_value < s.warning_low OR sr.reading_value > s.warning_high THEN 'WARNING'
            ELSE 'NORMAL'
        END AS alert_status,
        
        -- Data quality flags
        CASE 
            WHEN sr.reading_value < s.min_value OR sr.reading_value > s.max_value THEN true
            ELSE false
        END AS is_out_of_range,
        
        sr.event_time,
        sr.processing_time,
        current_timestamp() AS silver_processed_at
        
    FROM bronze.sensor_readings sr
    LEFT JOIN bronze.dim_sensors s ON sr.sensor_id = s.sensor_id
    WHERE s.is_active = true
""")

# ============================================================
# SILVER: Enriched Shipments
# ============================================================

silver_shipments = spark.sql("""
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
        
    FROM bronze.fact_crude_shipments cs
    LEFT JOIN bronze.dim_refineries r ON cs.refinery_id = r.refinery_id
    LEFT JOIN bronze.dim_crude_types ct ON cs.crude_type_id = ct.crude_type_id
""")

# ============================================================
# SILVER: Production with Efficiency Metrics
# ============================================================

silver_production = spark.sql("""
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
        
        -- Efficiency calculations
        po.yield_percentage / u.design_efficiency AS efficiency_ratio,
        po.output_volume_barrels / (u.capacity_bpd / 24.0) AS capacity_utilization,
        po.energy_consumed_mmbtu / po.output_volume_barrels AS energy_per_barrel,
        
        current_timestamp() AS silver_processed_at
        
    FROM bronze.fact_production_output po
    LEFT JOIN bronze.dim_processing_units u ON po.unit_id = u.unit_id
    LEFT JOIN bronze.dim_refineries r ON po.refinery_id = r.refinery_id
    LEFT JOIN bronze.dim_products p ON po.output_product_id = p.product_id
    LEFT JOIN bronze.dim_crude_types ct ON po.input_crude_type_id = ct.crude_type_id
""")
```

### Silver → Gold Transformations

```python
# Notebook: 05_silver_to_gold

# ============================================================
# GOLD: Real-Time Anomaly Alerts (Streaming Aggregation)
# ============================================================

gold_anomaly_alerts = spark.sql("""
    SELECT 
        refinery_id,
        unit_id,
        sensor_type,
        window(reading_timestamp, '5 minutes') AS time_window,
        COUNT(*) AS reading_count,
        SUM(CASE WHEN alert_status = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_count,
        SUM(CASE WHEN alert_status = 'WARNING' THEN 1 ELSE 0 END) AS warning_count,
        AVG(reading_value) AS avg_value,
        STDDEV(reading_value) AS stddev_value,
        MIN(reading_value) AS min_value,
        MAX(reading_value) AS max_value,
        
        -- Alert severity score
        (SUM(CASE WHEN alert_status = 'CRITICAL' THEN 10 
                  WHEN alert_status = 'WARNING' THEN 3 
                  ELSE 0 END) / COUNT(*)) AS alert_severity_score
                  
    FROM silver.sensor_readings
    GROUP BY refinery_id, unit_id, sensor_type, window(reading_timestamp, '5 minutes')
    HAVING critical_count > 0 OR warning_count > 5
""")

# ============================================================
# GOLD: Equipment Health Score
# ============================================================

gold_equipment_health = spark.sql("""
    WITH sensor_stats AS (
        SELECT 
            unit_id,
            sensor_type,
            DATE(reading_timestamp) AS reading_date,
            AVG(reading_value) AS avg_value,
            STDDEV(reading_value) AS value_volatility,
            SUM(CASE WHEN alert_status != 'NORMAL' THEN 1 ELSE 0 END) / COUNT(*) AS anomaly_rate
        FROM silver.sensor_readings
        WHERE reading_timestamp >= DATE_SUB(current_date(), 7)
        GROUP BY unit_id, sensor_type, DATE(reading_timestamp)
    ),
    unit_scores AS (
        SELECT 
            unit_id,
            reading_date,
            AVG(anomaly_rate) AS avg_anomaly_rate,
            AVG(value_volatility) AS avg_volatility
        FROM sensor_stats
        GROUP BY unit_id, reading_date
    )
    SELECT 
        u.unit_id,
        u.unit_name,
        u.unit_type,
        u.refinery_id,
        r.refinery_name,
        us.reading_date,
        
        -- Health score (100 = perfect, lower = worse)
        GREATEST(0, 100 - (us.avg_anomaly_rate * 500) - (us.avg_volatility / 10)) AS health_score,
        
        -- Days since maintenance
        DATEDIFF(current_date(), u.last_maintenance_date) AS days_since_maintenance,
        u.maintenance_interval_days,
        
        -- Maintenance urgency
        CASE 
            WHEN DATEDIFF(current_date(), u.last_maintenance_date) > u.maintenance_interval_days THEN 'OVERDUE'
            WHEN DATEDIFF(current_date(), u.last_maintenance_date) > u.maintenance_interval_days * 0.9 THEN 'DUE_SOON'
            ELSE 'OK'
        END AS maintenance_status
        
    FROM bronze.dim_processing_units u
    LEFT JOIN unit_scores us ON u.unit_id = us.unit_id
    LEFT JOIN bronze.dim_refineries r ON u.refinery_id = r.refinery_id
""")

# ============================================================
# GOLD: Daily Refinery KPIs
# ============================================================

gold_refinery_kpis = spark.sql("""
    SELECT 
        r.refinery_id,
        r.refinery_name,
        r.location_city,
        r.location_state,
        r.capacity_bpd,
        DATE(po.production_hour) AS production_date,
        
        -- Production metrics
        SUM(po.input_volume_barrels) AS total_crude_processed,
        SUM(po.output_volume_barrels) AS total_products_produced,
        SUM(po.output_volume_barrels) / SUM(po.input_volume_barrels) AS overall_yield,
        
        -- Capacity utilization
        SUM(po.input_volume_barrels) / r.capacity_bpd AS capacity_utilization,
        
        -- Energy efficiency
        SUM(po.energy_consumed_mmbtu) / SUM(po.output_volume_barrels) AS energy_intensity,
        
        -- Downtime
        SUM(po.downtime_minutes) AS total_downtime_minutes,
        SUM(po.downtime_minutes) / (COUNT(DISTINCT po.unit_id) * 60) AS downtime_percentage,
        
        -- Quality
        SUM(CASE WHEN po.quality_grade = 'On-Spec' THEN 1 ELSE 0 END) / COUNT(*) AS on_spec_rate
        
    FROM silver.production po
    JOIN bronze.dim_refineries r ON po.refinery_id = r.refinery_id
    GROUP BY r.refinery_id, r.refinery_name, r.location_city, r.location_state, 
             r.capacity_bpd, DATE(po.production_hour)
""")

# ============================================================
# GOLD: Product Yield Analysis
# ============================================================

gold_product_yield = spark.sql("""
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
        
    FROM silver.production po
    JOIN bronze.dim_crude_types ct ON po.input_crude_type_id = ct.crude_type_id
    JOIN bronze.dim_products p ON po.output_product_id = p.product_id
    GROUP BY ct.crude_type_id, ct.crude_name, ct.classification, ct.sweetness,
             p.product_id, p.product_name, p.product_category, p.typical_yield_pct
""")

# ============================================================
# GOLD: Inventory Status
# ============================================================

gold_inventory_status = spark.sql("""
    WITH latest_inventory AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY tank_id ORDER BY measurement_timestamp DESC) AS rn
        FROM silver.tank_inventory
    )
    SELECT 
        t.tank_id,
        t.tank_name,
        t.refinery_id,
        r.refinery_name,
        t.product_type,
        t.capacity_barrels,
        li.volume_barrels AS current_volume,
        li.fill_percentage,
        li.measurement_timestamp AS last_measured,
        
        -- Days of supply (based on average daily consumption)
        -- This would need production data to calculate properly
        
        -- Inventory status
        CASE 
            WHEN li.fill_percentage < t.min_operating_level THEN 'CRITICALLY_LOW'
            WHEN li.fill_percentage < 0.25 THEN 'LOW'
            WHEN li.fill_percentage > t.max_operating_level THEN 'OVER_FILLED'
            WHEN li.fill_percentage > 0.85 THEN 'NEAR_CAPACITY'
            ELSE 'NORMAL'
        END AS inventory_status
        
    FROM bronze.dim_storage_tanks t
    JOIN latest_inventory li ON t.tank_id = li.tank_id AND li.rn = 1
    JOIN bronze.dim_refineries r ON t.refinery_id = r.refinery_id
    WHERE t.is_active = true
""")
```

---

## Dashboard Recommendations

### Dashboard 1: Real-Time Operations Center

**Purpose:** Monitor live refinery operations and detect anomalies

**Visualizations:**
1. **Map View** - Refineries colored by health score (green/yellow/red)
2. **Alert Feed** - Live scrolling list of critical/warning alerts
3. **Sensor Heatmap** - Processing units × sensor types showing anomaly rates
4. **Trend Charts** - Key sensor readings over last 24 hours
5. **KPI Cards** - Total alerts, capacity utilization, active units

### Dashboard 2: Production Performance

**Purpose:** Track yield efficiency and production metrics

**Visualizations:**
1. **Yield by Crude Type** - Bar chart comparing yields across crude varieties
2. **Capacity Utilization Trend** - Line chart over time by refinery
3. **Product Mix** - Pie/donut chart of product output breakdown
4. **Energy Efficiency** - Energy per barrel trend
5. **Downtime Analysis** - Pareto chart of downtime causes

### Dashboard 3: Inventory & Supply Chain

**Purpose:** Monitor inventory levels and shipment status

**Visualizations:**
1. **Tank Levels** - Gauge charts for each tank showing fill %
2. **Shipment Map** - In-transit shipments on world map
3. **Days of Supply** - Inventory coverage by product type
4. **Quality Variance** - Scatter plot of actual vs. expected crude quality
5. **Delivery Performance** - On-time delivery rate trend

### Dashboard 4: Equipment Health & Maintenance

**Purpose:** Predictive maintenance and equipment monitoring

**Visualizations:**
1. **Health Score Leaderboard** - Ranked list of units by health score
2. **Maintenance Calendar** - Units due/overdue for maintenance
3. **Anomaly Trends** - 7-day rolling anomaly rates by unit
4. **Sensor Reliability** - % of sensors reporting good quality data
5. **Failure Risk Matrix** - Health score vs. days since maintenance

---

## Data Volume Summary

| Layer | Dataset | Est. Size | Notes |
|-------|---------|-----------|-------|
| Bronze | dim_* tables | ~100MB | Reference data |
| Bronze | fact_sensor_readings | ~70GB | Main volume driver |
| Bronze | fact_crude_shipments | ~5GB | |
| Bronze | fact_quality_tests | ~3GB | |
| Bronze | fact_tank_inventory | ~8GB | |
| Bronze | fact_production_output | ~10GB | |
| Silver | All tables | ~50GB | After cleansing/dedup |
| Gold | Aggregated tables | ~5GB | Summarized data |
| **Total** | | **~100-150GB** | |

Adjust `sample_rate` parameter in generation script to scale up/down.

---

## Quick Start Commands

```python
# 1. Install dbldatagen
%pip install dbldatagen

# 2. Run data generation (adjust sample_rate for size)
generate_all_data(spark, "/mnt/data/crude_oil_pipeline", sample_rate=1.0)

# 3. Start streaming simulator (choose one method)
# Option A: Kafka-based (requires Kafka cluster)
# Option B: File-based with Auto Loader (simpler setup)

# 4. Create Delta tables from parquet
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

for table in ["dim_refineries", "dim_processing_units", "dim_sensors", 
              "dim_storage_tanks", "dim_crude_types", "dim_products",
              "fact_crude_shipments", "fact_quality_tests", 
              "fact_tank_inventory", "fact_production_output"]:
    spark.read.parquet(f"/mnt/data/crude_oil_pipeline/bronze/{table}") \
        .write.format("delta").mode("overwrite").saveAsTable(f"bronze.{table}")
```

---

## Next Steps

1. **Generate the data** using the provided scripts
2. **Set up streaming** using either Kafka or Auto Loader approach
3. **Build Delta Live Tables pipeline** for automated Bronze→Silver→Gold
4. **Create dashboards** in Databricks SQL or connect to Power BI/Tableau
5. **Add data quality checks** using Delta Lake expectations
6. **Implement Unity Catalog** for governance (optional)