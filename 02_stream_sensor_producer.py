# Databricks notebook source
# MAGIC %md
# MAGIC # Sensor Data Stream Producer - Azure Event Hubs
# MAGIC
# MAGIC This notebook generates simulated sensor readings from oil refineries and streams them to Azure Event Hubs using the Kafka protocol.
# MAGIC
# MAGIC **Usage:**
# MAGIC - Configure the widgets below
# MAGIC - Run all cells
# MAGIC - The producer will run until you stop it or the duration expires

# COMMAND ----------

# MAGIC %pip install confluent-kafka

# COMMAND ----------

# DBTITLE 1,Create Widgets for Configuration
dbutils.widgets.text("rate", "1000", "Messages per Second")
dbutils.widgets.text("duration", "3600", "Duration (seconds, 0=infinite)")
dbutils.widgets.text("eventhub_name", "sensor-readings", "Event Hub Name")

# COMMAND ----------

# DBTITLE 1,Configuration
EVENTHUB_NAMESPACE = "parex-demo"
EVENTHUB_CONNECTION_STRING = "Endpoint=sb://....."
EVENTHUB_NAME = dbutils.widgets.get("eventhub_name")

# Producer settings from widgets
MESSAGES_PER_SECOND = int(dbutils.widgets.get("rate"))
DURATION_SECONDS = int(dbutils.widgets.get("duration"))
if DURATION_SECONDS == 0:
    DURATION_SECONDS = None  # Run indefinitely

# Kafka endpoint for Event Hubs
BOOTSTRAP_SERVERS = f"{EVENTHUB_NAMESPACE}.servicebus.windows.net:9093"

print("=" * 60)
print("Configuration")
print("=" * 60)
print(f"  Namespace:       {EVENTHUB_NAMESPACE}")
print(f"  Event Hub:       {EVENTHUB_NAME}")
print(f"  Bootstrap:       {BOOTSTRAP_SERVERS}")
print(f"  Rate:            {MESSAGES_PER_SECOND} msg/sec")
print(f"  Duration:        {'Infinite' if DURATION_SECONDS is None else f'{DURATION_SECONDS}s'}")
print("=" * 60)

# COMMAND ----------

# DBTITLE 1,Kafka Producer Configuration for Event Hubs
PRODUCER_CONFIG = {
    # Event Hubs endpoint
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    
    # Security - Event Hubs requires SASL_SSL
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': '$ConnectionString',  # Literal string, not a variable!
    'sasl.password': EVENTHUB_CONNECTION_STRING,
    
    # Producer settings
    'client.id': 'databricks-sensor-producer',
    'acks': 'all',
    'retries': 3,
    'retry.backoff.ms': 500,
    'linger.ms': 10,
    'batch.size': 32768,
    
    # Event Hubs tuning
    'request.timeout.ms': 60000,
    'socket.keepalive.enable': True,
}

# COMMAND ----------

# DBTITLE 1,Sensor Simulator Class
import random
import uuid
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, Any, List

@dataclass
class SensorSpec:
    """Specification for a sensor type."""
    sensor_type: str
    unit: str
    base_value: float
    variance: float
    min_valid: float
    max_valid: float
    warning_low: float
    warning_high: float
    critical_low: float
    critical_high: float
    anomaly_rate: float = 0.005


# Realistic sensor configurations for refinery equipment
SENSOR_SPECS = {
    'temperature': SensorSpec(
        sensor_type='Temperature', unit='Fahrenheit',
        base_value=450.0, variance=50.0,
        min_valid=100.0, max_valid=1000.0,
        warning_low=200.0, warning_high=700.0,
        critical_low=150.0, critical_high=850.0,
    ),
    'pressure': SensorSpec(
        sensor_type='Pressure', unit='PSI',
        base_value=150.0, variance=25.0,
        min_valid=0.0, max_valid=500.0,
        warning_low=50.0, warning_high=300.0,
        critical_low=25.0, critical_high=400.0,
    ),
    'flow_rate': SensorSpec(
        sensor_type='Flow Rate', unit='BPH',
        base_value=5000.0, variance=500.0,
        min_valid=0.0, max_valid=15000.0,
        warning_low=1000.0, warning_high=12000.0,
        critical_low=500.0, critical_high=14000.0,
    ),
    'level': SensorSpec(
        sensor_type='Level', unit='Percent',
        base_value=65.0, variance=12.0,
        min_valid=0.0, max_valid=100.0,
        warning_low=15.0, warning_high=90.0,
        critical_low=5.0, critical_high=98.0,
    ),
    'vibration': SensorSpec(
        sensor_type='Vibration', unit='mm/s',
        base_value=4.0, variance=2.0,
        min_valid=0.0, max_valid=50.0,
        warning_low=0.0, warning_high=15.0,
        critical_low=0.0, critical_high=25.0,
        anomaly_rate=0.01,
    ),
}


class SensorSimulator:
    """Simulates sensor readings from refinery equipment."""
    
    def __init__(self, num_refineries: int = 50, units_per_refinery: int = 10, sensors_per_unit: int = 20):
        self.sensors = {}
        spec_keys = list(SENSOR_SPECS.keys())
        sensor_idx = 0
        
        for ref_idx in range(num_refineries):
            refinery_id = f"REF-{ref_idx:03d}"
            
            for unit_idx in range(units_per_refinery):
                global_unit_idx = ref_idx * units_per_refinery + unit_idx
                unit_id = f"UNIT-{global_unit_idx:05d}"
                
                for sens_idx in range(sensors_per_unit):
                    sensor_id = f"SENS-{sensor_idx:06d}"
                    spec_key = spec_keys[sens_idx % len(spec_keys)]
                    spec = SENSOR_SPECS[spec_key]
                    
                    self.sensors[sensor_id] = {
                        'sensor_id': sensor_id,
                        'unit_id': unit_id,
                        'refinery_id': refinery_id,
                        'spec': spec,
                        'base_offset': random.uniform(-0.1, 0.1) * spec.base_value,
                    }
                    sensor_idx += 1
        
        self.sensor_ids = list(self.sensors.keys())
        print(f"Initialized {len(self.sensors):,} sensors across {num_refineries} refineries")
    
    def generate_reading(self, sensor_id: str = None) -> Dict[str, Any]:
        """Generate a single sensor reading."""
        if sensor_id is None:
            sensor_id = random.choice(self.sensor_ids)
        
        sensor = self.sensors[sensor_id]
        spec = sensor['spec']
        
        # Determine if anomaly
        is_anomaly = random.random() < spec.anomaly_rate
        
        if is_anomaly:
            if random.random() > 0.5:
                value = spec.warning_high + random.uniform(0, spec.critical_high - spec.warning_high) * random.uniform(0.5, 1.5)
            else:
                value = spec.warning_low - random.uniform(0, spec.warning_low - spec.critical_low) * random.uniform(0.5, 1.5)
        else:
            value = spec.base_value + sensor['base_offset'] + random.gauss(0, spec.variance)
        
        # Clamp to valid range
        value = max(spec.min_valid, min(spec.max_valid, value))
        
        # Determine quality
        if value <= spec.critical_low or value >= spec.critical_high:
            quality = "BAD"
        elif value <= spec.warning_low or value >= spec.warning_high:
            quality = "UNCERTAIN"
        else:
            quality = "GOOD"
        
        now = datetime.utcnow()
        timestamp_str = now.strftime('%Y-%m-%dT%H:%M:%S.') + f'{now.microsecond:06d}Z'
        
        return {
            'reading_id': str(uuid.uuid4()),
            'sensor_id': sensor_id,
            'unit_id': sensor['unit_id'],
            'refinery_id': sensor['refinery_id'],
            'sensor_type': spec.sensor_type,
            'measurement_unit': spec.unit,
            'reading_timestamp': timestamp_str,
            'reading_value': round(value, 2),
            'reading_quality': quality,
            'is_anomaly': is_anomaly,
            'is_interpolated': False,
            'event_time': timestamp_str,
            'processing_time': timestamp_str,
        }

# COMMAND ----------

# DBTITLE 1,Event Hubs Producer Class
import json
import time
from confluent_kafka import Producer

class EventHubsProducer:
    """Produces sensor readings to Azure Event Hubs."""
    
    def __init__(self, config: dict, topic: str):
        self.topic = topic
        self.producer = Producer(config)
        self.simulator = SensorSimulator()
        
        # Stats
        self.messages_sent = 0
        self.messages_failed = 0
        self.bytes_sent = 0
    
    def _delivery_callback(self, err, msg):
        """Callback for delivery confirmation."""
        if err is not None:
            self.messages_failed += 1
        else:
            self.messages_sent += 1
            self.bytes_sent += len(msg.value())
    
    def produce_batch(self, batch_size: int):
        """Produce a batch of sensor readings."""
        for _ in range(batch_size):
            reading = self.simulator.generate_reading()
            value = json.dumps(reading).encode('utf-8')
            key = reading['sensor_id'].encode('utf-8')
            
            try:
                self.producer.produce(
                    topic=self.topic,
                    key=key,
                    value=value,
                    callback=self._delivery_callback
                )
            except BufferError:
                self.producer.poll(1)
                self.producer.produce(
                    topic=self.topic,
                    key=key,
                    value=value,
                    callback=self._delivery_callback
                )
            
            self.producer.poll(0)
    
    def run(self, messages_per_second: int, duration_seconds: int = None, report_interval: int = 10):
        """Run the producer."""
        print("\n" + "=" * 70)
        print("Starting Event Hubs Sensor Producer")
        print("=" * 70)
        print(f"  Topic:           {self.topic}")
        print(f"  Target Rate:     {messages_per_second:,} msg/sec")
        print(f"  Duration:        {'Infinite' if duration_seconds is None else f'{duration_seconds}s'}")
        print(f"  Total Sensors:   {len(self.simulator.sensors):,}")
        print("=" * 70)
        print("\nProducing... (Cancel the cell to stop)\n")
        
        start_time = time.time()
        last_report = start_time
        
        batches_per_second = 10
        batch_size = max(1, messages_per_second // batches_per_second)
        batch_interval = 1.0 / batches_per_second
        
        try:
            while True:
                batch_start = time.time()
                
                # Produce batch
                self.produce_batch(batch_size)
                
                # Check duration
                elapsed = time.time() - start_time
                if duration_seconds and elapsed >= duration_seconds:
                    print(f"\n✓ Duration limit reached ({duration_seconds}s)")
                    break
                
                # Progress report
                if time.time() - last_report >= report_interval:
                    rate = self.messages_sent / elapsed if elapsed > 0 else 0
                    mb_sent = self.bytes_sent / (1024 * 1024)
                    print(
                        f"[{datetime.now().strftime('%H:%M:%S')}] "
                        f"Sent: {self.messages_sent:>10,} | "
                        f"Rate: {rate:>8,.0f}/s | "
                        f"Data: {mb_sent:>7.1f} MB | "
                        f"Errors: {self.messages_failed}"
                    )
                    last_report = time.time()
                
                # Rate limiting
                batch_elapsed = time.time() - batch_start
                if batch_elapsed < batch_interval:
                    time.sleep(batch_interval - batch_elapsed)
        
        except KeyboardInterrupt:
            print("\n\n⚠ Interrupted by user")
        
        finally:
            print("\nFlushing remaining messages...")
            self.producer.flush(30)
            
            total_time = time.time() - start_time
            mb_sent = self.bytes_sent / (1024 * 1024)
            
            print("\n" + "=" * 70)
            print("Producer Summary")
            print("=" * 70)
            print(f"  Messages Sent:     {self.messages_sent:,}")
            print(f"  Messages Failed:   {self.messages_failed:,}")
            print(f"  Data Sent:         {mb_sent:.2f} MB")
            print(f"  Runtime:           {total_time:.1f} seconds")
            print(f"  Average Rate:      {self.messages_sent / total_time:,.0f} msg/sec")
            print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Run the Producer
# Create and run the producer
producer = EventHubsProducer(
    config=PRODUCER_CONFIG,
    topic=EVENTHUB_NAME
)

producer.run(
    messages_per_second=MESSAGES_PER_SECOND,
    duration_seconds=DURATION_SECONDS,
    report_interval=10
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes
# MAGIC
# MAGIC - **To stop the producer**: Click the "Cancel" button on the cell above, or interrupt the notebook
# MAGIC - **To run indefinitely**: Set duration widget to `0`
# MAGIC - **Scaling**: For higher throughput, increase the rate or run multiple notebooks in parallel
# MAGIC - **Monitoring**: Check Azure Portal → Event Hubs → Metrics for incoming messages
# MAGIC
# MAGIC ### Volume Estimates
# MAGIC
# MAGIC | Rate (msg/sec) | Duration | Messages | Approx Size |
# MAGIC |----------------|----------|----------|-------------|
# MAGIC | 500 | 1 hour | 1.8M | ~500 MB |
# MAGIC | 1,000 | 1 hour | 3.6M | ~1 GB |
# MAGIC | 1,000 | 24 hours | 86M | ~25 GB |
# MAGIC | 5,000 | 24 hours | 432M | ~120 GB |