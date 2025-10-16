import json
import time
import websocket
from collections import deque
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import os

# ========== CONFIG ==========
# Use environment variables or service names if running inside the Docker network.
# Since you're using 'localhost', we assume the Python script runs on the host machine.
KAFKA_BROKER = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
BINANCE_SOCKET = "wss://stream.binance.com:9443/ws/btcusdt@trade"
TOPIC = "binance_trades"

# Batch settings
BATCH_SIZE = 50
BATCH_FLUSH_INTERVAL = 5  # seconds

# ========== AVRO SCHEMA (FIXED) ==========
# CRITICAL FIX: Changed price and quantity from 'float' to 'double' 
# to ensure sufficient precision for crypto trade data.
avro_schema_str = """
{
  "type": "record",
  "name": "BinanceTrade",
  "namespace": "com.crypto",
  "fields": [
    {"name": "event_type", "type": "string"},
    {"name": "event_time", "type": "long"},
    {"name": "symbol", "type": "string"},
    {"name": "trade_id", "type": "long"},
    {"name": "price", "type": "double"},   
    {"name": "quantity", "type": "double"}, 
    {"name": "trade_time", "type": "long"},
    {"name": "is_market_maker", "type": "boolean"},
    {"name": "ignore_flag", "type": "boolean"}
  ]
}
"""

# ========== SCHEMA REGISTRY & SERIALIZER ==========
MAX_RETRIES = 5
RETRY_DELAY = 5 # seconds

for attempt in range(MAX_RETRIES):
    try:
        schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
        # Attempt to register/fetch schema to verify connection
        avro_serializer = AvroSerializer(schema_registry_client, avro_schema_str)
        
        print(f"✅ Schema Registry connection established on attempt {attempt + 1}.")
        # Optional: Test connectivity to Schema Registry
        print(f"Schema Registry Status: {schema_registry_client.get_subjects()}")
        break # Exit loop if successful
    
    except Exception as e:
        if attempt < MAX_RETRIES - 1:
            print(f"⚠️ Schema Registry connection failed on attempt {attempt + 1}/{MAX_RETRIES}. Retrying in {RETRY_DELAY} seconds. Error: {e}")
            time.sleep(RETRY_DELAY)
        else:
            print(f"❌ FATAL: Could not initialize Schema Registry client after {MAX_RETRIES} attempts. Is the service running at {SCHEMA_REGISTRY_URL}? Error: {e}")
            exit(1)


# ========== PRODUCER CONFIG ==========
producer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    # Assign the serializer to the value
    'value.serializer': avro_serializer,
    'acks': 'all',
    'enable.idempotence': True,
    'retries': 5,
    'max.in.flight.requests.per.connection': 1,
    'linger.ms': 100,
    'batch.size': 32_768,
    'compression.type': 'snappy'
}
producer = SerializingProducer(producer_conf)

# ========== STATE ==========
buffer = deque()
last_flush_time = time.time()
msg_count = 0
start_time = time.time()

# ========== CALLBACK ==========
def delivery_report(err, msg):
    """Callback for successful or failed delivery."""
    if err:
        print(f"❌ Delivery failed: {err}")
    # else:
    #     # Reduced verbosity to prevent excessive logging
    #     print(f"✅ Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

# ========== BATCH FLUSH ==========
def flush_batch():
    """Flushes the buffered messages to the Kafka producer."""
    global buffer, last_flush_time
    count_to_send = len(buffer)
    
    while buffer:
        record = buffer.popleft()
        # Key is crucial for partitioning - ensure it's a string/bytes
        key = str(record["symbol"]) 
        
        # We don't print the whole record here to avoid spamming the console
        producer.produce(
            topic=TOPIC, 
            key=key,
            value=record, 
            on_delivery=delivery_report
        )
    
    # Crucial: Call flush() to wait for messages to be delivered
    producer.flush() 
    last_flush_time = time.time()
    print(f"🚀 Flushed {count_to_send} messages to Kafka (batch size or timeout reached)")

# ========== WEBSOCKET HANDLERS ==========
def on_message(ws, message):
    global msg_count, start_time, last_flush_time
    
    try:
        data = json.loads(message)

        # 1. Clean and map the data types to match the Avro schema
        record = {
            "event_type": data.get("e"),
            "event_time": int(data.get("E", 0)), # Ensure time is an integer (long in Avro)
            "symbol": data.get("s"),
            "trade_id": int(data.get("t", 0)),
            "price": float(data.get("p", 0)),
            "quantity": float(data.get("q", 0)),
            "trade_time": int(data.get("T", 0)), # Ensure time is an integer
            "is_market_maker": data.get("m"),
            "ignore_flag": data.get("M")
        }

        buffer.append(record)
        msg_count += 1
        
        # 2. Asynchronously serve delivery reports and network events (CRITICAL)
        producer.poll(0)

        # Flush based on size or time
        if len(buffer) >= BATCH_SIZE or (time.time() - last_flush_time) >= BATCH_FLUSH_INTERVAL:
            flush_batch() 

    except json.JSONDecodeError as e:
        print(f"❌ JSON Decode Error: {e} - Message: {message[:100]}...")
    except Exception as e:
        print(f"❌ Unexpected Error in on_message: {e}")
            

def on_open(ws):
    print("🔗 Connected to Binance WebSocket...")

def on_error(ws, error):
    print("❌ Error:", error)

def on_close(ws, code, msg):
    print("🔒 Connection closed:", code, msg)
    print("Flushing remaining messages...")
    flush_batch()

# ========== MAIN ==========
if __name__ == "__main__":
    print(f"Starting producer for {TOPIC} at {KAFKA_BROKER} with schema registry {SCHEMA_REGISTRY_URL}")
    print("!!! ENSURE ALL DOCKER SERVICES ARE UP BEFORE RUNNING !!!")
    
    # We must allow time for Zookeeper, Kafka, and Schema Registry to start
    # before we attempt the connection/schema registration.
    time.sleep(20) # Conservative waiting mechanism for all services to start

    ws = websocket.WebSocketApp(
        BINANCE_SOCKET,
        on_message=on_message,
        on_open=on_open,
        on_error=on_error,
        on_close=on_close
    )
    # ws.run_forever() is blocking, but ensures persistent connection
    ws.run_forever()
