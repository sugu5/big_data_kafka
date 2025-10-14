import json
import time
import websocket
from collections import deque
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# ========== CONFIG ==========
BINANCE_SOCKET = "wss://stream.binance.com:9443/ws/btcusdt@trade"
KAFKA_BROKER = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "binance_trades"

# Batch settings
BATCH_SIZE = 50
BATCH_FLUSH_INTERVAL = 5  # seconds

# ========== AVRO SCHEMA ==========
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
    {"name": "price", "type": "float"},
    {"name": "quantity", "type": "float"},
    {"name": "trade_time", "type": "long"},
    {"name": "is_market_maker", "type": "boolean"},
    {"name": "ignore_flag", "type": "boolean"}
  ]
}
"""

# ========== SCHEMA REGISTRY ==========
schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
avro_serializer = AvroSerializer(schema_registry_client, avro_schema_str)

# ========== PRODUCER CONFIG ==========
producer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
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
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

# ========== BATCH FLUSH ==========
def flush_batch():
    global buffer, last_flush_time
    while buffer:
        record = buffer.popleft()
        key = str(record["symbol"])
        producer.produce(
            topic=TOPIC, 
            key=key,
            value=record, 
            on_delivery=delivery_report
        )
    producer.flush()
    last_flush_time = time.time()
    print(f"🚀 Flushed batch to Kafka (batch size or timeout reached)")

# ========== WEBSOCKET HANDLERS ==========
def on_message(ws, message):
    global msg_count, start_time, last_flush_time
    data = json.loads(message)

    record = {
        "event_type": data.get("e"),
        "event_time": data.get("E"),
        "symbol": data.get("s"),
        "trade_id": data.get("t"),
        "price": float(data.get("p", 0)),
        "quantity": float(data.get("q", 0)),
        "trade_time": data.get("T"),
        "is_market_maker": data.get("m"),
        "ignore_flag": data.get("M")
    }

    buffer.append(record)
    msg_count += 1

    # Track message rate per minute
    elapsed = time.time() - start_time
    if elapsed >= 60:
        print(f"📈 Messages received per minute: {msg_count}")
        msg_count = 0
        start_time = time.time()

    # Flush based on size or time
    if len(buffer) >= BATCH_SIZE or (time.time() - last_flush_time) >= BATCH_FLUSH_INTERVAL:
        flush_batch()                   

def on_open(ws):
    print("🔗 Connected to Binance WebSocket...")

def on_error(ws, error):
    print("❌ Error:", error)

def on_close(ws, code, msg):
    print("🔒 Connection closed:", code, msg)
    flush_batch()

# ========== MAIN ==========
if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        BINANCE_SOCKET,
        on_message=on_message,
        on_open=on_open,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()