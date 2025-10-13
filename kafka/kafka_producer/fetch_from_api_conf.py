import json
import websocket
import time
from kafka import KafkaProducer
from datetime import datetime

BINANCE_SOCKET = "wss://stream.binance.com:9443/ws/btcusdt@trade"

# Kafka producer with tuned configurations
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    enable_idempotence=True,
    retries=5,
    max_in_flight_requests_per_connection=1,
    linger_ms=1000,
    batch_size=32768,
    compression_type='snappy',
    delivery_timeout_ms=120000
)

buffer = []
BATCH_SIZE = 100  # send to Kafka after 100 messages
msg_count = 0
start_time = time.time()

def flush_buffer():
    """Send all buffered messages to Kafka"""
    global buffer
    if buffer:
        for msg in buffer:
            producer.send('crypto_stream', value=msg)
        producer.flush()  # ensure delivery before clearing
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Flushed {len(buffer)} messages to Kafka ✅")
        buffer = []

def print_throughput():
    """Print how many messages received per minute"""
    global msg_count, start_time
    now = time.time()
    elapsed = now - start_time
    if elapsed >= 60:
        mpm = msg_count / (elapsed / 60)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] → {mpm:.2f} messages/minute")
        msg_count = 0
        start_time = now

def on_message(ws, message):
    global msg_count
    msg_count += 1
    msg = json.loads(message)
    buffer.append(msg)

    if len(buffer) >= BATCH_SIZE:
        flush_buffer()

    print_throughput()

def on_open(ws):
    print("✅ Connected to Binance WebSocket...")

def on_error(ws, error):
    print("❌ Error:", error)

def on_close(ws, close_status_code, close_msg):
    print("🔻 Closed connection:", close_status_code, close_msg)
    flush_buffer()  # flush before exit

if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        BINANCE_SOCKET,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )
    ws.run_forever()
