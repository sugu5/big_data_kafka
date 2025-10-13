import json
import websocket
import time
from kafka import KafkaProducer
from datetime import datetime

BINANCE_SOCKET = "wss://stream.binance.com:9443/ws/btcusdt@trade"

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

msg_count = 0
start_time = time.time()

def print_throughput():
    """Print how many messages received per minute"""
    global msg_count, start_time
    now = time.time()
    elapsed = now - start_time
    if elapsed >= 60:
        mpm = msg_count / (elapsed / 60)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] → {mpm:.2f} messages/minute")
        # reset
        msg_count = 0
        start_time = now

def on_message(ws, message):
    global msg_count
    msg_count += 1
    msg = json.loads(message)
    producer.send('crypto_stream', value=msg)
    print_throughput()

def on_open(ws):
    print("✅ Connected to Binance WebSocket...")

def on_error(ws, error):
    print("❌ Error:", error)

def on_close(ws, close_status_code, close_msg):
    print("🔻 Closed connection:", close_status_code, close_msg)

if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        BINANCE_SOCKET,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )
    ws.run_forever()
