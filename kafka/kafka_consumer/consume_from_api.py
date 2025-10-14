from kafka import KafkaConsumer
import json

# Create Kafka consumer
consumer = KafkaConsumer(
    'crypto_stream',        
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='stock_consumer_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Listening for messages on 'stock_stream' topic...")

for message in consumer:
    print("Received message:", message.value)
