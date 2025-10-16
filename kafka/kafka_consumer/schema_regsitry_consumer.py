from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer, SerializationContext, MessageField

# Schema Registry setup
schema_registry_conf = {
    'url': 'http://localhost:8081'
}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Avro value deserializer
avro_deserializer = AvroDeserializer(
    schema_str=None,  # Optional, can auto-fetch from registry
    schema_registry_client=schema_registry_client
)

# Consumer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'key.deserializer': StringDeserializer('utf_8'),
    'value.deserializer': avro_deserializer,
    'group.id': 'binance_consumer_group',
    'auto.offset.reset': 'earliest'
}

consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe(['binance_trades'])

print("🎯 Subscribed to topic: binance_trades")
print("📥 Waiting for messages...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        print(f"✅ Received: key={msg.key()}, value={msg.value()}")
except KeyboardInterrupt:
    print("✅ Consumer closed cleanly.")
finally:
    consumer.close()