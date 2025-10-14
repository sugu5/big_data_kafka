# Big Data Kafka

This project demonstrates the use of Kafka for big data processing. Below are the key components and configurations used in the project.

---

## Fetch Data from API
**File:** `fetch_from_api.py`  
This script is responsible for fetching data from an API and producing it to a Kafka topic.

---

## Producer Configuration
**File:** `fetch_from_api_conf.py`  
The producer is configured with the following features:

- ✅ **Batching:** Sends 100 messages at a time.
- ✅ **Idempotent:** Ensures no duplicates even on retry or crash.
- ✅ **Reliable Delivery:** Waits for all replicas (acks=all).
- ✅ **High Throughput:** Uses compression and linger settings.
- ✅ **No Loss on Shutdown:** Flushes the buffer on close.

---

## Usage
1. Start Kafka and Zookeeper using Docker Compose.
2. Run the producer script to fetch data from the API and send it to Kafka.
3. Consume the data from the Kafka topic using a consumer script.