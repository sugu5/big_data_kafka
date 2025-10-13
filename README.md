# big_data_kafka
<!-- Fetch from api file: fetch_from_api.py -->
simply  fetch data from api



<!-- fetch from api file : fetch_from_api_conf.py -->
Tweek in configuration for the producer 
✅ Batching: Sends 100 messages at a time.
✅ Idempotent: No duplicates even on retry or crash.
✅ Reliable delivery: Waits for all replicas (acks=all).
✅ High throughput: Uses compression and linger.
✅ No loss on shutdown: Flushes buffer on close.