from kafka.admin import KafkaAdminClient

def list_topics():
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092"
    )
    try:
        topics = admin_client.list_topics()
        print("\nAvailable topics:")
        for topic in topics:
            print(f"- {topic}")
    except Exception as e:
        print(f"Error listing topics: {e}")
    finally:
        admin_client.close()

if __name__ == "__main__":
    list_topics()