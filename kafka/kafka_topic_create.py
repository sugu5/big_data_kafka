from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

def create_topic(topic_name, num_partitions=2, replication_factor=1):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092"
    )
    
    topic_list = []
    topic_list.append(NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor
    ))
    
    try:
        admin_client.create_topics(new_topics=topic_list)
        print(f"Topic '{topic_name}' created successfully!")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists!")
    except Exception as e:
        print(f"Error creating topic: {e}")
    finally:
        admin_client.close()

if __name__ == "__main__":
    # Create a new topic
    create_topic("binance_trades")