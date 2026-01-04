import argparse
from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config, INPUT_TOPIC_BASE, OUTPUT_TOPIC_BASE
import time

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

def main(args):
    topics = [INPUT_TOPIC_BASE, OUTPUT_TOPIC_BASE]
    topics_to_create = []
    num_partitions = 2
    replication_factor = 1
    existing_topics = admin_client.list_topics()

    for topic in topics:
        topic_name = f'{topic}_{args.topic_tag}'
        if topic_name in existing_topics:
            try:
                admin_client.delete_topics([topic_name])
                print(f"Deleting existing topic: '{topic_name}'...")
                time.sleep(2)  # Wait for deletion to propagate

                # Check if we managed to delete the topic
                if topic_name in admin_client.list_topics():
                    print(f"Topic '{topic_name}' still exists after deletion attempt.")
                else:
                    print(f"Topic '{topic_name}' deleted.")

            except Exception as e:
                print(f"Error deleting topic '{topic_name}': {e}")
        topics_to_create.append(NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor))


    try:
        if topics_to_create:
            admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
            print(f"Topics {[t.name for t in topics_to_create]} created successfully.")
            print("My existing topics:")
            [print(topic) for topic in admin_client.list_topics() if f"{args.topic_tag}" in topic]
        else:
            print("No topics to create.")
    except Exception as e:
        print(f"An error occurred: {e}")

    # Закриття зв'язку з клієнтом
    admin_client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--topic-tag", type=str, 
        default="ek_1", help="topic suffix. Default: 'ek_1'")

    args = parser.parse_args()
    
    main(args)