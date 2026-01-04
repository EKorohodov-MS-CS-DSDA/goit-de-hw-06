import argparse
from kafka import KafkaConsumer
from configs import kafka_config, INPUT_TOPIC_BASE, OUTPUT_TOPIC_BASE
import json

def main(args=None):
    
# Назва топіку
    topics = [f"{OUTPUT_TOPIC_BASE}_{args.topic_tag}"]

    # Створення Kafka Consumer
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password'],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',  # Зчитування повідомлень з початку
        enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
        group_id='my_consumer_group_3'   # Ідентифікатор групи споживачів
    )

    print(f"Subscribed to topic '{topics}'")

    # Обробка повідомлень з топіку
    try:
        for message in consumer:
            print("Received message:")
            print(f"[{message.topic}]: {message.value}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        consumer.close()  # Закриття consumer

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--topic-tag", type=str, 
        default="ek_1", help="topic suffix. Default: 'ek_1'")

    args = parser.parse_args()
    main(args)