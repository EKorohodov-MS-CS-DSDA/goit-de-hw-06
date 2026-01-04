import argparse
from kafka import KafkaProducer
from configs import kafka_config, INPUT_TOPIC_BASE, OUTPUT_TOPIC_BASE
import json
import uuid
import time
import random
from datetime import datetime, timezone

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def main(args=None):
    # Назва топіку
    topic_name = f'{INPUT_TOPIC_BASE}_{args.topic_tag}'
    
    sensor_id = str(uuid.uuid4())

    send_interval = args.send_interval  # Інтервал відправлення повідомлень в секундах
    max_messages = args.max_messages  # Максимальна кількість повідомлень для відправки
    i = 0
    while True:
        if max_messages != 0 and i >= max_messages:
            break
        i += 1
        # Відправлення повідомлення в топік
        try:
            data = {
                "sensor_id": sensor_id, # Ідентифікатор сенсора
                "timestamp": datetime.now().isoformat(),  # Часова мітка
                "temperature": random.randint(20, 50),  # Випадкове значення
                "humidity": random.randint(10, 90)  # Випадкове значення
            }

            producer.send(topic_name, value=data)
            producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
            print(f"[{i}] ['{topic_name}'] Sensor {data.get('sensor_id')}: Temperature: {data.get('temperature')}, Humidity: {data.get('humidity')}.")
            time.sleep(send_interval)
        except Exception as e:
            print(f"An error occurred: {e}")

    producer.close()  # Закриття producer

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--topic-tag", type=str, 
        default="ek_1", help="topic suffix. Default: 'ek_1'")
    parser.add_argument("--max-messages", type=int, 
        default=20, help="Maximum number of messages to send. 0 = inf. Default: 10.")
    parser.add_argument("--send-interval", type=int,
        default=1, help="Interval between messages in seconds. Default: 1.")

    args = parser.parse_args()
    
    main(args)
