kafka_config = {
    "bootstrap_servers": ['77.81.230.104:9092'],
    "username": 'admin',
    "password": 'VawEzo1ikLtrA8Ug8THa',
    "security_protocol": 'SASL_PLAINTEXT',
    "sasl_mechanism": 'PLAIN',
}

# Build the JAAS config string after the dict values are available
kafka_config['kafka.sasl.jaas.config'] = (
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    f"username=\"{kafka_config['username']}\" password=\"{kafka_config['password']}\";"
)

ALERTS_CONDITIONS_FILE = './alerts_conditions.csv'
INPUT_TOPIC_BASE = 'building_sensors'
OUTPUT_TOPIC_BASE = 'alerts'