from kafka.admin import KafkaAdminClient, NewTopic
from config import kafka_config

# підключення до Kafka Admin
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# імена топіків
topics = [
    "building_sensors_eugene_churylov_1",
    "temperature_alerts_eugene_churylov_1",
    "humidity_alerts_eugene_churylov_1"
]

# cтворення топіків
new_topics = [
    NewTopic(name=topics[0], num_partitions=3, replication_factor=1),
    NewTopic(name=topics[1], num_partitions=1, replication_factor=1),
    NewTopic(name=topics[2], num_partitions=1, replication_factor=1)
]

try:
    admin_client.create_topics(new_topics=new_topics, validate_only=False)
    print("Топіки успішно створені!")
except Exception as e:
    print(f"Деякі топіки вже існують або виникла помилка: {e}")

# виведення всіх топіків
print("Список усіх топіків:")
[print(topic) for topic in admin_client.list_topics() if "eugene_churylov_1" in topic]

# закриття клієнта
admin_client.close()
