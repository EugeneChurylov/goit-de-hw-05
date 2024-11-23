import json
from kafka import KafkaConsumer
from config import kafka_config

# топіки для підписки
topics = ["temperature_alerts_eugene_churylov_1", "humidity_alerts_eugene_churylov_1"]

# налаштування Kafka Consumer
consumer = KafkaConsumer(
    *topics,
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

print("Підписка на топіки:", topics)

try:
    for message in consumer:
        alert = message.value
        topic = message.topic
        print(f"[{topic}] Сповіщення: {alert}")
except KeyboardInterrupt:
    print("\nЗавершення роботи.")
finally:
    consumer.close()
