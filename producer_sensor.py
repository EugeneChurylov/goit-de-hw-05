import time
import random
from kafka import KafkaProducer
import json
from config import kafka_config

# ідентифікатор датчика (випадковий, але постійний для одного запуску)
sensor_id = random.randint(1000, 9999)

# Налаштування Kafka продюсера
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # сериалізація JSON
)

# ім'я топіка
topic_name = "building_sensors_eugene_churylov_1"

print(f"Початок роботи датчика ID: {sensor_id}")

try:
    while True:
        # генерація випадкових даних
        temperature = random.uniform(25, 45)
        humidity = random.uniform(15, 85)
        timestamp = time.time()

        # формування повідомлення
        message = {
            "sensor_id": sensor_id,
            "timestamp": timestamp,
            "temperature": round(temperature, 2),
            "humidity": round(humidity, 2)
        }

        # відправка повідомлення у топік
        producer.send(topic_name, value=message)
        print(f"Дані відправлено: {message}")

        # затримка перед наступним повідомленням
        time.sleep(5)
except KeyboardInterrupt:
    print("\nЗавершення роботи датчика.")
finally:
    producer.close()
