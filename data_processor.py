import json
from kafka import KafkaConsumer, KafkaProducer
from config import kafka_config

# топіки
input_topic = "building_sensors_eugene_churylov_1"
temperature_alerts_topic = "temperature_alerts_eugene_churylov_1"
humidity_alerts_topic = "humidity_alerts_eugene_churylov_1"

# налаштування Kafka Consumer
consumer = KafkaConsumer(
    input_topic,
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

# налаштування Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Підписка на топік:", input_topic)

try:
    for message in consumer:
        sensor_data = message.value
        sensor_id = sensor_data.get("sensor_id")
        temperature = sensor_data.get("temperature")
        humidity = sensor_data.get("humidity")
        timestamp = sensor_data.get("timestamp")

        # перевірка даних
        if temperature > 40:
            alert_message = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "temperature": temperature,
                "alert": "Температура перевищує 40°C!"
            }
            producer.send(temperature_alerts_topic, value=alert_message)
            print(f"Температурне сповіщення: {alert_message}")

        if humidity > 80 or humidity < 20:
            alert_message = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "humidity": humidity,
                "alert": "Вологість поза межами допустимих значень (20-80%)!"
            }
            producer.send(humidity_alerts_topic, value=alert_message)
            print(f"Сповіщення про вологість: {alert_message}")

except KeyboardInterrupt:
    print("\nЗавершення обробки.")
finally:
    consumer.close()
    producer.close()
