# producer.py
from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    message = {
        'timestamp': time.time(),
        'sensor_id': random.randint(1, 5),
        'value': round(random.uniform(20.0, 50.0), 2)
    }
    print(f"Sending: {message}")
    producer.send('realtime-data', message)
    time.sleep(2)
