#author:sukriya
# consumer script for kafka producer
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='test-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Starting consumer...")


for message in consumer:
    data = message.value
    print(f"Received message at {data['timestamp']} with random number {data['random_number']}")

