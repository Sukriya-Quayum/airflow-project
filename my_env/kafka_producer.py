#author :sukriya
# random number every 5 min and kafka producer script

from kafka import KafkaProducer
import random
import time
from datetime import datetime
import json

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'], 
    value_serializer=lambda v: json.dumps(v).encode('utf-8') 
)


topic = 'test-topic'

def generate_and_send_message():
  
    random_number = random.randint(1, 100)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    
    message = {
        'timestamp': current_time,
        'random_number': random_number
    }
    
    
    producer.send(topic, message)
    print(f"Sent message: {message}")

def main():
    
    while True:
        generate_and_send_message()
        time.sleep(2) 

if __name__ == "__main__":
    main()
