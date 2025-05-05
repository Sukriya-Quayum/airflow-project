#Author:sukriya
# consumer script stock information

from kafka import KafkaConsumer
from datetime import datetime
import json


consumer = KafkaConsumer(
      'stock_topic',
      bootstrap_servers='localhost:9092',
      auto_offset_reset='earliest',
      enable_auto_commit=True,
      group_id='stock-consumer-group'
)

print("Waiting for stock messages...")


for message in consumer:
    stock_db = json.loads(message.value.decode('utf-8'))
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Received: {stock_db}")
