#Author:sukriya
# fetches the stock information of my choice

import yfinance as yf
from datetime import datetime
from kafka import KafkaProducer
import time
import json

producer = KafkaProducer(
          bootstrap_servers ='localhost:9092',
          value_serializer=lambda x: json.dumps(x).encode('utf-8')

)
vusa = yf.Ticker("VUSA.L")

while True:
    data = vusa.history(period='1d', interval='1m')

    if not data.empty:
         price =round(data['Close'].iloc[-1], 3)
         message =  {
               "symbol": "VUSA.L",
                "price": float(price),
                  
         }
         producer.send('stock_topic', value=message)
         print(f"Sent: {message}")

    time.sleep(60)
