#author:sukriya
# fetch stock information using yfinance in kafka

from kafka import KafkaProducer
import yfinance as yf
import json
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  
)


topic = 'stock-topic'

def fetch_stock_data(stock_symbol='AAPL'):
    
    stock = yf.Ticker(stock_symbol)
    stock_info = stock.history(period="1d")  
    if not stock_info.empty:
        latest_data = stock_info.iloc[-1]  
        stock_data = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'symbol': stock_symbol,
            'price': latest_data['Close'],
            'volume': latest_data['Volume']
        }
        return stock_data
    else:
        return None

def send_stock_data_to_kafka():
    stock_symbol = 'AAPL'  
    stock_data = fetch_stock_data(stock_symbol)
    
    if stock_data:
        producer.send(topic, stock_data)
        print(f"Sent stock data: {stock_data}")
    else:
        print(f"Failed to fetch data for {stock_symbol}")

if __name__ == "__main__":
    send_stock_data_to_kafka()

