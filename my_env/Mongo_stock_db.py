
#author:sukriya
from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import stats
import json 


mongo_URI=("mongodb+srv://sbq0002user:A3muxesb@cluster0.xlemc.mongodb.net/stock_db?retryWrites=true&w=majority&appName=Cluster0")
client = MongoClient(mongo_URI)

db =client["stock_db"]
collection =db["historical_prices"]

consumer = KafkaConsumer(
      'stock_topic',
       bootstrap_servers=['localhost:9092'],
       value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def pearsons_skw(prices):
    n= len(prices)
    if n < 3:
         return 0
    mean =np.mean(prices)
    std  = np.std(prices)
    skewness = (n *np.sum([(x - mean) ** 3 for x in prices]))/((n-1)*(n-2)*(std ** 3))
    return skewness

def consume_kafka():
    for message in consumer:
        print(f"Received message: {message.value}")
        stock_db = message.value
        price = stock_db['price']


        stock_data = {
             "symbol": stock_db['symbol'],
             "price": round(price, 3)
            
}

        collection.insert_one(stock_data)
        print(f"Inserted into MongoDB: {stock_data}")

#fetch all vusa.l entries from  mongo
        all_entries = list(collection.find({"symbol": stock_db['symbol']}).sort("timestamp", 1))
        prices = [entry['price'] for entry in all_entries]
# statistical 
        if not prices:
            print("No prices data available.")
        else:
            try:
               mean = np.mean(prices)
               median = np.median(prices)
               std = np.std(prices)

               mode_data = stats.mode(prices, keepdims=True)
               mode = mode_data.mode[0]
# pearsons
               skewness = pearsons_skw(prices)

#result
               print(f"Mean price: {mean:2f}")
               print(f"Median price: {median:2f}")
               print(f"Mode price: {mode:2f}")
               print(f"Standard Deviation: {std:2f}")
               print("=" * 40)

               print(f"Pearson's Coefficient of Skewness: {skewness:2f}")
               print("=" * 40)

            except Exception as e:
               print("Stats calculate failed:", e)


#plot line

        plt.figure(figsize=(10,6))
        plt.plot(prices, marker='o', color='blue', label='VUSA.L Stock Price')
        plt.title('VUSA.L Stock Price')
        plt.xlabel('Time')
        plt.ylabel('Price')
        plt.legend()
        plt.grid(True)
        plt.show()


consume_kafka()
