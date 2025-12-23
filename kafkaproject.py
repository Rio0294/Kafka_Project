# import asyncio
# import websockets

# async def live_prices():
#     url = "wss://testnet.binance.vision/ws/btcusdt@aggTrade"

#     async with websockets.connect(url) as ws:
#         print("Connected to live stream")

#         count = 0
#     async for message in ws:
#         print("Live data:", message)
#         count += 1
#         if count >= 50:
#             break

#         async for message in ws:
#             print("Live data:", message)

# asyncio.run(live_prices())


# import websocket
# import json
# import threading
# import time

# WS_URL = "wss://ws.postman-echo.com/raw"

# def send_messages(ws):
#     while True:
#         fake_stock_event = {
#             "symbol": "AAPL",
#             "price": round(180 + (time.time() % 5), 2),
#             "volume": 100,
#             "timestamp": int(time.time())
#         }
#         ws.send(json.dumps(fake_stock_event))
#         time.sleep(1)

# def on_open(ws):
#     print("Connection opened")
#     threading.Thread(target=send_messages, args=(ws,)).start()

# def on_message(ws, message):
#     data = json.loads(message)
#     print("Received:", data)

# def on_error(ws, error):
#     print("Error:", error)

# def on_close(ws, close_status_code, close_msg):
#     print("Connection closed")
#     print("Status code:", close_status_code)
#     print("Message:", close_msg)

# ws = websocket.WebSocketApp(
#     WS_URL,
#     on_open=on_open,
#     on_message=on_message,
#     on_error=on_error,
#     on_close=on_close
# )

# ws.run_forever()

#////////////////////////////////////////////////////////////////////////////////////////////

import requests
import json
from datetime import datetime
from kafka import KafkaProducer
from kafka import KafkaConsumer
import pandas as pd
import pyodbc 

#Fetch data from API
response = requests.get("https://financialmodelingprep.com/stable/search-name?query=apple&apikey=WKVgrPC3nHyltW8geY4ssaogMVDiWqLM")


if response.status_code == 200:
    value = response.json()    
    key = "Apple"
    print("Fetched Data successfully")
else:
    print(f"Error : {response.status_code}")
    value = None

# print(value)

#Kakfka Producer Setup
producer = KafkaProducer(
    bootstrap_servers = '127.0.0.1:9092', 
    key_serializer = str.encode,
    value_serializer = lambda v: json.dumps(v).encode('utf-8')
)

#Kafka Consumer Setup
consumer = KafkaConsumer(
    "stocks",
    bootstrap_servers = '127.0.0.1:9092',
    key_deserializer = lambda k: k.decode('utf-8'),
    value_deserializer = lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset = 'earliest'
)

#Producer send 
producer.send("stocks", key=key, value=value)
producer.flush()
print(f"Sent: {key} → {value}")

for message in consumer:
    print(f"Received - Key: {message.key}, Value: {message.value}")
    break  

#Connect to SQL Server
try:
    # database_name = input("Enter a database name to create: ")
    connection = pyodbc.connect("Driver={SQL Server};"
                      "Server=RIYA\SQLEXPRESS;"
                      "Database=KafkaProject;"
                      "Trusted_Connection=True;")
    connection.autocommit = True
    # connection.execute(f'create database {database_name}')
    print("Connected to Database")
    
except pyodbc.Error as ex:
    print("Connection failed")

#Inserting Data into SQL Server
#Creating DataFrame
df = pd.DataFrame(value)

print(df)

df.to_csv('stocks_info.csv', index= False)