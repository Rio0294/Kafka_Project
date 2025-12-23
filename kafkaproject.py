#Importing Libraries
import requests
import json
from datetime import datetime
from kafka import KafkaProducer
from kafka import KafkaConsumer
import pandas as pd
import pyodbc 

#Fetching data from API
response = requests.get("https://financialmodelingprep.com/stable/search-name?query=apple&apikey=WKVgrPC3nHyltW8geY4ssaogMVDiWqLM")


if response.status_code == 200:
    value = response.json()    
    key = "Apple"
    print("Fetched Data successfully")
else:
    print(f"Error : {response.status_code}")
    value = None


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
    database_name = input("Enter a database name to create: ")
    connection = pyodbc.connect("Driver={SQL Server};"
                      "Server=RIYA\SQLEXPRESS;"
                      "Database=KafkaProject;"
                      "Trusted_Connection=True;")
    connection.autocommit = True
    connection.execute(f'create database {database_name}')
    print("Connected to Database")
    
except pyodbc.Error as ex:
    print("Connection failed")

#Inserting Data into SQL Server
#Creating DataFrame
df = pd.DataFrame(value)

print(df)

df.to_csv('stocks_info.csv', index= False)