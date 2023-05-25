from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from json import loads, dumps
import numpy as np
import pymongo

city = 'Philadelphia'
client = pymongo.MongoClient("mongodb+srv://atharvak:2018065968%40VITvv@cluster0.omtiph0.mongodb.net/test")
db = client["CS-779-Project"]
collection = db[city+"_historical_hourly"]


data = []
topic = city+'_historical_hourly'
consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id= 'group1',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

consumer.poll()
consumer.seek_to_beginning()
for message in consumer:
    print(message.value)
    collection.insert_one(message.value)
    if consumer.assignment() and all(consumer.position(tp) >= consumer.end_offsets([tp])[tp] for tp in consumer.assignment()):
        # break the loop if all messages have been consumed
        break

consumer.close()