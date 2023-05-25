from kafka import KafkaConsumer
import pymongo
from json import loads, dumps
import requests
from datetime import datetime, timedelta
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

api_key = "1cbdf708de8504702b9e60729dadd9f0"
city = 'Boston'

# Connect to MongoDB
client = pymongo.MongoClient("mongodb+srv://atharvak:2018065968%40VITvv@cluster0.omtiph0.mongodb.net/test")
db = client["CS-779-Project"]
collection = db[city+"_historical_hourly"]

start_date = datetime.strptime("2018-04-30","%Y-%m-%d")
end_date = datetime.strptime("2023-04-23","%Y-%m-%d")
#print(str(start_date.date()))
weather_data = pd.DataFrame(columns=['Date','Time','Temperature','wind_speed','wind_degree','wind_dir','weather_code','weather_descriptions','precip','humidity','visibility','pressure','cloudcover','heatindex','dewpoint','windchill','windgust','feelslike','uv_index'])
while start_date != end_date:
    doc = collection.find_one({"historical."+str(start_date.date())+".date":str(start_date.date())})
    #print(doc)
    for i in range(0,24):
        date = start_date
        time = doc['historical'][str(start_date.date())]['hourly'][i]['time']
        temp = doc['historical'][str(start_date.date())]['hourly'][i]['temperature']
        wind_speed = doc['historical'][str(start_date.date())]['hourly'][i]['wind_speed']
        wind_degree = doc['historical'][str(start_date.date())]['hourly'][i]['wind_degree']
        wind_dir = doc['historical'][str(start_date.date())]['hourly'][i]['wind_dir']
        weather_code = doc['historical'][str(start_date.date())]['hourly'][i]['weather_code']
        weather_descriptions = doc['historical'][str(start_date.date())]['hourly'][i]['weather_descriptions'][0]
        precip = doc['historical'][str(start_date.date())]['hourly'][i]['precip']
        humidity = doc['historical'][str(start_date.date())]['hourly'][i]['humidity']
        visibility = doc['historical'][str(start_date.date())]['hourly'][i]['visibility']
        pressure = doc['historical'][str(start_date.date())]['hourly'][i]['pressure']
        cloudcover = doc['historical'][str(start_date.date())]['hourly'][i]['cloudcover']
        heatindex = doc['historical'][str(start_date.date())]['hourly'][i]['heatindex']
        dewpoint = doc['historical'][str(start_date.date())]['hourly'][i]['dewpoint']
        windchill = doc['historical'][str(start_date.date())]['hourly'][i]['windchill']
        windgust = doc['historical'][str(start_date.date())]['hourly'][i]['windgust']
        feelslike = doc['historical'][str(start_date.date())]['hourly'][i]['feelslike']
        uv_index = doc['historical'][str(start_date.date())]['hourly'][i]['uv_index']
        weather_data.loc[len(weather_data.index)] = [date,time,temp,wind_speed,wind_degree,wind_dir,weather_code,weather_descriptions,precip,humidity,visibility,pressure,cloudcover,heatindex,dewpoint,windchill,windgust,feelslike,uv_index]

        #print(doc['historical'][str(start_date.date())]['hourly'][i]['temperature'])
    start_date += timedelta(days=1)
#print(weather_data)

collection = db[city+"_current"]
collection.drop()
collection = db[city+"_current"]
topic = city+'_current'
consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id= 'group1',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

consumer.poll()
consumer.seek_to_beginning()
for message in consumer:
    collection.insert_one(message.value)
    if consumer.assignment() and all(consumer.position(tp) >= consumer.end_offsets([tp])[tp] for tp in consumer.assignment()):
        # break the loop if all messages have been consumed
        break

consumer.close()

cursor = collection.find()
current_data = pd.DataFrame(columns=['Time','Temperature','wind_speed','wind_degree','wind_dir','weather_code','weather_descriptions','precip','humidity','visibility','pressure','cloudcover','feelslike','uv_index'])
for doc in cursor:
    obs_time = doc['current']['observation_time']
    temp = doc['current']['temperature']
    wind_speed = doc['current']['wind_speed']
    wind_degree = doc['current']['wind_degree']
    wind_dir = doc['current']['wind_dir']
    weather_code = doc['current']['weather_code']
    weather_descriptions = doc['current']['weather_descriptions'][0]
    precip = doc['current']['precip']
    humidity = doc['current']['humidity']
    visibility = doc['current']['visibility']
    pressure = doc['current']['pressure']
    cloudcover = doc['current']['cloudcover']
    feelslike = doc['current']['feelslike']
    uv_index = doc['current']['uv_index']
    current_data.loc[len(current_data.index)] = [obs_time,temp,wind_speed,wind_degree,wind_dir,weather_code,weather_descriptions,precip,humidity,visibility,pressure,cloudcover,feelslike,uv_index]
#print(current_data)

weather_data = weather_data[['Temperature','wind_speed','wind_degree','wind_dir','weather_code','weather_descriptions','precip','humidity','visibility','pressure','cloudcover','feelslike','uv_index']]
current_data = current_data[['Temperature','wind_speed','wind_degree','wind_dir','weather_code','weather_descriptions','precip','humidity','visibility','pressure','cloudcover','feelslike','uv_index']]
merged_data = pd.concat([weather_data,current_data],ignore_index=True)
print(merged_data)
merged_data.drop(merged_data.tail(32).index,inplace=True)
# print(merged_data['weather_descriptions'].drop_duplicates())

X = merged_data[['Temperature','wind_speed','wind_degree','precip','humidity','visibility','pressure','cloudcover','feelslike','uv_index']]
Y = merged_data[['weather_descriptions']]

model = LogisticRegression(multi_class='multinomial',solver='lbfgs',max_iter=1000)
model.fit(X,Y)

url = f"http://api.weatherstack.com/current?access_key={api_key}&query={city}"
response = requests.get(url)
if response.status_code == 200:
    weather_current = response.json()
    weather_current_df = pd.DataFrame(columns=['Temperature','wind_speed','wind_degree','wind_dir','weather_code','weather_descriptions','precip','humidity','visibility','pressure','cloudcover','feelslike','uv_index'])
    temp = weather_current['current']['temperature']
    wind_speed = weather_current['current']['wind_speed']
    wind_degree = weather_current['current']['wind_degree']
    wind_dir = weather_current['current']['wind_dir']
    weather_code = weather_current['current']['weather_code']
    weather_descriptions = weather_current['current']['weather_descriptions'][0]
    precip = weather_current['current']['precip']
    humidity = weather_current['current']['humidity']
    visibility = weather_current['current']['visibility']
    pressure = weather_current['current']['pressure']
    cloudcover = weather_current['current']['cloudcover']
    feelslike = weather_current['current']['feelslike']
    uv_index = weather_current['current']['uv_index']
    weather_current_df.loc[len(weather_current_df.index)] = [temp,wind_speed,wind_degree,wind_dir,weather_code,weather_descriptions,precip,humidity,visibility,pressure,cloudcover,feelslike,uv_index]
    y_pred = model.predict(weather_current_df[['Temperature','wind_speed','wind_degree','precip','humidity','visibility','pressure','cloudcover','feelslike','uv_index']])
    print('Predicted:',y_pred)
    print('Actual:',weather_current_df[['weather_descriptions']])