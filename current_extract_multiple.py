import requests
import json
from kafka import KafkaProducer
import time
from datetime import datetime, timedelta

api_key = "1cbdf708de8504702b9e60729dadd9f0"
cities = ['Cleveland','Boston','Detroit','Chicago','Houston','Phoenix','Philadelphia','Atlanta','Cincinnati','Dallas']

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

while True:
    for city in cities:
        url = f"http://api.weatherstack.com/current?access_key={api_key}&query={city}"
        response = requests.get(url)
        if response.status_code == 200:
            weather_data = response.json()
            message = json.dumps(weather_data)
            producer.send(city+'_current', message.encode('utf-8'))
        else:
            print(f"Error calling Weatherstack API. Status code: {response.status_code}")
    time.sleep(15*60)
