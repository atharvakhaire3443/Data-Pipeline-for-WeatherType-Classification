import requests
import json
from kafka import KafkaProducer
import time
from datetime import datetime, timedelta

api_key = "1cbdf708de8504702b9e60729dadd9f0"
cities = ['Cleveland','Boston','Detroit','Chicago','Houston','Phoenix','Philadelphia','Atlanta','Cincinnati','Dallas']
start_date = datetime.now() - timedelta(days=365*5)
end_date = datetime.now()

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

for city in cities:
    while start_date <= end_date:
        date_str = start_date.strftime("%Y-%m-%d")
        url = f"http://api.weatherstack.com/historical?access_key={api_key}&query={city}&historical_date={date_str}&hourly=1&interval=1"
        response = requests.get(url)
        if response.status_code == 200:
            weather_data = response.json()
            message = json.dumps(weather_data)
            producer.send(city+'_historical_hourly', message.encode('utf-8'))
        else:
            print(f"Error calling Weatherstack API for {date_str}. Status code: {response.status_code}")
        start_date += timedelta(days=1)
    start_date = datetime.now() - timedelta(days=365*5)
