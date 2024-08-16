from kafka import KafkaProducer
import json
from json import dumps

p = KafkaProducer(bootstrap_servers=['localhost:9092'],
              api_version=(0,11,5),
              value_serializer=lambda x: dumps(x).encode('utf-8'))

data = {'name': 'roscoe'}

p.send('Tutorial2.pets', value = data)

p.flush()

print("Message sent to Kafka Topic successfully!")