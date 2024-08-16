from confluent_kafka import Producer
import json
import base64
import cv2
from datetime import date
from kafka import KafkaProducer
from json import dumps

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def produce_image(producer, topic, file_name, base64_content):
    img = cv2.imread(file_name)
    message = {
        "file_name": file_name,
        "height": img.shape[0],
        "width": img.shape[1],
        "date": date.today().strftime("%d-%m-%Y"),
        "base64_content": base64_content
    }

    producer.produce(topic, key=file_name, value=json.dumps(message), callback=delivery_report)
    

# Kafka producer configuration
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'python-producer'
}

# Create Kafka producer instance
producer = Producer(producer_conf)

# Example usage
file_name = "guitar.png"
with open(file_name, "rb") as image_file:
    base64_content = base64.b64encode(image_file.read()).decode('utf-8')

produce_image(producer, 'image', file_name, base64_content)
