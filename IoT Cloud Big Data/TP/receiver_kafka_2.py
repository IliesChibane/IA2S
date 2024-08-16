from confluent_kafka import Consumer, KafkaError
import json
import base64
import os
from PIL import Image
import io
import pymongo

import matplotlib.pyplot as plt
import matplotlib.image as mpimg

atlas = "mongodb+srv://ilieschibane:pokemon@cluster0.1rpdeqo.mongodb.net/?retryWrites=true&w=majority"
localhost = "mongodb://localhost:27017"

myclient = pymongo.MongoClient(atlas)
mydb = myclient["mydatabase"]
mycol = mydb["image_test"]
# mycol.drop()
# mycol = mydb["image_test"]

dblist = myclient.list_database_names()

if "mydatabase" in dblist:
 print("The database exists.")
else : print("The database does not exist.")

def show_image(file_path):
    # Load and display the image using Matplotlib
    img = mpimg.imread(file_path)
    plt.imshow(img)
    plt.axis('off')  # Turn off axis labels
    plt.show()


def save_image(file_name, base64_content, save_directory='img_received'):
    # Create the save directory if it doesn't exist
    os.makedirs(save_directory, exist_ok=True)

    # Construct the full path to save the image
    full_path = os.path.join(save_directory, file_name)

    # Decode base64
    image_bytes = base64.b64decode(base64_content)

    # Save the image to a file
    with open(full_path, "wb") as image_file:
        image_file.write(image_bytes)

    print(f"Image saved: {full_path}")

def consume_image(consumer, topic):
    consumer.subscribe([topic])

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        try:
            data = json.loads(msg.value())
            file_name = data["file_name"]
            height = data["height"]
            width = data["width"]
            date = data["date"]

            mycol.insert_one(data)

            print(f"Image received and saved in the database: {file_name}")
            print(f"Height: {height}, Width: {width}, Date: {date}")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
        except KeyError as e:
            print(f"Missing key in JSON: {e}")

# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'python-consumer',
    'auto.offset.reset': 'earliest'
}

# Create Kafka consumer instance
consumer = Consumer(consumer_conf)

# Example usage
consume_image(consumer, 'image')

