import paho.mqtt.client as mqtt
import time
import os
import pymongo
import json
import ast



myclient = pymongo.MongoClient("mongodb://localhost:27017")
mydb = myclient["mydatabase"]
mycol = mydb["lab_4"]
dblist = myclient.list_database_names()

if "mydatabase" in dblist:
  print("The database exists.")


#=======================




########################################################################
def on_connect(client, userdata, flags, rc):
    if rc == 0:
         print("Connected to broker")
         global Connected                
         Connected = True               
    else:
         print("Connection failed")
########################################################################



def on_message(client, userdata, message):
    print("message received " ,str(message.payload.decode("utf-8")))
    print("message topic=",message.topic)
    print("message qos=",message.qos)
    print("message retain flag=",message.retain)

    
    res=str(message.payload.decode("utf-8"))
    result = ast.literal_eval(res)
    x = mycol.insert_one(result)
   

    ## partie injection des données dans Kafka
########################################################################
Connected = False
#broker_address= "broker.hivemq.com" 
 
broker_address="localhost"
port = 1883                   
 
print("creating new instance")
client = mqtt.Client("python_test")
client.on_message=on_message          #attach function to callback
client.on_connect=on_connect
print("connecting to broker")
client.connect(broker_address, port)  #connect to broker
client.loop_start()                   #start the loop
 
while Connected != True:              #Wait for connection
    time.sleep(0.1)
 
print("Subscribing to topic","test")
client.subscribe("test")
 
#------------------------------------------------------------------------- 
try:
    while True: 
        time.sleep(1)
#------------------------------------------------------------------------- 
except KeyboardInterrupt:
    print("exiting")
    client.disconnect()
    client.loop_stop()