import paho.mqtt.client as mqttClient
import time
import os
import psutil

 
######################################################################## 
def on_connect(client, userdata, flags, rc):
  
    if rc == 0:
        print("Connected to broker")
        global Connected               
        Connected = True               
  
    else:
        print("Connection failed")
########################################################################
def on_publish(client,userdata,result):
    print("data published \n")
    pass
########################################################################
# Code principal  
########################################################################

Connected = False
#broker_address= "broker.hivemq.com" 
broker_address= "localhost"
port = 1883
 
client = mqttClient.Client("Python_publisher")     #create new instance
client.on_connect= on_connect                      #attach function to callback
client.on_publish = on_publish                     #attach function to callback 
client.connect(broker_address, port=port)          #connect to broker
client.loop_start()                                #start the loop
  
while Connected != True:                           #Wait for connection
    time.sleep(0.1)
 
print("maintenant je suis connecté au broker broker_address \n")
 
client.publish("Sensors/Temperature", payload="hello")
#-------------------------------------------------------------------------
try:
    while True:
        print("publishing to test/message")
        # Getting loadover 15 minutes
        load1, load5, load15 = psutil.getloadavg()
        cpu_usage = (load15/os.cpu_count()) * 100
        
        print("The CPU usage is : "+ cpu_usage)        
        message = "The CPU usage is : ".cpu_usage
        # Getting % usage of virtual_memory ( 3rd field)
		print("%RAM memory used:', psutil.virtual_memory()[2])
		message = message + "%RAM memory used:'+ psutil.virtual_memory()[2]
        client.publish(topic, payload=str(message))
        time.sleep(1)
        print("message published with success to test/message")

#-------------------------------------------------------------------------
except KeyboardInterrupt:
    print("sortir de la boucle exiting")
    client.disconnect()
    client.loop_stop()