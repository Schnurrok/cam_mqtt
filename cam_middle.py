import paho.mqtt.client as mqtt
import time
import json 
import pandas as pd
import datetime
from datetime import datetime 
from threading import Timer
import threading
from time import sleep
import openpyxl
import sys
import os



TOPIC = "Lab/Test/Mitte"
BROKER_ADDRESS = "test.mosquitto.org"
PORT = 1883
QOS = 1

df = pd.DataFrame(columns =['timestamp', 'people'])
t = time.time()
t1 = time.time()
t2 = time.time()
anz_pers_middle = 0 

timestamp=datetime.now().strftime("%d_%m_%Y-%I_%M")
str_timestamp = str(timestamp)

def on_message(client, userdata, message): 


    global anz_pers_middle
    global df
    global t
    global t1
    global t2
    global dif
    global dif1
    
    msg = str(message.payload.decode("utf-8"))
    
    json_msg = json.loads(msg)
    dif= time.time()-t
    
     
    if 'heatmapEvent' in json_msg.keys():
        anz_pers_middle += 1
        if  dif>10: 
            df = df.append({'timestamp':datetime.now(), 'people':anz_pers_middle}, ignore_index=True)
            print("Anzahl der Personen:", anz_pers_middle) 
            t=time.time()
            anz_pers_middle = 0
    else:
        if  dif>10:
            df = df.append({'timestamp':datetime.now(), 'people': 0}, ignore_index=True)    
            print("Anzahl der Personen:", anz_pers_middle)
            t=time.time()
            anz_pers_middle = 0
    
    df.to_excel("Pers_Anz_Mitte_temp.xlsx", index = False)
        
    dif1 = time.time() - t1    
    
    if dif1 > 120:
        df.to_excel("Pers_Anz_Mitte_Final_{}.xlsx".format(str_timestamp), index = False)
        os.remove("Pers_Anz_Mitte_temp.xlsx")
        sys.exit("Zeit um")

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT Broker: " + BROKER_ADDRESS)
    client.subscribe(TOPIC)


if __name__ == "__main__":
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER_ADDRESS, PORT)
    client.loop_forever()
    
    
