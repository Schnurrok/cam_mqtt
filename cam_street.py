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



TOPIC = "Lab/Test/Strasse"
BROKER_ADDRESS = "test.mosquitto.org"
PORT = 1883
QOS = 1

df = pd.DataFrame(columns =['timestamp', 'people'])
t = time.time()
t1 = time.time()
t2 = time.time()
anz_pers_street = 0 

timestamp=datetime.now().strftime("%Y_%m_%d-%I_%M")
str_timestamp = str(timestamp)

def on_message(client, userdata, message): 


    global anz_pers_street
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
        anz_pers_street += 1
        if  dif>10: 
            df = df.append({'timestamp':datetime.now(), 'people':anz_pers_street}, ignore_index=True)
            print("Anzahl der Personen (Strasse):", anz_pers_street) 
            t=time.time()
            anz_pers_street = 0
    else:
        if  dif>10: 
            df = df.append({'timestamp':datetime.now(), 'people': 0}, ignore_index=True)    
            print("Anzahl der Personen (Strasse):", anz_pers_street)  
            t=time.time()
            anz_pers_street = 0
    df.to_excel("Pers_Anz_Strasse_temp.xlsx", sheet_name ='sheet1', index = False)
        
    dif1 = time.time() - t1    
    
    if dif1 > 54000:
        df.to_excel("Pers_Anz_Strasse_Final_{}.xlsx".format(str_timestamp), sheet_name ='sheet1', index = False)
        os.remove("Pers_Anz_Strasse_temp.xlsx")
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
    
    
