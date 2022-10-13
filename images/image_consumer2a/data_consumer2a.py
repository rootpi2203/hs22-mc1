from kafka import KafkaConsumer, KafkaProducer
import json
import uuid
import pandas as pd
import csv
import time
from datetime import datetime

server1 = 'broker1:9093'
server2 = 'broker2:9095'
server3 = 'broker3:9097'

topic1 = "data_gen1"
topic2 = "data_gen2"

print(f'{datetime.now()}: config..', flush=True)

consumer2a = KafkaConsumer(topic2,
                         auto_offset_reset='earliest',
                         bootstrap_servers=[server2],
                         api_version=(0, 10),
                         value_deserializer = json.loads,
                         consumer_timeout_ms=500,
                         client_id="cunsum2a")

def consume_2(consumer, topic_name):
    # get data from Kafka
    messages = []
    for msg in consumer:
        messages.append(msg.value)
    print(f'{datetime.now()}{len(messages)} messages received on topic: {topic_name}', flush=True)

    # save received data to csv
    if len(messages) != 0:
        # append to file
        df_consumer1 = pd.DataFrame(messages)
        df_consumer1.to_csv('consum_Heizungsdaten.csv', mode='a', header=False, index=False)
        print('messages saved to file', flush=True)
        print(flush=True)

    else:
        print('no new messages - waiting for producer', flush=True)

wait_time = 30
print(f"{datetime.now()}: Waiting for producer .. {wait_time}sec", flush=True)
time.sleep(wait_time)
print(f"{datetime.now()}: start up .. ", flush=True)

while True:

    consume_2(consumer2a, topic2)
    time.sleep(4)




