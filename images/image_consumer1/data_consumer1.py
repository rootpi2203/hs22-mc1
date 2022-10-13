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

consumer1 = KafkaConsumer(topic1,
                         auto_offset_reset='earliest',
                         bootstrap_servers=[server1],
                         api_version=(0, 10),
                         value_deserializer = json.loads,
                         #consumer_timeout_ms=1000,
                         client_id='consum1')

def consume_1(consumer, topic_name):
    # get data from Kafka
    for msg in consumer:
        print(f'{datetime.now()}:message received on topic: {topic_name}', flush=True)
        print(f'index: {msg.value["index"]} - data:{msg.value["data"]} ', flush=True)

        # save received data to csv
        if len(msg.value) != 0:
            # write new file when index 0 -> file start
            if msg.value['index'] == 0:
                df_consumer1 = pd.DataFrame(msg.value)
                df_consumer1.to_csv('consum_sinus_data.csv', mode='w', index=False)
                print('message saved in NEW file', flush=True)
                print(flush=True)
            # append to file
            else:
                df_consumer1 = pd.DataFrame(msg.value)
                df_consumer1.to_csv('consum_sinus_data.csv', mode='a', header=False, index=False)
                print('message saved to file', flush=True)
                print(flush=True)

        else:
            print('no new messages - waiting for producer')

wait_time = 30
print(f"{datetime.now()}: Waiting for producer .. {wait_time}sec", flush=True)
time.sleep(wait_time)
print(f"{datetime.now()}: start up .. ", flush=True)

consume_1(consumer1, topic1)

