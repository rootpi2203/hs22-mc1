from kafka import KafkaConsumer, KafkaProducer
import json
import uuid
import pandas as pd
import csv
import time

server1 = 'broker1:9093'
server2 = 'broker2:9095'
server3 = 'broker3:9097'

topic1 = "data_gen1"
topic2 = "data_gen2"

print('config..')

consumer1 = KafkaConsumer(topic1,
                         auto_offset_reset='earliest',
                         bootstrap_servers=[server1],
                         api_version=(0, 10),
                         value_deserializer = json.loads,
                         consumer_timeout_ms=1000,
                         client_id='joe')


def consume_1(consumer, topic_name):
    # get data from Kafka
    messages = []
    for msg in consumer:
        #print(msg.key.decode("utf-8"), msg.value)
        messages.append(msg.value)
    print(f'{len(messages)} messages received on topic: {topic_name}')

    # save received data to csv
    if len(messages) != 0:
        # write new file
        if messages[0]['index'] == 0:
            df_consumer1 = pd.DataFrame(messages)
            df_consumer1.to_csv('consum_sinus_data.csv', mode='w', index=False)
            print('messages saved in new file')
        # append to file
        else:
            df_consumer1 = pd.DataFrame(messages)
            df_consumer1.to_csv('consum_sinus_data.csv', mode='a', header=False, index=False)
            print('messages saved to file')
        return True
    else:
        print('no new messages - waiting for producer')
        return False

#wait_time = 60
#print(f"Waiting for producer .. {wait_time}sec")
#time.sleep(wait_time)

#consume_1(consumer1, topic1)


wait_start_up_producer = True
wait_time = 30
while True:
    if wait_start_up_producer:
        print(f"Waiting for producer .. {wait_time}sec")
        time.sleep(wait_time)
        print("start up..")
        wait_start_up_producer = False

    new_data = consume_1(consumer1, topic1)
    #if not new_data:
        #break
    time.sleep(5)
