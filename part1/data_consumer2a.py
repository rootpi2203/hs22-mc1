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

consumer2a = KafkaConsumer(topic2,
                         auto_offset_reset='earliest',
                         bootstrap_servers=[server2],
                         api_version=(0, 10),
                         value_deserializer = json.loads,
                         consumer_timeout_ms=1000)

def consume_2(consumer, topic_name):
    # get data from Kafka
    messages = []
    for msg in consumer:
        #print(msg.key.decode("utf-8"), msg.value)
        messages.append(msg.value)
    print(f'{len(messages)} messages received on topic: {topic_name}')

    # save received data to csv
    if len(messages) != 0:
        # append to file
        df_consumer1 = pd.DataFrame(messages)
        df_consumer1.to_csv('../data/consum_Heizungsdaten.csv', mode='a', header=False, index=False)
        print('messages saved to file')
        return True
    else:
        print('no new messages - producer has stopped')
        return False


while True:
    new_data = consume_2(consumer2a, topic2)
    if not new_data:
        break
    time.sleep(4)


