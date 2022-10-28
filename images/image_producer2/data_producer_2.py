from kafka import KafkaConsumer, KafkaProducer
import json
import uuid
import pandas as pd
import csv
import time
from datetime import datetime
import socket  #socket.gethostname()

def connect_kafka_producer(servers):
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=servers, api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print(f'Message published successfully to topic: {topic_name}.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

def produce_xy(producer, topic_data, topic_perf, sleep_hz, hostname):
    with open('gen2_Heizungsdaten.csv') as f:
        next(f)
        for i, line in enumerate(f):
            print(f'index {i}')
            message = json.dumps({'data': str(line)})
            perf_message = json.dumps({'time': str(datetime.now()), 'host': str(hostname), 'mess_num': str(1)})
            print(message)

            # publish data
            publish_message(producer, topic_data, str(uuid.uuid4()), message)
            # publish performance
            publish_message(producer, topic_perf, str(uuid.uuid4()), perf_message)

            time.sleep(sleep_hz)


server1 = 'broker1:9093'
server2 = 'broker2:9095'
server3 = 'broker3:9097'

topic = "data_gen2"
topic_perf = "performance"
hostname = socket.gethostname()

producer2 = connect_kafka_producer(server2)

hz = 0.5
produce_xy(producer2, topic, topic_perf, hz, hostname)
