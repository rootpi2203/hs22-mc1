from kafka import KafkaConsumer, KafkaProducer
import json
import uuid
import pandas as pd
import csv
import time

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
        print(f'Message published successfully to topic:  {topic_name}.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

def produce_xy(producer, topic_name, sleep_hz):
    with open('gen1_sinus_data.csv') as f:
        reader = [line.split() for line in f]
        for i, line in enumerate(reader):
            print(f'index {i}, data: {line}')
            message = json.dumps({'index':i, 'data': line})
            publish_message(producer, topic_name, str(uuid.uuid4()), message)
            time.sleep(sleep_hz)


server1 = 'broker1:9093'
server2 = 'broker2:9095'
server3 = 'broker3:9097'
topic = "data_gen1"

producer1 = connect_kafka_producer(server1)

hz = 0.5
produce_xy(producer1, topic, hz)
