import json
import requests
from kafka import KafkaConsumer

from api_producer import topic, bootstrap_server, jprint


def json_deserializer(data):
    return json.loads(data.decode('utf-8'))


def consume():
    consumer = KafkaConsumer(
        topic, bootstrap_servers=bootstrap_server, auto_offset_reset='earliest', key_deserializer=json_deserializer, value_deserializer=json_deserializer)
    # )
    # write the message to a json file
    msg_count = 0
    for msg in consumer:
        msg_count += 1
        with open('tvshows_data.json', 'a') as f:
            json.dump(msg.value, f)
        print("written msg, to tvshows_data.json".format(msg_count))
        print(msg.value)
        break


if __name__ == "__main__":
    consume()
