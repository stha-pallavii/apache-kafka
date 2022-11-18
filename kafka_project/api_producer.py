import requests
import json
from kafka import KafkaProducer

bootstrap_server = ['localhost:9092']
topic = 'tvshows'
tv_shows_url = "https://api.tvmaze.com/shows"


def jprint(obj):
    text = json.dumps(obj, sort_keys=False, indent=4)
    print(text)


def json_serializer(data):
    return json.dumps(data).encode('utf-8')


def produce(producer, topic, data):
    producer.send(topic, data)
    producer.flush()


def produce_message():
    response = requests.get(tv_shows_url)
    if response.status_code != 200:
        print(
            f"Error in reading data from url\nError: {response.status_code}, Message: {response.reason}")
        return
    try:
        producer = KafkaProducer(bootstrap_servers=bootstrap_server,
                                 value_serializer=json_serializer, key_serializer=json_serializer)
    except:
        print("Error! Could not create kafka producer")
        return

    produce(producer, topic, response.json())
    print("Successfully produced to topic {} message:".format(
        topic))
    jprint(response.json())


if __name__ == '__main__':
    produce_message()
