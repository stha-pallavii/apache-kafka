import json
from kafka import KafkaProducer
from schemas import q1_genre_shows_count_schema, q2_shows_list_count_schema, q3_shows_weight_schema,  q4_highest_rated_shows_schema


# creating a list of schemas
schema_list = [q1_genre_shows_count_schema, q2_shows_list_count_schema, q3_shows_weight_schema,  q4_highest_rated_shows_schema]

bootstrap_server = ['localhost:9092']


q1_genre_shows_count_topic = 'topic1'
q2_shows_list_count_topic = 'topic2'
q3_shows_weight_topic = 'topic3'
q4_highest_rated_shows_topic = 'topic4'

# creating a list of topics
topic_list = [q1_genre_shows_count_topic, q2_shows_list_count_topic, q3_shows_weight_topic,\
q4_highest_rated_shows_topic]


q1_genre_shows_count_file = './output_json/q1_genre_shows_count.json'
q2_shows_list_count_file = './output_json/q2_shows_list_count.json'
q3_shows_weight_file = './output_json/q3_shows_weight.json'
q4_highest_rated_shows_file = './output_json/q4_highest_rated_shows.json'


def jprint(obj):
    text = json.dumps(obj, sort_keys=False, indent=4)
    print(text)


def json_serializer(data):
    return json.dumps(data).encode('utf-8')


def produce(producer, topic, data):
    producer.send(topic, data)
    producer.flush()


def produce_message():
    # opening individual json file for reading the data
    with open(q1_genre_shows_count_file, 'r') as file1:
        q1_genre_shows_count_json = json.load(file1)

    with open(q2_shows_list_count_file, 'r') as file2:
        q2_shows_list_count_json = json.load(file2)

    with open(q3_shows_weight_file, 'r') as file3:
        q3_shows_weight_json = json.load(file3)

    with open(q4_highest_rated_shows_file, 'r') as file4:
        q4_highest_rated_shows_json = json.load(file4)

    producer_list = []    
    try:
        for i in range(len(topic_list)):
            producer = KafkaProducer(bootstrap_servers=bootstrap_server,
                                  value_serializer=json_serializer)
            producer_list.append(producer)

    except:
        print("Error! Could not create kafka producer")
        return

#-------------------------------------------------------------------------------------------------------------------

    # creating a list of json data files 
    json_data_list = [q1_genre_shows_count_json, q2_shows_list_count_json, q3_shows_weight_json,\
    q4_highest_rated_shows_json]

#-------------------------------------------------------------------------------------------------------------------

    # producing data from each topic by each producer
    for i in range(len(topic_list)):
        for item in json_data_list[i]:
            data_to_produce = {
                "schema": schema_list[i],
                "payload": item
            }
            produce(producer_list[i], topic_list[i], data_to_produce)
        print(f'data{i+1} has successfully been produced by producer{i+1}.')


    print('Successfully produced data by producer')


if __name__ == '__main__':
    produce_message()