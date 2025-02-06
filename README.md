### Apache Kafka
This repository contains the codes that I used while learning Apache Kafka.

View the [Kafka Project] (https://github.com/stha-pallavii/apache-kafka/tree/kafka_project/kafka_project) branch to see the project I built using Apache Kafka
[Project pipeline diagram](https://github.com/stha-pallavii/apache-kafka/blob/ccbc3d464b10fa8ec572aa6c7c1dcad7e3d37f63/kafka_project/kafka_project_pipeline_diagram)

## Steps:
1. Find [API](https://api.tvmaze.com/shows)

2. Create kafka [producer](https://github.com/stha-pallavii/apache-kafka/blob/kafka_project/kafka_project/api_producer.py) and produce data from the API into kafka topic

3. Create kafka [consumer](https://github.com/stha-pallavii/apache-kafka/blob/kafka_project/kafka_project/api_consumer.py), consume data from the topic, and dump the [data](https://github.com/stha-pallavii/apache-kafka/tree/kafka_project/kafka_project/data) as [json file](https://github.com/stha-pallavii/apache-kafka/blob/kafka_project/kafka_project/data/tvshows_data.json)

4. Read the json data into pyspark dataframe and conduct pre-processing and transformations - [Notebook](https://github.com/stha-pallavii/apache-kafka/blob/kafka_project/kafka_project/kafka_spark_transformation_preprocessing.ipynb)
	4.a. Preprocessing: Drop columns, Bring all fields to same level by using explode() on arrays and getItem() on struct type data
	4.b. Transformation: Carry out four transformations (as task 1,2,3,4)
	4.c. Dump the transformed dataframes into [json files](https://github.com/stha-pallavii/apache-kafka/tree/kafka_project/kafka_project/output_json)
	
5. Create [schemas](https://github.com/stha-pallavii/apache-kafka/blob/kafka_project/kafka_project/schemas.py) for all transformed json files

6. Create [producers](https://github.com/stha-pallavii/apache-kafka/blob/kafka_project/kafka_project/spark_producer.py) for producing data from each json file into individual kafka topics

7. Create [sink connectors](https://github.com/stha-pallavii/apache-kafka/blob/kafka_project/kafka_project/sink_connectors_configuration%20copy.txt) for consuming data from each topic into [database tables](https://github.com/stha-pallavii/apache-kafka/tree/kafka_project/kafka_project/postgres_sink_tables) in PostgreSQL.
