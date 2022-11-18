# Kafka Final Project
[Project pipeline diagram]()

## Steps:
1. Find [API](https://api.tvmaze.com/shows)

2. Create kafka [producer]() and produce data from the API into kafka topic

3. Create kafka [consumer](), consume data from the topic, and dump the [data]() as [json file]()

4. Read the json data into pyspark dataframe and conduct pre-processing and transformations - [Notebook]()
	4.a. Preprocessing: Drop columns, Bring all fields to same level by using explode() on arrays and getItem() on struct type data
	4.b. Transformation: Carry out four transformations (as task 1,2,3,4)
	4.c. Dump the transformed dataframes into [json files]()
	
5. Create [schema]() for each transformed json file

6. Create [producers]() for producing data from each json file into individual kafka topics

7. Create [sink connectors]() for consuming data from each topic into [database tables]() in PostgreSQL.
	
