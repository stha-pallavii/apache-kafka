## Installation and set-up:
    • download zip file from https://www.confluent.io/installation/
    • extract to Home (~/) or any other location
    • there should be a folder confluent-7.2.2 

### Contents of confluent folder:
#### 1. bin
    • contains all executable files
    • starting part of all kafka commands like
        ◦ zookeeper-server-start
        ◦ zookeeper-server-stop
        ◦ kafka-server-start
        ◦ kafka-server-stop
        ◦ kafka-topics
        ◦ kafka-console-producer
        ◦ kafka-console-consumer
        ◦ kafka-consumer-groups, etc.
        
#### 2. etc
    • Inside etc folder, there is kafka folder
    • contains properties of kafka (configuration properties)
        ◦ server.properties - properties needed to start kafka server 
        ◦ zookeeper-properties -  properties needed to start zookeeper
        ◦ producer-properties – properties to configure producers
        ◦ consumer.properties - properties to configure consumers, etc……..
-- when we work on cluster, we may need to configure these properties
-- but for single system, no need to configure

#### 3. share
    • share → java → kafka folder
    • contains jar files
    • jar files will be used in kafka-connect
    • if we wanna add new jar files, we need to add it to this folder
    
## After installation:
	fish_add_path <full_path_to_confluent_folder>/bin/
In my case, the folder is in Home directory, so
    fish_add_path ~/confluent-7.2.2/bin/
    
## Start Kafka
### Start the zookeeper server
Zookeeper is responsible to monitor the kafka cluster and co-ordinate with each broker. So, we need to start the zookeeper server before starting kafka server.
Server can be started only after providing the location of zookeeper and server properties

    • zookeeper-server-start <path to zookeeper.properties file>

    • zookeeper-server-start ~/confluent-7.2.2/etc/kafka/zookeeper.properties 

    • zookeeper-server-start ~/confluent-7.2.2/etc/kafka/zookeeper.properties & disown
      (This starts the zookeeper server in the background → you can keep coding in the same terminal → to close, zookeeper-server-stop )

### Start the kafka server
    • kafka-server-start <location to server.properties>


    • kafka-server-start ~/confluent-7.2.2/etc/kafka/server.properties 

    • kafka-server-start ~/confluent-7.2.2/etc/kafka/server.properties & disown
      (This starts the zookeeper server in the background → you can keep coding in the same terminal → to close, kafka-server-stop )

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

## Basic Commands of Kafka

### Topic
#### 1. See list of existing topics
        kafka-topics --bootstrap-server localhost:9092 --list
       
#### 2. Create new topic  (name = topic name)
       kafka-topics --bootstrap-server localhost:9092 --create --topic name

#### 3. Delete a topic
       kafka-topics --bootstrap-server localhost:9092 --delete --topic name

#### 4. Specify no. of partitions while creating topic
       kafka-topics --bootstrap-server localhost:9092 --create --topic name --partitions number
       
       eg: kafka-topics --bootstrap-server localhost:9092 --create --topic demo_topic --partitions 3

#### 5. Get detailed information (describe) of a Topic
       kafka-topics --bootstrap-server localhost:9092 --describe --topic name

	(Topic name, TopicId, PartitionCount, ReplicationFactor,
	 for each partition, leader broker id, replica broker id, etc)

#### 6. Specify replication factor while creating topic
       kafka-topics --bootstrap-server localhost:9092 --create --topic name --partitions number --replication-factor no

(For this to work, we should have more brokers than the replication-factor\
 maximum replication-factor = n – 1   (n = no. of brokers)


### Producer and Consumer
#### 1. Create a producer (Kafka Producer)
	kafka-console-producer --bootstrap-server localhost:9092 --topic name 
    (The producer can now publish data to the topic)

#### 2. Create consumer (Kafka Consumer)
	kafka-console-consumer --bootstrap-server localhost:9092 --topic name       
(creates kafka consumer and retrieves only new messages)
(The consumer can read the data published by producer)

##### Note:
- After creating producer, we can just type the message and enter, line by line
- But data published by the producer before creating the consumer won’t be read by consumer
- After creating consumer, if we add new data by producer, then it can be seen in consumer console as well
- To get the consumer read the data from the beginning as well, append --from-beginning to consumer creation command

	kafka-console-consumer --bootstrap-server localhost:9092 --topic name --from-beginning 

(creates kafka consumer and retrieves all messages – but previously published messages may not be read in order)

#### 3. Consumer Group – Create a new consumer group (Kafka Consumer Group)
Assign a consumer to a consumer group (while creating consumer)

	kafka-console-consumer –bootstrap-server localhost:9092 --topic name --group groupname

	kafka-console-consumer –bootstrap-server localhost:9092 --topic name --from-beginning --group groupname

Consumers in a consumer-group have same properties as consumers not assigned to a group (read messages in the same way)

#### Add new consumer to an existing consumer group
	kafka-console-consumer –bootstrap-server localhost:9092 --topic name --from-beginning --group groupname 

- (Here groupname should be existing group in which we wanna add our new consumer)
- After running this command,  go back to producer terminal and add new data
- Check both consumer terminals, data is randomly divided between these consumers
- describe the consumer group to see which partition is being read by which consumer-id


#### 4. See a list of existing consumer groups
	kafka-consumer-groups --bootstrap-server localhost:9092 --list

#### 5.a. Get details about a particular consumer group
	kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group groupname

- group, topic, partition, current-offset, log-end-offset, lag, consumer-id, host, client-id

- current-offset = the last committed offset of the consumer (upto which offset of the partition has consumer finished reading data?)

- log-end-offset = highest offset of the partition (upto which offset of the partition has the producer published the message?)

- lag = by how many offsets is the consumer lagging behind?
  Lag = log-end-offset - current-offset

#### 5.b. Describe all consumer groups
	kafka-consumer-groups --bootstrap-server localhost:9092 --all-groups --describe

#### 6. Delete a consumer group
	kafka-consumer-groups --bootstrap-server localhost:9092 --delete --group groupname

## Stop the server:
	Ctrl + C
	exit

#### If you have run server in background,
	kafka-server-stop
	zookeeper-server-stop

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

## Kafka Connect
Kafka connect is a component of Apache Kafka which helps in streaming data between Kafka and other data systems.
2 types of connectors:
    a) source connector: import/ingest data from source systems into kafka topics
    b) sink connector: export/deliver data from kafka topics to target systems

Source systems → Kafka Connect → Kafka Cluster → Kafka Connect → Target systems
#### 1. Install JDBC connector: #latest version	
       	confluent-hub install confluentinc/kafka-connect-jdbc:latest

#### 2. Start Kafka Connect
	(After starting zookeeper and kafka server, start connect-distributed and schema-registry)
	connect-distributed <path to confluent folder>/etc/kafka/connect-distributed.properties

	connect-distributed ~/confluent-7.2.2/etc/kafka/connect-distributed.properties


#### 3. Start Schema Registry
    • schema-registry-start <locationTo schema-registry.properties>


## Kafka Source Connector Configuration




## Kafka Sink Connector Configuration




------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
