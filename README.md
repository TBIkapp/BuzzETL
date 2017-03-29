# BuzzETL
created by 
- Raúl Marín Pérez (CE)
- Paul Done (SA)
- Tim Biedenkapp (SA)

## install kafka with brew
### MAC
    brew install kafka
### UBUNTU
    From https://kafka.apache.org/downloads download latest Kafka tarball and unpack to new directory
    
## start zookeeper
### MAC
    zkServer start
### UBUNTU
    bin/zookeeper-server-start.sh config/zookeeper.properties

## start kafka cluster
### MAC
    kafka-server-start /usr/local/etc/kafka/server.properties
### UBUNTU
    bin/kafka-server-start.sh config/server.properties

## Test Kafka is working ok by creating a topic, sending a message and then subscribing to receive the message
### MAC
	kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
	kafka-topics --list --zookeeper localhost:2181
    kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning
    kafka-console-producer --broker-list localhost:9092 --topic test
     (enter some words at the prompt pressing enter after each to send each)
### UBUNTU
    bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
    bin/kafka-topics.sh --list --zookeeper localhost:2181
    bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
    bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
     (enter some words at the prompt pressing enter after each to send each)


## Configure Eclipse with the project:
	Install Eclipse if you don't already have it from: https://eclipse.org/downloads/
	Import existing project into Eclipse workspace

## Run Java Classes via Eclipse IDE:
    SparkETL.java (consumer)
    SimpleProducer (producer)
