# EventHub-StressTest
This repo consist of source code for posting receiving events to Event-Hub through kafka client

# Motivation
Most of the how-to for connecting and understanding the concept can be found below link
https://github.com/Azure/azure-event-hubs-for-kafka

This code base only generates the loads based on the payloads and number of threads passed in teh runtime arguments including the topic name.

# How to Run
Please do update the config files for your eventhub settings in Azure.

Build the jars with dependencies :
``mvn clean package``

### Prducer

Execute the jar :

``java -classpath event-hubs-kafka-java-producer-1.0-SNAPSHOT-jar-with-dependencies.jar TestEventHubProducer <TOPIC_NAME> payload-<1/2/3/4>.json``

### Consumer

Execute the jar :

``java -classpath event-hubs-kafka-java-consumer-1.0-SNAPSHOT-jar-with-dependencies.jar TestEventHubConsumer <TOPIC_NAME> ``

