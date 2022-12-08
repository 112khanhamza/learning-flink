# Learning Flink
How to Setup Kafka? - run the below commands

 - brew install kafka
 - brew services start zookeeper
 - brew services start kafka
 - **kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic flink.kafka.streaming.source**

To Check if Kafka is Producing and Consuming

 - **kafka-console-consumer --bootstrap-server localhost:9092 --topic flink.kafka.streaming.source --from-beginning**
 - **kafka-console-producer --broker-list localhost:9092 --topic flink.kafka.streaming.source**

Flink Web UI will be available on the below url
http://localhost:8081/#/overview
