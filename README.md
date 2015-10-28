A simple Kafka producer & consumer using the 0.9 api


Step 1. Create the topic

	bin/kafka-topics.sh  --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic testTopic



Step 2. 


Option 1: 

	Run the Message Producer & Consumer to read & write from all partitions

	kafka.AllMessagesConsumer src/main/resources/AllMessagesConsumer.properties 3

	kafka.AllMessagesProducer src/main/resources/AllMessagesProducer.properties 1


Option 2:

	Run the Producer & Consumer with pub/sub going to a specific partition using a custom kafka partitioner

	kafka.SinglePartitionMessageConsumer src/main/resources/SinglePartitionConsumer.properties

	kfaka.SinglePartitionMessageProducer src/main/resources/SinglePartitionProducer.properties

