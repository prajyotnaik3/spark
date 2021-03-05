# Creating a simple Consumer Producer
# This example is created using cloudxlab

# Terminal 1 - Create a Kafka topic
<pre>
# Include Kafka binaries in the path. HDP includes the kafka and installs at /usr/hdp/current/kafka-broker
	export PATH=$PATH:/usr/hdp/current/kafka-broker/bin

# Create the topic
#Replace localhost with the hostname of node where zookeeper server is running. Generally, zk runs on all hosts on the cluster.
# Replace test with your topic name
	kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic prajyot_kafka
  
#Check if topic is created
	kafka-topics.sh  --list --zookeeper localhost:2181
</pre>
  
# Terminal 1 - Produce the results
<pre>
  # Locate the kafka brokers
  # The kafka brokers inform zookeeper about their IPs adresses. Most of the eco-system considers the zookeeper as a central registry.
  # First launch zookeeper client
	zookeeper-client

  # find the ip address of any broker from zookeeper-client using command get /brokers/ids/<id>
  # On the zookeeper-client prompt, list all the brokers that registered
	ls /brokers/ids
  # This will print a list of ids on your screen
  # Now, get the information about all the ids using the get command with the nodes listed in previous command
  # For example:
	get /brokers/ids/1001
  # Note the host address that will be like cxln4.c.thelab-240901.internal:6667

#Terminal 1 - start kafka producer
	kafka-console-producer.sh --broker-list cxln4.c.thelab-240901.internal:6667 --topic prajyot_kafka
  #This will give you a prompt to type the input which will be pushed to the topic
  #Type some messages
	my first kafka topic
	this is a cow this is a bow
</pre>

# Terminal 2 - Test Consuming Messages
<pre>
  # Test if producer is working by consuming messages in another terminal
  # Replace localhost with the hostname of broker
	export PATH=$PATH:/usr/hdp/current/kafka-broker/bin kafka-console-consumer.sh --zookeeper localhost:2181 --topic prajyot_kafka --from-beginning
  # You will get the messages produced by producer
  # Switch to Terminal 1 and type a message and press enter
  # Switch back to terminal 2 and see if the consumer received the message
  # Come out of consumer prompt by pressing CTRL+C once you have checked if the messages are reaching the consumer.
</pre>

