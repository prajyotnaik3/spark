# Creating a simple Kafka Producer and Consumer using Terminal
This example is created using cloudxlab. It is recommended that one performs it over there. A free 7-day trial is available.

We will be using multiple terminals to carry out following steps.
- First thing we need to create is, a Kafka topic.
- Then create a Kafka Producer, which will send the messages to the topic.
- Finally create a Kafka Consumer, which will receive the messages from the topic.

# Terminal 1 - Create a Kafka topic.
Hadoop includes the kafka and is installed at "/usr/hdp/current/kafka-broker". To include Kafka binaries in the path, run the following command.
<pre>
export PATH=$PATH:/usr/hdp/current/kafka-broker/bin
</pre>
Now, create the topic using following command. Replace "localhost" with the hostname of node, where zookeeper server is running. Generally, zookeeper (zk) runs on all hosts on the cluster. Also, replace test with your topic name.
<pre>
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
</pre>
We ran a modified version of this command, replacing the hostname and topic name.
<pre>
kafka-topics.sh --create --zookeeper cxln4.c.thelab-240901.internal:2181 --replication-factor 1 --partitions 1 --topic prajyot_kafka
</pre>
Now, check if the topic is created using the following command. Again, replace your topic name here.
<pre>
kafka-topics.sh  --list --zookeeper localhost:2181 | grep test
</pre>
We ran a modifeied version as follows:
<pre>
kafka-topics.sh  --list --zookeeper cxln4.c.thelab-240901.internal:2181 | grep prajyot_kafka
</pre>
  
# Terminal 1 - Produce the results (Producer) 
We will be running the following commands in the same terminal. If you are using a different terminal then include the kafka-binaries in path.

To create a producer, we need to locate the Kafka brokers first. The Kafka brokers inform zookeeper about their IP adresses. Most of the eco-system consider the zookeeper as a central registry. First launch zookeeper client using following command:
<pre>
zookeeper-client
</pre>
We can get the IP address of a broker using zookeeper-client with following command.
<pre>
get /brokers/ids/< id >
</pre>
But first, we need to find a broker ID. On the zookeeper-client prompt, list all the brokers that are registered using following command:
<pre>
ls /brokers/ids
</pre>
This will print a list of IDs on your screen. Now, get the information about one of the IDs, using the get command with one of the IDs listed in previous command. For example:
<pre>
get /brokers/ids/1001
</pre>
Note the host address that will be like "cxln4.c.thelab-240901.internal:6667" in case of cloudxlab. Now we can create a Kafka Producer. Again, remember to replace hostname and topic name.
<pre>
kafka-console-producer.sh --broker-list localhost:6667 --topic test
</pre>
We ran a modified command as follows in our case.
<pre>
kafka-console-producer.sh --broker-list cxln4.c.thelab-240901.internal:6667 --topic prajyot_kafka
</pre>
This will give you a prompt to type the input messages, which will be pushed to the topic. Type some messages:
<pre>
my first kafka topic
this is a cow this is a bow
</pre>

# Terminal 2 - Test Consuming Messages (Consumer)
Now, we will test if the producer is working by consuming messages in another terminal. Again, begin by exporting the kafka-binaries.
<pre>
export PATH=$PATH:/usr/hdp/current/kafka-broker/bin
</pre>
Now, create a Kafka consumer using the following command. Replace localhost with the hostname of broker. Also, replace the topic name (test).
<pre>
kafka-console-consumer.sh --zookeeper localhost:2181 --topic test --from-beginning
</pre>
<pre>
kafka-console-consumer.sh --zookeeper cxln4.c.thelab-240901.internal:2181 --topic prajyot_kafka --from-beginning
</pre>
You will receive the messages produced by producer in first terminal in second terminal. Switch to Terminal 1 and type a message and press enter. Switch back to Terminal 2 and see if the consumer received the message. Come out of consumer prompt by pressing CTRL+C once you have checked, if the messages are reaching the consumer. 
