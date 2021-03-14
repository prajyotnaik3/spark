# Kafka-Producer using PySpark

In this document, we discuss about creating a simple Kafka Producer using PySpark and consuming the messages using a kafka consumer in Terminal.

We will the perform following steps:
- Create a Kafka topic.
- Create a Kafka consumer in console.
- Create a Kafka producer using pyspark.
We will perform these steps on cloudxlab.

# 0. Initial Setup (Terminal 1)
- Download the spark-streaming-kafka jar
<pre>		
wget https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-8_2.11/2.0.2/spark-streaming-kafka-0-8_2.11-2.0.2.jar
</pre>	
- Find zookeeper host address using zookeper-client. Launch zookeeper-client using following command in the terminal.
<pre>
zookeeper-client
</pre>		
In zookeeper-client, find ids of available brokers and their details.
<pre>
ls /brokers/ids
</pre>		
We got output as [1004, 1003, 1002] which are the ids of brokers/ids. 

Get the information of one of the broker ids using following command. Here, replace id 1002 with one of the ID that you find.
<pre>
get /brokers/ids/1002
</pre>		
Note down the host and port values. In my case I found:
<pre>
"host":"cxln4.c.thelab-240901.internal"
"port":6667
</pre>
Press Ctrl + C to exit from zookeeper-client.

# 1. Create a Kafka topic in console (Terminal 1)
- Include the kafka binaries in the path.

Hadoop includes the kafka and is installed at "/usr/hdp/current/kafka-broker". To include Kafka binaries in the path, run the following command.
<pre>	
export PATH=$PATH:/usr/hdp/current/kafka-broker/bin
</pre>
- Create the topic.

Now, create the topic using following command. Replace "localhost" with the hostname of node, where zookeeper server is running. Generally, zookeeper (zk) runs on all hosts on the cluster. Also, replace test with your topic name.
<pre>	
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
</pre>
In my case, I will replace localhost with the host we found in the step 0. 
<pre>		
kafka-topics.sh --create --zookeeper cxln4.c.thelab-240901.internal:2181 --replication-factor 1 --partitions 1 --topic prajyot_kafka
</pre>		
Note that we haven't changed the port number, as zookeeper works on port 2181.

# 2. Create a Kafka Consumer in console (Terminal 1)
- Include the Kafka binaries in the path. 
<pre>
export PATH=$PATH:/usr/hdp/current/kafka-broker/bin
</pre>
- Create the Kafka Consumer. Replace the host address (localhost) and topic name (test).
<pre>
kafka-console-consumer.sh --bootstrap-server localhost:2181 --topic test
</pre>
With the host we found in step 0, this command is modified as:
<pre>
kafka-console-consumer.sh --bootstrap-server cxln4.c.thelab-240901.internal:2181 --topic prajyot_kafka
</pre>

# 3. Produce data using PySpark program (Terminal 2)
- Create Producer code in Python

Let's first create a file KafkaProducer.py that will contain our python code for Producer.
<pre>
vim KafkaProducer.py
</pre>
Press i to enter insert mode and paste following code. Press Esc, followed by :wq and Enter key to save the code to the file.
<pre>
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaClient, KafkaProducer
		
sc = SparkContext(appName="Kafka Producer")
ssc = StreamingContext(sc, 10)
		
producer = KafkaProducer(bootstrap_servers = 'cxln4.c.thelab-240901.internal:6667')
		
records = ["This", "is", "a", "produced", "message"]
		
for record in records:
    producer.send('prajyot_kafka', str(record))
    producer.flush()
		
ssc.start()
ssc.awaitTermination()
</pre>
- Run the Producer code in KafkaProducer.py
<pre>	
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 KafkaProducer.py
</pre>
Switch back to Terminal 1 and verify the receipt of messages by consumer.
