# Kafka-Consumer using PySpark
In this document, we discuss about creating a simple Kafka Consumer using PySpark for consuming the messages produced using a kafka producer in Terminal. 

We will perform following steps:
- Create a Kafka topic.
- Create a Kafka producer in console.
- Create a Kafka consumer using pyspark.
We will perform these steps on cloudxlab.

# 0. Initial Setup (Terminal 1)
- Download spark-streaming-kafka jar
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

# 2. Create a Kafka Producer in console (Terminal 1)
- Include the Kafka binaries in the path
<pre>
export PATH=$PATH:/usr/hdp/current/kafka-broker/bin
</pre>		
- Create Kafka Producer 
<pre>	
kafka-console-producer.sh --broker-list cxln4.c.thelab-240901.internal:6667 --topic prajyot_kafka
</pre>  
This will give you a prompt to type the input messages which will be pushed to the topic. Type some messages
<pre>
my first kafka topic
this is a cow this is a bow
</pre>

# 3. Consume data produced by Kafka Producer in step (2) (Terminal 2)
- Create Consumer code in Python
Let's first create a file KafkaConsumer.py that will contain our python code for Consumer.
<pre>
vim KafkaConsumer.py
</pre>		
Press i to enter insert mode and paste following code. Press Esc, followed by :wq and Enter key to save the code to the file.
<pre>
# import SparkContext
from pyspark import SparkContext
# import Spark Streaming Context
from pyspark.streaming import StreamingContext
# import Kafka utils
from pyspark.streaming.kafka import KafkaUtils
# Get the SparkContext
sc = SparkContext("local[3]", "WordCountApp")
# Stream Context with time interval of 5 seconds
ssc = StreamingContext(sc, 5)
# Read the DStream
lines = KafkaUtils.createStream(ssc, 'cxln4.c.thelab-240901.internal:2181', "spark-streaming-consumer", {'prajyot_kafka':1})
# Print the strea line by line
lines.pprint()
# Start the computation
ssc.start()
# Wait for the computation to terminate
ssc.awaitTermination()
</pre>	
- Run the Consumer code in KafkaConsumer.py
<pre>
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 KafkaConsumer.py
</pre>
Switch to Terminal 1 and enter some messages
Switch back to Terminal 2 and verify the receipt of messages by consumer.
