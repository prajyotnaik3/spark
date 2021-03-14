# Word count example on Spark Streaming using a TCP source

In this example we will see how to stream a TCP source using spark. We will do this in two steps:
- Create a data server where data will be produced.
- Create a DStream in pyspark and count words in lines that are read from the data server.

This experiment has been performed on cloudxlab.

# Create a Data Server (Terminal 1)

For this I have used Netcat (already installed on most Unix systems).

Let's create a sever with a hostname: localhost and port: 9999. This can be done using following command.
<pre>
nc -localhost 9999
</pre>

# Create DStream in PySpark (Terminal 2)

Create a file WordCount.py using following command.
<pre>
vim WordCount.py
</pre>
Press i to go to insert mode. And paste following code in the file:
<pre>
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

#Create a local StreamingContext with two working thread and batch interval of 5 seconds
sc = SparkContext("local[2]", "SocketTextStream")
ssc = StreamingContext(sc, 5)

#Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 9999)

#Split each line into words
words = lines.flatMap(lambda line: line.split(" "))

#Count each word in each batch
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x+y)

#Print the first ten words
wordCounts.pprint()

#Start the computation
ssc.start()

#Wait for the computation to terminate
ssc.awaitTermination()
</pre>
Press Esc, followed :wq and press Enter to save the file.

Alternatively, use any editor of your choice to create file WordCount.py with above code.

Now, start the example by running following command:
<pre>
spark-submit WordCount.py
</pre>
Now, switch back to Terminal 1 to create messages and note that they are received, word counts are shown in Terminal 2. 

Press Ctrl + C to get out of the terminal.
