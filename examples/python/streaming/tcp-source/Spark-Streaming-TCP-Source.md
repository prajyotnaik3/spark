# Spark Streaming using a TCP source

In this example we will see how to stream a TCP source using spark. We will do this in two steps:
- Create a data server where data will be produced.
- Create a DStream in pyspark that will read the data from data server.

This experiment has been performed on cloudxlab.

# Create a Data Server (Terminal 1)

For this I have used Netcat (already installed on most Unix systems).

Let's create a sever with a hostname: localhost and port: 9999. This can be done using following command.
<pre>
nc -localhost 9999
</pre>

# Create DStream in PySpark (Terminal 2)

Create a file SocketTextStream.py using following command.
<pre>
vim SocketTextStream.py
</pre>
Press i to go to insert mode. And paste following code in the file:
<pre>
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]", "SocketTextStream")
ssc = StreamingContext(sc, 5)

lines = ssc.socketTextStream("localhost", 9999)

lines.pprint()

ssc.start()
ssc.awaitTermination()
</pre>
Press Esc, followed :wq and press Enter to save the file.

Alternatively, use any editor of your choice to create file SocketTextStream.py with above code.

Now, start the example by running following command:
<pre>
spark-submit SocketTextStream.py
</pre>
Now, switch back to Terminal 1 to create messages and note that they are received in Terminal 2. 

Press Ctrl + C to get out of the terminal.
