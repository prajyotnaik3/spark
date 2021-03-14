# Streaming a data directory (File-Streaming)

In this example we will see how to stream data directory using spark. We will do this in two steps:
- Create a data directory from where data will be streamed.
- Create a DStream in pyspark that will read the data from data directory.

This experiment has been performed on cloudxlab.

# Create a Data directory on HDFS (Terminal 1)

Let's create a directory on hdfs using following command
<pre>
hadoop fs -ls mkdir FileStreamData
</pre>
We will use this directory as data directory. We will need absolute address of this directory. 

In cloudxlab, to find address of a directory,create a file in the drectory and remove it.
<pre>
hadoop fs -touchz FileStreamData/a.txt
hadoop fs -rm FileStreamData/a.txt
</pre>
Absolute address of this file will be shown.

Files contained in this directory will be streamed one by one. Some of the points to note are:
- The files must have the same data format.
- The files must be created in the datadirectory by atomically moviing or renaming them into the data directory.
- Once moved, the files must not be changed. So if the files are being continuously appended, the new data will not be read.

Create some files, enter some data and move them to data directory. You can run the following commands:
<pre>
vim a.txt
vim b.txt
vim c.txt
hadoop fs -get a.txt FileStreamData/
hadoop fs -get b.txt FileStreamData/
hadoop fs -get c.txt FileStreamData/
</pre>

# Create DStream in PySpark to read file stream (Terminal 2)

Create a file TextFileStream.py using following command.
<pre>
vim TextFileStream.py
</pre>
Press i to go to insert mode. And paste following code in the file:
<pre>
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

#Create a local StreamingContext with two working threads and batch interval of 5 seconds
sc = SparkContext("local[2]", "TextFileStream")
ssc = StreamingContext(sc, 5)

#Create a DStream that will connect to a data directory in a file system compatible with HDFS API
lines = ssc.textFileStream("hdfs://cxln1.c.thelab-240901.internal:8020/user/prajyotnaikxxxxx/FileStreamData/")

#Print the lines 
lines.pprint()

#Start the computation
ssc.start()
#Wait for the computation to terminate
ssc.awaitTermination()
</pre>
Press Esc, followed :wq and press Enter to save the file.

Alternatively, use any editor of your choice to create file TextFileStream.py with above code.

Now, start the example by running following command:
<pre>
spark-submit TextFileStream.py
</pre>
You will see the contents of three text files (a.txt, b.txt, c.txt) in our data directory.

Now, switch back to Terminal 1, create a new file, add some data and move it to data directory.
<pre>
vim d.txt
hadoop fs -get d.txt FileStreamData/
</pre>
Switch back to terminal 2 and note the streamed output.

Press Ctrl + C to get out of the terminal.
