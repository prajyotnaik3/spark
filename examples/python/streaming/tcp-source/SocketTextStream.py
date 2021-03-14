from pyspark import SparkContext
from pyspark.streaming import StreamingContext

#Create a local StreamingContext with two working thread and batch interval of 5 seconds
sc = SparkContext("local[2]", "SocketTextStream")
ssc = StreamingContext(sc, 5)

#Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 9999)

#Print the lines 
lines.pprint()

#Start the computation
ssc.start()

#Wait for the computation to terminate
ssc.awaitTermination()