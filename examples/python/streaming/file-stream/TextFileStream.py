from pyspark import SparkContext
from pyspark.streaming import StreamingContext

#Create a local StreamingContext with two working threads and batch interval of 5 seconds
sc = SparkContext("local[2]", "TextFileStream")
ssc = StreamingContext(sc, 5)

#Create a DStream that will connect to a data directory in a file system compatible with HDFS API
lines = ssc.textFileStream("hdfs://cxln1.c.thelab-240901.internal:8020/user/prajyotnaik37389/FileStreamData/")

#Print the lines 
lines.pprint()

#Start the computation
ssc.start()
#Wait for the computation to terminate
ssc.awaitTermination()