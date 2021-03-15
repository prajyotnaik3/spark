from pyspark import SparkContext
from pyspark.streaming import StreamingContext

#Create a local StreamingContext with two working thread and batch interval of 5 seconds
sc = SparkContext("local[2]", "WindowedWordCount")
ssc = StreamingContext(sc, 5)

#Set-up checkpoint directory in HDFS
ssc.checkpoint("hdfs://cxln1.c.thelab-240901.internal:8020/user/prajyotnaikxxxxx/CheckPointDirectory/")

#Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 9999)

#Split each line into words
words = lines.flatMap(lambda line: line.split(" "))

#Count each word in each of the last 60 seconds interval
pairs = words.map(lambda word: (word, 1))
windowedWordCounts = pairs.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 60, 5)

#Print the first ten words
windowedWordCounts.pprint()

#Start the computation
ssc.start()
#Wait for the computation to terminate
ssc.awaitTermination()