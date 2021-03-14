from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]", "SocketTextStream")
ssc = StreamingContext(sc, 5)

lines = ssc.socketTextStream("localhost", 9999)

lines.pprint()

ssc.start()
ssc.awaitTermination()