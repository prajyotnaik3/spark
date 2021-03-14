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
