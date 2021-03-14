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
