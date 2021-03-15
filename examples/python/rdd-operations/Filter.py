from pyspark import SparkContext

sc = SparkContext("local[2]", "Filter")

#Read data and create RDD
lines = sc.textFile("hdfs://cxln1.c.thelab-240901.internal:8020/user/prajyotnaikxxxxx/sparkData/data.txt")

#Filter lines of odd length
evenLengthLines = lines.filter(lambda line: len(line) % 2 == 0).collect()
print(evenLengthLines)