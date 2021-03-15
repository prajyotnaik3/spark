from pyspark import SparkContext

sc = SparkContext("local[2]", "map-flatMap")

#Read data and create RDD
lines = sc.textFile("hdfs://cxln1.c.thelab-240901.internal:8020/user/prajyotnaikxxxxx/sparkData/data.txt")

#Map each input line to its respective length, 1-to-1 mapping
lineLengths = lines.map(lambda line: len(line)).collect()
print(lineLengths)

#Map each input line to words, 1-to-(0 or more) mapping
words = lines.flatMap(lambda line: line.split(" ")).collect()
print(words)