from pyspark import SparkContext

sc = SparkContext("local[2]", "map-flatMap")

#Example 1 - word count
#Read the input file
lines = sc.textFile("hdfs://cxln1.c.thelab-240901.internal:8020/user/prajyotnaikxxxxx/sparkData/data.txt")

#Split each line into respective words, and assc=ociate each word with a count of 1
words = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1))

#Count the words and collect them into a list
wordsCount = words.reduceByKey(lambda x, y: x + y).colect()

#Output result
print(wordsCount)