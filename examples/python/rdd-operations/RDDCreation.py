from pyspark import SparkContext

sc = SparkContext("local[2]", "RDDCreation")

#Creating RDD using Parallelized Collections
data = [1, 2, 3, 4, 5]
distData1 = sc.parallelize(data)

#Creating RDD from external datasets using textFile
distData2 = sc.textFile("hdfs://cxln1.c.thelab-240901.internal:8020/user/prajyotnaikxxxxx/sparkData/data1.txt")

#Creating RDD from extrenal datasets using wholeTextFiles (For directories containing multiple small files)
distData3 = sc.wholeTextFiles("hdfs://cxln1.c.thelab-240901.internal:8020/user/prajyotnaikxxxxx/sparkData/").flatMap(lambda x: x[1].split("\n"))