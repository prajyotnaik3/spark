from pyspark import SparkContext

sc = SparkContext("local[2]", "map-flatMap")

#Example 2 - Iris Dataset, average values of fields
#Read the input file
irisLines = sc.textFile("hdfs://cxln1.c.thelab-240901.internal:8020/user/prajyotnaikxxxxx/sparkData/iris.csv")

#Delete first row that contains header data
irisData = irisData.filter(lambda example: not example.startswith("sepal_length"))

#Map Iris data lines to individual entries 
irisRDD = iris.map(lambda example: example.split(',')).map(lambda example: (str(example[4]), (float(example[0]), float(example[1]), float(example[2]), float(example[3]), 1)))

#Sum the values of columns and compute mean values for each column
irisSumRDD = irisRDD.reduceByKey(lambda xv, yv: (xv[0] + yv[0], xv[1] + yv[1], xv[2] + yv[2], xv[3] + yv[3], xv[4] + yv[4]))
irisAvgs = irisSumRDD.map(lambda (k, v): (k, v[0]/v[4], v[1]/v[4], v[2]/v[4], v[3]/v[4])).collect()

print(irisAvgs)