from pyspark import SparkContext, HiveContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *

sc = SparkContext("local[2]")

sqlContext = SQLContext(sc)

#Creating dataframe from a csv file
df1 = sqlContext.read.csv("hdfs://cxln1.c.thelab-240901.internal:8020/user/prajyotnaikxxxxx/sparkData/iris.csv", header = True, inferSchema = True)

df1.printSchema()
df1.show()

#Creating dataframe from a json file
df2 = sqlContext.read.json("hdfs://cxln1.c.thelab-240901.internal:8020/user/prajyotnaikxxxxx/sparkData/employee.json")

df2.printSchema()
df2.show()

#Creating dataframe using a hive table
hqlContext = HiveContext(sc)

df3 = hqlContext.sql("select * from prajyot.employee")

df3.printSchema()
df3.show()

#Creating dataframe from RDD
lines = sc.textFile("hdfs://cxln1.c.thelab-240901.internal:8020/user/prajyotnaik37389/sparkData/employee.txt")
parts = lines.map(lambda line: line.split("\t"))

#By inferring the schema using Reflection
employee = parts.map(lambda e: Row(id = int(e[0]), name = e[1], age = int(e[2])))
df4 = sqlContext.createDataFrame(employee)

df4.printSchema()
df4.show()

#By programmatically specifying the schema
employee2 = parts.map(lambda e: (int(e[0]), e[1].strip(), int(e[2])))
schema = StructType([StructField("id", IntegerType(), True), StructField("name", StringType(), True), StructField("age", IntegerType(), True)])
df5 = sqlContext.createDataFrame(employee2, schema)

df5.printSchema()
df5.show()