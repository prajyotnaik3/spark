from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark import HiveContext

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