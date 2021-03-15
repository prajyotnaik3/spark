from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext("local[2]")

sqlContext = SQLContext(sc)

#Creating dataframe from a csv file
df = sqlContext.read.csv("hdfs://cxln1.c.thelab-240901.internal:8020/user/prajyotnaikxxxxx/sparkData/iris.csv", header = True, inferSchema = True)

df.printSchema()
df.show()

#Select
df.select(df['species'] == 'setosa').show()
df.select(df['sepal_length'], df['species']).show()
#Filter
df.filter(df['sepal_length'] < 5).show()
#Filter and select
df.filter((df['sepal_length'] < 5) & (df['species'] != 'setosa')).select(df['sepal_length'], df['species']).show()
#GroupBy
df.groupBy(df['species']).count().show()
df.groupBy(df['species']).mean().show()