from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext("local[2]")

sqlContext = SQLContext(sc)

#Creating dataframe from a csv file
irisDF = sqlContext.read.csv("hdfs://cxln1.c.thelab-240901.internal:8020/user/prajyotnaikxxxxx/sparkData/iris.csv", header = True, inferSchema = True)

irisDF.printSchema()
irisDF.show()

#Create a temporary table using dataframe
irisDF.registerTempTable("iris")

#Select query
sqlContext.sql("select * from iris").show()
sqlContext.sql("select sepal_length, species from iris").show()

#Where clause
sqlContext.sql("select * from iris where species = 'setosa'").show()
sqlContext.sql("select sepal_length, species from iris where sepal_length < 5 and species != 'setosa'").show()

#Group by clause
sqlContext.sql("select species, count(*) as count from iris group by species").show()
sqlContext.sql("select species, mean(sepal_length), mean(sepal_width), mean(petal_length), mean(petal_width) from iris group by species").show()

#Order by clause
sqlContext.sql("select * from iris order by sepal_length").show()
sqlContext.sql("select * from iris order by sepal_length, sepal_width").show()