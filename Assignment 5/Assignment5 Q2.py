# 2. Read ncdc data from mysql table and print average temperature per year in DESC order.

# import packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# create sparksession
spark = SparkSession.builder.appName('Assign5-Q2').getOrCreate()

# read from mysql table
ncdc_df = spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/dbdadb") \
    .option("dbtable", "ncdc") \
    .option("user", "dbda") \
    .option("password", "dbda") \
    .load()

# ncdc_df.printSchema()
# ncdc_df.show()

result = ncdc_df.select('yr','temp')\
                .groupBy('yr').avg('temp')\
                .withColumnRenamed('avg(temp)','AvgTemp')

result.show()