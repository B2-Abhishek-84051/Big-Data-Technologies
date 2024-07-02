# 1.Clean NCDC data and write year, temperature and quality data into mysql table.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('spark-Assign-01').getOrCreate()
spark.sparkContext.setCheckpointDir('/tmp/ncdc-avgtemp')

ncdcFile = '/tmp/ncdc'
ncdc = spark.read\
        .text(ncdcFile)

# ncdc.printSchema()
# ncdc.show(truncate=False)

regex = r'^.{15}([0-9]{4}).{68}([-\+][0-9]{4})([0-9]).*$'
temps = ncdc.select(
            regexp_extract('value',regex,1).alias('yr').cast('int'),
            regexp_extract('value',regex,2).alias('temp').cast('int'),
            regexp_extract('value',regex,3).alias('quality').cast('int')
        ).where('quality IN (0,1,2,3,4,5,9) AND temp != 9999')

temps.printSchema()
temps.show()

dbUrl = 'jdbc:mysql://localhost:3306/dbdadb'
dbDriver = 'com.mysql.cj.jdbc.Driver'
dbUser = 'dbda'
dbPasswd = 'dbda'
dbTable = 'ncdc'
temps.write\
    .option('url', dbUrl)\
    .option('driver', dbDriver)\
    .option('user', dbUser) \
    .option('password', dbPasswd) \
    .option('dbtable', dbTable) \
    .mode('OVERWRITE') \
    .format('jdbc') \
    .save()

spark.stop()

print(f"---done---")

# yravgtemps = temps.where('quality IN (0,1,2,3,4,5,9) AND temp != 9999')\
#                     .groupBy('yr').avg('temp')\
#                     .withColumnRenamed('avg(temp)','avgtemp')

# yravgtemps.printSchema()
# yravgtemps.show()
