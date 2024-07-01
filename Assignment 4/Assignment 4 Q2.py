# Find max sal per dept per job in emp.csv file.
# SELECT deptno,job,max(sal) FROM emp GROUP BY deptno,job

# import packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# create SparkSession
spark = SparkSession.builder \
            .getOrCreate()

# load data and create dataframe
filepath = '/home/abhishek/Desktop/BigData/data/emp.csv'
emp = spark.read \
        .option('header', 'false') \
        .option('inferSchema', 'true') \
        .csv(filepath) \
        .toDF('empno','ename','job','mgr','hire','sal','comm','deptno')

# test dataframe
emp.printSchema()
emp.show(n=5, truncate=False)

# perform operations on dataframes
result = emp \
        .select('deptno','job','sal') \
        .groupBy('deptno','job').max('sal') \
        
# display result
# result.printSchema()
result.show()

# stop spark session
spark.stop()
