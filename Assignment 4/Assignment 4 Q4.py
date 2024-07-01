# Count number of movie ratings per year.

# import packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# create SparkSession
spark = SparkSession.builder \
            .appName("Convert Epoch Time to Unix Time") \
            .getOrCreate()

# load data and create dataframe
filepath = '/home/abhishek/Desktop/BigData/data/movies/ratings.csv'
rating_table = spark.read \
        .option('header', 'true') \
        .option('inferSchema', 'true') \
        .csv(filepath)

# convert epoch time in seconds to timestamp and then to Unix time
rating_table = rating_table.withColumn('timestamp', from_unixtime('timestamp'))

# test dataframe
rating_table.printSchema()
rating_table.show(n=5, truncate=False)

# perform operations on dataframes
result = rating_table.withColumn('year', year('timestamp')) \
               .groupBy('year') \
               .agg(count('rating').alias('count_rating')) \
               .orderBy('year')
        
# display result
result.printSchema()
result.show()

# stop spark session
spark.stop()

# SELECT YEAR(timestamp),COUNT(rating) FROM rating;