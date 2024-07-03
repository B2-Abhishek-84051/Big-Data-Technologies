# 3. Count number of movie ratings per month using sql query (using temp views).

# Import the packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Build Spark session
spark = SparkSession.builder\
    .appName('Assign5 Q3')\
    .getOrCreate()

# Load movies data and create DataFrame
movies_df = spark.read\
    .option('header', 'true')\
    .option('inferSchema', 'true')\
    .csv('/home/abhishek/Desktop/BigData/data/movies/movies.csv')

# Test movies DataFrame
# movies_df.printSchema()
# movies_df.show()

# Load rating data and create DataFrame
ratings_df = spark.read\
    .option('header', 'true')\
    .option('inferSchema', 'true')\
    .csv('/home/abhishek/Desktop/BigData/data/movies/ratings.csv')

# Convert the epoch timestamp to a readable timestamp
ratings_df2 = ratings_df.withColumn('timestamp', from_unixtime(col('timestamp')).alias('timestamp'))

# Select required columns
ratings_df2 = ratings_df2.select('userId', 'movieId', 'rating', year('timestamp'))

# Test ratings DataFrame
ratings_df2.printSchema()
ratings_df2.show()

# Stop Spark session
spark.stop()
