# 3. Count number of movie ratings per month using sql query (using temp views).

# import the packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# build spark session
spark = SparkSession.builder\
    .appName('Assign5 Q3')\
    .getOrCreate()

# load movies data and create dataframe
movies_df = spark.read\
    .option('header','true')\
    .option('inferSchema','true')\
    .csv('/home/abhishek/Desktop/BigData/data/movies/movies.csv')

# test dataframe
# movies_df.printSchema()
# movies_df.show()

# load rating data and create dataframe
ratings_df = spark.read\
    .option('header','true')\
    .option('inferSchema','true')\
    .csv('/home/abhishek/Desktop/BigData/data/movies/ratings.csv')

ratings_df2 = ratings_df.
# test rating dataframe
ratings_df.printSchema()
ratings_df.show()

# stop spark session
spark.stop()