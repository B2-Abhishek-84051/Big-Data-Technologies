# Wordcount using Spark Dataframes and nd top 10 words (except stopwords).

# import packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col, regexp_replace, count

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("Word Count") \
    .getOrCreate()

# load data and create dataframe
textfile_path = '/home/abhishek/hadoop-3.3.2/LICENSE.txt'

# Read the word file into a DataFrame
wordfile_df = spark.read.text(textfile_path)

# Define the list of stopwords
stopwords = ['', 'the', 'a', 'an', 'or', 'of', 'and', 'to', 'any', 'for', 'in', 'by', 'from', 'that', 'under', 'over', 'this', 'such', 'as', 'shall', 'your', 'my']

# Perform the transformations as per the SQL query
words_df = wordfile_df \
    .select(explode(split(lower(col("value")), '[^a-z0-9]+')).alias("word")) \
    .filter(~col("word").isin(stopwords)) \
    .groupBy("word") \
    .agg(count("word").alias("cnt")) \
    .orderBy(col("cnt").desc()) \
    .limit(20)

# Show the result
words_df.show()

