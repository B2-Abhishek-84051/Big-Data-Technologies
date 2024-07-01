-- 5. Execute following queries for movies dataset.




-- 1. Upload movies data (movies_caret.csv) into HDFS directory (not in hive warehouse).

> hadoop fs -mkdir -p /user/abhishek/movies/input

> hadoop fs -put /home/abhishek/DBDA_DATA/BigData/BigData/data/movies/movies_caret.csv /user/abhishek/movies/input




-- 2. Create external table movies1 with schema - id INT, title STRING, genres STRING. Find number of 'Action' movies.


> hadoop fs -mkdir -p /user/abhishek/movies1/input

> hadoop fs -put /home/abhishek/DBDA_DATA/BigData/BigData/data/movies/movies.csv /user/abhishek/movies1/input


CREATE EXTERNAL TABLE movies1(
id INT,
title STRING,
genres STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/abhishek/movies1/input';

DESCRIBE movies1;

SELECT COUNT(id) actionmovies FROM movies1 WHERE genres='Action';


+---------------+
| actionmovies  |
+---------------+
| 32            |
+---------------+




-- 3. Create external table movies2 with schema - id INT, title STRING, genres ARRAY<STRING>.Find number of movies having single genre.

> hadoop fs -mkdir -p /user/abhishek/movies2/input

> hadoop fs -put /home/abhishek/DBDA_DATA/BigData/BigData/data/movies/movies.csv /user/abhishek/movies2/input

CREATE EXTERNAL TABLE movies2(
id INT,
title STRING,
genres ARRAY<STRING>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/abhishek/movies2/input';

SELECT COUNT(id) AS single_genres_count
FROM movies2
WHERE size(genres) = 1;

+----------------------+
| single_genres_count  |
+----------------------+
| 9125                 |
+----------------------+
