-- Predict Similar movies for given movie Id. Get the recommended movies titles from movies table.
-- Hints
---- Start with above small data tables to test accuracy of the steps.
---- You will need to create new intermediate tables to store results of earlier queries.
---- For main data use ORC format to speed-up the queries.
---- You may need to change reducer tasks memory for quicker execution and avoid OutOfMemory errors.
------ SET mapreduce.reduce.memory.mb = 4096;
------ SET mapreduce.reduce.java.opts = -Xmx4096m;

CREATE TABLE tratings(
userId INT,
movieId INT,
rating DOUBLE,
rtime BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES('skip.header.line.count'='1');

LOAD DATA LOCAL
INPATH '/home/abhishek/Desktop/BigData/data/tratings.csv'
INTO TABLE ratings;

SELECT t1.movieId m1, t2.movieId m2, t1.rating r1, t2.rating r2 
FROM ratings t1 INNER JOIN ratings t2 ON t1.userId = t2.userId
WHERE t1.movieId < t2.movieId;

CREATE TABLE cor_movies2 AS
SELECT t1.movieId m1, t2.movieId m2, t1.rating r1, t2.rating r2 
FROM ratings t1 INNER JOIN ratings t2 ON t1.userId = t2.userId
WHERE t1.movieId < t2.movieId;

SELECT * FROM cor_movies2;

SELECT m1, m2, CORR(r1,r2) FROM cor_movies2
GROUP BY m1, m2;

CREATE TABLE cor_table2 AS
SELECT m1, m2, COUNT(m1) cnt, CORR(r1,r2) cor FROM cor_movies2
GROUP BY m1, m2
HAVING CORR(r1,r2) IS NOT NULL;

SELECT cor_table2.*,movies.title FROM cor_table2 
inner join movies on cor_table2.m2=movies.m2 Limit 5;

CREATE TABLE movie_rec AS
SELECT cor_table2.*,movies.title FROM cor_table2 
inner join movies on cor_table2.m2=movies.m2 Limit 5;

SELECT title FROM movie_rec where m1=858 ORDER BY cnt desc limit 1;
+---------------------------------+
|              title              |
+---------------------------------+
| Godfather: Part II, The (1974)  |
+---------------------------------+
1 row selected (29.323 seconds)