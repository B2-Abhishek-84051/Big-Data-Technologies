-- 4. Execute following queries for books.csv dataset.
-- 1. Create table "books_staging" and load books.csv in it.

create table books_staging(
    id INT,
    name STRING,
    author STRING,
    subject STRING,
    price DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH
'/home/abhishek/Desktop/BigData/data/books.csv'
INTO TABLE books_staging;

-- 2. Create table "books_orc" as transactional table.

CREATE TABLE books_orc(
    id INT,
    name STRING,
    author STRING,
    subject STRING,
    price DOUBLE
)
STORED AS ORC
TBLPROPERTIES('transactional'='true');

INSERT INTO books_orc
SELECT id, name,author,subject,price FROM books_staging;

SELECT * FROM books_orc;

-- 3. Create a materialized view for summary -- Subjectwise average book price.

CREATE MATERIALIZED VIEW mv_bookAvg AS
select subject,round(avg(price),2) AS avgPrice from books_orc group by subject;

-- 4. Display a report that shows subject and average price in descending order 
-- on materialized view.

select * from mv_bookAvg order by avgPrice Desc;
+---------------------+----------------------+
| mv_bookavg.subject  | mv_bookavg.avgprice  |
+---------------------+----------------------+
| C++ Programming     | 675.21               |
| Java Programming    | 519.67               |
| Operating Systems   | 447.38               |
| C Programming       | 242.2                |
+---------------------+----------------------+

-- 5. Create a new file newbooks.csv.
--      20,Atlas Shrugged,Ayn Rand,Novel,723.90
--      21,The Fountainhead,Ayn Rand,Novel,923.80
--      22,The Archer,Paulo Cohelo,Novel,623.94
--      23,The Alchemist,Paulo Cohelo,Novel,634.80


-- 6. Upload the file newbooks.csv into books_staging.

LOAD DATA LOCAL INPATH
'/home/abhishek/Desktop/newbooks.csv'
INTO TABLE books_staging;

-- 7. Insert "new" records from books_staging into books_orc.

INSERT INTO books_orc
SELECT * FROM books_staging WHERE id < 1000;

-- 8. Display a report that shows subject and average price in descending order 
-- on materialized view. -- Are new books visible in report?

SELECT subject, avgprice 
FROM mv_books
ORDER BY avgprice DESC;

+--------------------+---------------------+
|      subject       |      avgprice       |
+--------------------+---------------------+
| C++ Programming    | 675.214             |
| Java Programming   | 519.67              |
| Operating Systems  | 447.3836666666666   |
| C Programming      | 281.78499999999997  |
+--------------------+---------------------+

-- Are new books visible in report?
-- NO

-- 9. Rebuild the materialized view.

ALTER MATERIALIZED VIEW mv_books REBUILD;

-- 10. Display a report that shows subject and average price in descending order 
-- on materialized view. -- Are new books visible in report?

SELECT subject, avgprice 
FROM mv_books
ORDER BY avgprice DESC;

-- Are new books visible in report?
-- YES

-- 11. Increase price of all Java books by 10% in books_orc.

UPDATE books_orc
SET price = price * 1.1
WHERE subject LIKE 'Java%';

-- 12. Rebuild the materialized view.

ALTER MATERIALIZED VIEW mv_books REBUILD;

-- 13. Display a report that shows subject and average price in descending order 
-- on materialized view. -- Are new price changes visible in report?

+--------------------+---------------------+
|      subject       |      avgprice       |
+--------------------+---------------------+
| Novel              | 726.6099999999999   |
| C++ Programming    | 675.214             |
| Java Programming   | 607.6934966666666   |
| Operating Systems  | 447.3836666666666   |
| C Programming      | 281.78499999999997  |
+--------------------+---------------------+


-- Are new books visible in report?
-- YES

-- 14. Delete all Java books.

DELETE FROM books_orc
WHERE subject LIKE 'Java%';

-- 15. Rebuild the materialized view.

ALTER MATERIALIZED VIEW mv_books REBUILD;

-- 16. Display a report that shows subject and average price in descending order 
-- on materialized view. -- Are new price changes visible in report?

SELECT subject, avgprice 
FROM mv_books
ORDER BY avgprice DESC;


+--------------------+---------------------+
|      subject       |      avgprice       |
+--------------------+---------------------+
| Novel              | 726.6099999999999   |
| C++ Programming    | 675.214             |
| Operating Systems  | 447.3836666666666   |
| C Programming      | 281.78499999999997  |
+--------------------+---------------------+

-- Are new books visible in report?
--YES