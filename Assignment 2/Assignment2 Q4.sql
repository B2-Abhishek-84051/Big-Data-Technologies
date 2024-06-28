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
-- 4. Display a report that shows subject and average price in descending order 
-- on materialized view.
-- 5. Create a new le newbooks.csv.
-- 20,Atlas Shrugged,Ayn Rand,Novel,723.90
-- 21,The Fountainhead,Ayn Rand,Novel,923.80
-- 22,The Archer,Paulo Cohelo,Novel,623.94
-- 23,The Alchemist,Paulo Cohelo,Novel,634.80
-- 6. Upload the le newbooks.csv into books_staging.
-- 7. Insert "new" records from books_staging into books_orc.
-- 8. Display a report that shows subject and average price in descending order 
-- on materialized view. -- Are new books visible in report?
-- 9. Rebuild the materialized view.
-- 10. Display a report that shows subject and average price in descending order 
-- on materialized view. -- Are new books visible in report?
-- 11. Increase price of all Java books by 10% in books_orc.
-- 12. Rebuild the materialized view.
-- 13. Display a report that shows subject and average price in descending order 
-- on materialized view. -- Are new price changes visible in report?
-- 14. Delete all Java books.
-- 15. Rebuild the materialized view.
-- 16. Display a report that shows subject and average price in descending order 
-- on materialized view. -- Are new price changes visible in report?